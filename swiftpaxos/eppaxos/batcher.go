package eppaxos

import (
	"bytes"
	"sync"
	"time"

	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
)

type Batcher struct {
	r         *Replica
	ch        chan batchOp
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

type batchOp struct {
	peer  int32
	rpc   uint8
	msg   fastrpc.Serializable
	enqTs time.Time
}

func NewBatcher(r *Replica, size int, _, _ interface{}) *Batcher {
	b := &Batcher{
		r:      r,
		ch:     make(chan batchOp, size),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go b.run()
	return b
}

func (b *Batcher) Send(peer int32, rpc uint8, msg fastrpc.Serializable) {
	// Do not accept new sends after stop
	select {
	case <-b.stopCh:
		return
	default:
	}
	select {
	case b.ch <- batchOp{peer: peer, rpc: rpc, msg: msg, enqTs: time.Now()}:
	default:
		// fallback synchronous send when channel full
		b.r.PrintDebug("direct send due to full queue", "peer", peer, "rpc", rpc, "time", time.Now())
		b.r.SendMsg(peer, rpc, msg)
	}
}

// Close stops the background batching goroutine and waits for it to exit.
func (b *Batcher) Close() {
	b.closeOnce.Do(func() {
		close(b.stopCh)
	})
	<-b.doneCh
}

func (b *Batcher) run() {
	defer close(b.doneCh)
	for {
		var first batchOp
		select {
		case <-b.stopCh:
			return
		case first = <-b.ch:
		}

		batchStart := time.Now()
		// Drain current queue similar to swift/batcher.go: use current length snapshot
		n := len(b.ch) + 1
		collect := make([]batchOp, n)
		collect[0] = first
		for i := 1; i < n; i++ {
			op := <-b.ch
			collect[i] = op
		}
		drainDone := time.Now()

		buckets := make(map[int32][]batchOp, 8)
		for _, op := range collect {
			buckets[op.peer] = append(buckets[op.peer], op)
		}
		bucketDone := time.Now()

		// queue age stats
		var (
			minAge time.Duration = 1<<63 - 1
			maxAge time.Duration
			sumAge time.Duration
		)
		now := batchStart
		for _, op := range collect {
			if !op.enqTs.IsZero() {
				age := now.Sub(op.enqTs)
				if age < minAge {
					minAge = age
				}
				if age > maxAge {
					maxAge = age
				}
				sumAge += age
			}
		}
		avgAge := time.Duration(0)
		if n > 0 && sumAge > 0 {
			avgAge = sumAge / time.Duration(n)
		}

		// peer order (deterministic) for readability
		peerOrder := make([]int, 0, len(buckets))
		for pid := range buckets {
			peerOrder = append(peerOrder, int(pid))
		}
		// simple insertion sort to avoid importing sort
		for i := 1; i < len(peerOrder); i++ {
			v := peerOrder[i]
			j := i - 1
			for j >= 0 && peerOrder[j] > v {
				peerOrder[j+1] = peerOrder[j]
				j--
			}
			peerOrder[j+1] = v
		}

		b.r.PrintDebug(
			"batch stats",
			"collect_n", n,
			"drain_dur", drainDone.Sub(batchStart),
			"bucket_dur", bucketDone.Sub(drainDone),
			"queue_age_min", minAge,
			"queue_age_avg", avgAge,
			"queue_age_max", maxAge,
			"num_peers", len(buckets),
			"peer_order", peerOrder,
		)

		b.sendBatches(buckets)
	}
}

func (b *Batcher) sendBatches(buckets map[int32][]batchOp) {
	for peer, ops := range buckets {
		var rid, inst int32 = -1, -1
		if len(ops) > 0 {
			// try to extract replica/instance if this is a PreAcceptReply batch
			if par, ok := ops[0].msg.(*PreAcceptReply); ok {
				rid = int32(par.Replica)
				inst = par.Instance
			}
		}
		// Encode outside the global lock to avoid blocking other peers
		encStart := time.Now()
		var buf bytes.Buffer
		rpcCount := make(map[uint8]int, 8)
		rpcBytes := make(map[uint8]int, 8)
		for _, op := range ops {
			before := buf.Len()
			buf.WriteByte(op.rpc)
			op.msg.Marshal(&buf)
			delta := buf.Len() - before
			rpcCount[op.rpc]++
			rpcBytes[op.rpc] += delta
		}
		encDur := time.Since(encStart)

		// Now short-hold the global lock only for the actual socket write+flush
		tlock := time.Now()
		b.r.PrintDebug("about to lock peer writer", peer, "ops", len(ops), "first_replica", rid, "first_instance", inst, "time", tlock)
		b.r.M.Lock()
		b.r.PrintDebug("acquired peer writer lock", peer, "ops", len(ops), "lock_wait", time.Since(tlock))
		w := b.r.PeerWriters[peer]
		if w == nil {
			b.r.M.Unlock()
			continue
		}
		preBuffered := w.Buffered()
		writeStart := time.Now()
		// single bulk write
		wroteBytes, writeErr := w.Write(buf.Bytes())
		writeDur := time.Since(writeStart)
		midBuffered := w.Buffered()
		b.r.PrintDebug("wrote to peer buffer", peer, "ops", len(ops), "first_replica", rid, "first_instance", inst, "encode_dur", encDur, "write_size", buf.Len(), "bytes_written", wroteBytes, "buf_before_write", preBuffered, "buf_after_write", midBuffered, "write_dur", writeDur, "rpc_counts", rpcCount, "rpc_bytes", rpcBytes, "time", writeStart)
		if wroteBytes != buf.Len() {
			b.r.PrintDebug("partial write to peer", peer, "bytes_written", wroteBytes, "expected_bytes", buf.Len())
		}
		flushStart := time.Now()
		flushErr := w.Flush()
		flushDur := time.Since(flushStart)
		const slowWrite = 30 * time.Millisecond
		const slowFlush = 50 * time.Millisecond
		if writeDur > slowWrite {
			b.r.PrintDebug("slow write to peer", peer, "ops", len(ops), "write_dur", writeDur, "bytes_written", wroteBytes, "expected_bytes", buf.Len(), "rpc_counts", rpcCount, "rpc_bytes", rpcBytes)
		}
		if flushDur > slowFlush {
			b.r.PrintDebug("slow flush to peer", peer, "ops", len(ops), "flush_dur", flushDur, "buf_before_write", preBuffered, "buf_after_write", midBuffered, "buf_after_flush", w.Buffered(), "encode_dur", encDur, "write_dur", writeDur, "rpc_counts", rpcCount, "rpc_bytes", rpcBytes)
		}
		if writeErr != nil {
			b.r.PrintDebug("write error to peer", peer, "err", writeErr)
		}
		if flushErr != nil {
			b.r.PrintDebug("flush error to peer", peer, "err", flushErr)
		}
		b.r.PrintDebug("flushed to peer", peer, "ops", len(ops), "flush_dur", flushDur, "total_peer_dur", time.Since(encStart), "buf_after_flush", w.Buffered())
		b.r.M.Unlock()
	}
}
