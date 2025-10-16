package replica

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

type Replica struct {
	*dlog.Logger

	M     sync.Mutex
	N     int
	F     int
	Id    int32
	Alias string

	PeerAddrList       []string
	Peers              []net.Conn
	PeerReaders        []*bufio.Reader
	PeerWriters        []*bufio.Writer
	ClientWriters      map[int32]*bufio.Writer
	Config             *config.Config
	Alive              []bool
	PreferredPeerOrder []int32

	State        *state.State
	RPC          *fastrpc.Table
	StableStore  *os.File
	Stats        *defs.Stats
	Shutdown     bool
	Listener     net.Listener
	ProposeChan  chan *defs.GPropose
	PProposeChan chan *defs.GPropose
	BeaconChan   chan *defs.GBeacon

	Thrifty bool
	Exec    bool
	LRead   bool
	Dreply  bool
	Beacon  bool
	Durable bool

	// Broadcast of average RTTs
	LatBroadcasted bool
	PeerLatencies  map[int32][]int64

	Ewma      []float64
	Latencies []int64

	Dt *defs.LatencyTable
}

func New(alias string, id, f int, addrs []string, thrifty, exec, lread bool, config *config.Config, l *dlog.Logger) *Replica {
	n := len(addrs)
	r := &Replica{
		Logger: l,

		N:     n,
		F:     f,
		Id:    int32(id),
		Alias: alias,

		PeerAddrList:       addrs,
		Peers:              make([]net.Conn, n),
		PeerReaders:        make([]*bufio.Reader, n),
		PeerWriters:        make([]*bufio.Writer, n),
		ClientWriters:      make(map[int32]*bufio.Writer),
		Config:             config,
		Alive:              make([]bool, n),
		PreferredPeerOrder: make([]int32, n),

		State:        state.InitState(),
		RPC:          fastrpc.NewTableId(defs.RPC_TABLE),
		StableStore:  nil,
		Stats:        &defs.Stats{M: make(map[string]int)},
		Shutdown:     false,
		Listener:     nil,
		ProposeChan:  make(chan *defs.GPropose, defs.CHAN_BUFFER_SIZE),
		PProposeChan: make(chan *defs.GPropose, defs.CHAN_BUFFER_SIZE),
		BeaconChan:   make(chan *defs.GBeacon, defs.CHAN_BUFFER_SIZE),

		Thrifty: thrifty,
		Exec:    exec,
		LRead:   lread,
		Dreply:  true,
		Beacon:  false,
		Durable: false,

		LatBroadcasted: false,
		PeerLatencies:  make(map[int32][]int64),

		Ewma:      make([]float64, n),
		Latencies: make([]int64, n),

		Dt: defs.NewLatencyTable(defs.LatencyConf, defs.IP(), addrs),
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0.0
		r.Latencies[i] = 0
	}

	return r
}

func (r *Replica) Ping(args *defs.PingArgs, reply *defs.PingReply) error {
	return nil
}

func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	return nil
}

func (r *Replica) FastQuorumSize() int {
	return r.F + (r.F+1)/2
}

func (r *Replica) SlowQuorumSize() int {
	return (r.N + 1) / 2
}

func (r *Replica) WriteQuorumSize() int {
	return r.F + 1
}

func (r *Replica) ReadQuorumSize() int {
	return r.N - r.F
}

func (r *Replica) ConnectToPeers() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	for i := 0; i < int(r.Id); i++ {
		for {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				break
			}
			time.Sleep(1e9)
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			r.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
		r.Printf("OUT Connected to %d", i)
	}
	<-done
	r.Printf("Replica %d: done connecting to peers", r.Id)
	r.Printf("Node list %v", r.PeerAddrList)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		go r.replicaListener(rid, reader)
	}
}

func (r *Replica) ConnectToPeersNoListeners() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	for i := 0; i < int(r.Id); i++ {
		for {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				break
			}
			time.Sleep(1e9)
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			r.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	r.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

func (r *Replica) WaitForClientConnections() {
	r.Println("Waiting for client connections")

	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			r.Println("Accept error:", err)
			continue
		}
		go r.clientListener(conn)
	}
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	// observe lock wait for global mutex
	lt := time.Now()
	r.PrintDebug("r.M locking", "where", "SendMsg", "peer", peerId, "time", lt)
	r.M.Lock()
	r.PrintDebug("r.M locked", "where", "SendMsg", "peer", peerId, "lock_wait", time.Since(lt))
	defer r.M.Unlock()
	w := r.PeerWriters[peerId]
	if w == nil {
		r.Printf("Connection to %d lost!", peerId)
		return
	}
	r.PrintDebug("about to send to peer", peerId, "rpc", code, "time", time.Now())
	tEnc := time.Now()
	w.WriteByte(code)
	msg.Marshal(w)
	tFlush := time.Now()
	pre := w.Buffered()
	r.PrintDebug("send flush start", "peer", peerId, "rpc", code, "encode_dur", tFlush.Sub(tEnc), "buf_before", pre)
	w.Flush()
	r.PrintDebug("send flush end", "peer", peerId, "rpc", code, "flush_dur", time.Since(tFlush), "buf_after", w.Buffered())
}

func (r *Replica) SendClientMsg(id int32, code uint8, msg fastrpc.Serializable) {
	lt := time.Now()
	r.PrintDebug("r.M locking", "where", "SendClientMsg", "client", id, "time", lt)
	r.M.Lock()
	r.PrintDebug("r.M locked", "where", "SendClientMsg", "client", id, "lock_wait", time.Since(lt))
	defer r.M.Unlock()

	w := r.ClientWriters[id]
	if w == nil {
		r.Printf("Connection to client %d lost!", id)
		return
	}
	r.PrintDebug("about to send to client", id, "rpc", code, "time", time.Now())
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	lt := time.Now()
	r.PrintDebug("r.M locking", "where", "SendMsgNoFlush", "peer", peerId, "time", lt)
	r.M.Lock()
	r.PrintDebug("r.M locked", "where", "SendMsgNoFlush", "peer", peerId, "lock_wait", time.Since(lt))
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		r.Printf("Connection to %d lost!", peerId)
		return
	}
	tEnc := time.Now()
	w.WriteByte(code)
	msg.Marshal(w)
	r.PrintDebug("send noflush", "peer", peerId, "rpc", code, "encode_dur", time.Since(tEnc), "buf_now", w.Buffered())
}

func (r *Replica) ReplyProposeTS(reply *defs.ProposeReplyTS, w *bufio.Writer, lock *sync.Mutex) {
	// Use per-client lock to avoid blocking global mutex on client IO
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	r.PrintDebug("about to reply to client (ReplyProposeTS)", time.Now(), "cmdId", reply.CommandId)
	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) SendBeacon(peerId int32) {
	lt := time.Now()
	r.PrintDebug("r.M locking", "where", "SendBeacon", "peer", peerId, "time", lt)
	r.M.Lock()
	r.PrintDebug("r.M locked", "where", "SendBeacon", "peer", peerId, "lock_wait", time.Since(lt))
	defer r.M.Unlock()

	w := r.PeerWriters[peerId]
	if w == nil {
		r.Printf("Connection to %d lost!", peerId)
		return
	}
	w.WriteByte(defs.GENERIC_SMR_BEACON)
	beacon := &defs.Beacon{
		Timestamp: time.Now().UnixNano(),
	}
	beacon.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyBeacon(beacon *defs.GBeacon) {
	lt := time.Now()
	r.PrintDebug("r.M locking", "where", "ReplyBeacon", "peer", beacon.Rid, "time", lt)
	r.M.Lock()
	r.PrintDebug("r.M locked", "where", "ReplyBeacon", "peer", beacon.Rid, "lock_wait", time.Since(lt))
	defer r.M.Unlock()

	w := r.PeerWriters[beacon.Rid]
	if w == nil {
		r.Printf("Connection to %d lost!", beacon.Rid)
		return
	}
	w.WriteByte(defs.GENERIC_SMR_BEACON_REPLY)
	rb := &defs.BeaconReply{
		Timestamp: beacon.Timestamp,
	}
	rb.Marshal(w)
	w.Flush()
}

func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.M.Lock()
	r.PreferredPeerOrder = aux
	r.M.Unlock()
}

func (r *Replica) ComputeClosestPeers() []float64 {
	npings := 20

	for j := 0; j < npings; j++ {
		for i := int32(0); i < int32(r.N); i++ {
			if i == r.Id {
				continue
			}
			r.M.Lock()
			if r.Alive[i] {
				r.M.Unlock()
				r.SendBeacon(i)
			} else {
				r.Latencies[i] = math.MaxInt64
				r.M.Unlock()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	quorum := make([]int32, r.N)

	r.M.Lock()
	for i := int32(0); i < int32(r.N); i++ {
		pos := 0
		for j := int32(0); j < int32(r.N); j++ {
			if (r.Latencies[j] < r.Latencies[i]) ||
				((r.Latencies[j] == r.Latencies[i]) && (j < i)) {
				pos++
			}
		}
		quorum[pos] = int32(i)
	}
	r.M.Unlock()

	if r.Dt == nil {
		r.UpdatePreferredPeerOrder(quorum)
	} else {
		sort.Slice(r.PreferredPeerOrder, func(i, j int) bool {
			di := r.Dt.WaitDurationID(int(r.PreferredPeerOrder[i]))
			dj := r.Dt.WaitDurationID(int(r.PreferredPeerOrder[j]))
			return dj == time.Duration(0) || di < dj
		})
	}

	latencies := make([]float64, r.N-1)

	for i := 0; i < r.N-1; i++ {
		node := r.PreferredPeerOrder[i]
		lat := float64(r.Latencies[node]) / float64(npings*1000000)
		r.Println(node, "->", lat, "ms")
		latencies[i] = lat
	}

	// Broadcast the average RTTs once to all peers.
	if !r.LatBroadcasted {
		// Convert to average RTT in milliseconds
		avgMs := make([]int64, len(r.Latencies))
		for i, tot := range r.Latencies {
			avgMs[i] = tot / int64(npings) / 1_000_000 // ns -> ms
		}

		msg := &defs.LatenciesMsg{
			Rid:       r.Id,
			Latencies: avgMs,
		}

		r.Println("LAT_BROADCAST", msg.Rid, msg.Latencies)

		for pid := int32(0); pid < int32(r.N); pid++ {
			if pid == r.Id {
				continue
			}
			r.SendMsg(pid, defs.LATENCIES_BROADCAST, msg)
		}
		r.LatBroadcasted = true
	}

	return latencies
}

func (r *Replica) waitForPeerConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

	port := strings.Split(r.PeerAddrList[r.Id], ":")[1]
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		r.Fatal(r.PeerAddrList[r.Id], err)
	}
	r.Listener = l
	for i := r.Id + 1; i < int32(r.N); i++ {
		conn, err := r.Listener.Accept()
		if err != nil {
			r.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			r.Println("Connection establish error:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
		r.Printf("IN Connected to %d", id)
	}

	done <- true
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var (
		msgType      uint8
		err          error = nil
		gbeacon      defs.Beacon
		gbeaconReply defs.BeaconReply
	)

	for err == nil && !r.Shutdown {
		// Earliest userland point before any read: attempting to read msg type
		//r.PrintDebug("before read msgType", "from", rid, "time", time.Now())
		if msgType, err = reader.ReadByte(); err != nil {
			break
		}
		// Earliest point after kernel returns a byte: message type received
		r.PrintDebug("recv msgType", "from", rid, "type", msgType, "time", time.Now())

		switch uint8(msgType) {

		case defs.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			r.ReplyBeacon(&defs.GBeacon{
				Rid:       int32(rid),
				Timestamp: gbeacon.Timestamp,
			})
			break

		case defs.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			r.M.Lock()
			r.Latencies[rid] += time.Now().UnixNano() - gbeaconReply.Timestamp
			r.M.Unlock()
			now := time.Now().UnixNano()
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(now-gbeaconReply.Timestamp)
			break

		case defs.LATENCIES_BROADCAST:
			latmsg := &defs.LatenciesMsg{}
			if err = latmsg.Unmarshal(reader); err != nil {
				break
			}
			r.M.Lock()
			r.PeerLatencies[latmsg.Rid] = latmsg.Latencies
			r.M.Unlock()
			r.Println("LAT_RECEIVED", latmsg.Rid, latmsg.Latencies) // already in ms
			break

		default:
			p, exists := r.RPC.Get(msgType)
			if exists {
				obj := p.Obj.New()
				// Just before decoding payload
				//r.PrintDebug("about to decode", "from", rid, "type", msgType, "time", time.Now())
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				if ts, ok := obj.(interface{ SetRecvTs(time.Time) }); ok {
					// stamp the earliest receive time
					ts.SetRecvTs(time.Now())
				}
				// stamp sender id if message supports it (no wire change)
				if ss, ok := obj.(interface{ SetSender(int8) }); ok {
					ss.SetSender(int8(rid))
				}
				// earliest userland log: fully decoded and before enqueue to channel
				repId, instId := int32(-1), int32(-1)
				// best-effort extract fields via reflection without importing eppaxos (avoid cycles)
				if rv := reflect.ValueOf(obj); rv.Kind() == reflect.Ptr {
					v := rv.Elem()
					if v.IsValid() {
						if f := v.FieldByName("Replica"); f.IsValid() && f.Kind() == reflect.Int32 {
							repId = int32(f.Int())
						}
						if f := v.FieldByName("Instance"); f.IsValid() && f.Kind() == reflect.Int32 {
							instId = int32(f.Int())
						}
					}
				}
				if repId != -1 || instId != -1 {
					r.PrintDebug("wire recv done", "from", rid, "type", msgType, "replica", repId, "inst", instId, "time", time.Now())
				} else {
					r.PrintDebug("wire recv done", "from", rid, "type", msgType, "time", time.Now())
				}
				// Enqueue with optional simulated delay; avoid goroutine/Sleep(0) when delay is zero
				var delay time.Duration
				if r.Dt != nil {
					delay = r.Dt.WaitDurationID(rid)
				}
				if delay == 0 {
					ts := time.Now()
					p.Chan <- obj
					blk := time.Since(ts)
					if blk > 100*time.Microsecond {
						r.PrintDebug("chan enqueue blocked", "from", rid, "type", msgType, "block_dur", blk, "time", time.Now())
					}
				} else {
					go func(obj fastrpc.Serializable, d time.Duration) {
						time.Sleep(d)
						ts := time.Now()
						p.Chan <- obj
						blk := time.Since(ts)
						if blk > 100*time.Microsecond {
							r.PrintDebug("chan enqueue blocked", "from", rid, "type", msgType, "block_dur", blk, "time", time.Now())
						}
					}(obj, delay)
				}
			} else {
				r.Fatal("Error: received unknown message type ", msgType, " from ", rid)
			}
		}
	}

	r.M.Lock()
	r.Alive[rid] = false
	r.M.Unlock()
}

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	var (
		msgType byte
		err     error
	)

	//r.M.Lock()
	//r.Println("Client up", conn.RemoteAddr(), "(", r.LRead, ")")
	//r.M.Unlock()

	addr := strings.Split(conn.RemoteAddr().String(), ":")[0]
	isProxy := r.Config.Proxy.IsProxy(r.Alias, addr)

	mutex := &sync.Mutex{}

	dchan := defs.NewDelayProposeChan(r.Dt.WaitDuration(addr), r.ProposeChan)

	for !r.Shutdown && err == nil {
		// Earliest userland point before any client read
		r.PrintDebug("before read client msgType", conn.RemoteAddr(), "time", time.Now())
		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {
		case defs.PROPOSE:
			propose := &defs.Propose{}
			if err = propose.Unmarshal(reader); err != nil {
				break
			}
			r.M.Lock()
			r.ClientWriters[propose.ClientId] = writer
			r.M.Unlock()
			op := propose.Command.Op
			if r.LRead && (op == state.GET || op == state.SCAN) {
				r.ReplyProposeTS(&defs.ProposeReplyTS{
					OK:        defs.TRUE,
					CommandId: propose.CommandId,
					Value:     propose.Command.Execute(r.State),
					Timestamp: propose.Timestamp,
				}, writer, mutex)
			} else {
				dchan.Write(&defs.GPropose{
					Propose: propose,
					Reply:   writer,
					Mutex:   mutex,
					Proxy:   isProxy,
					Addr:    addr,
				})
			}
			break

		case defs.READ:
			// TODO: do something with this
			read := &defs.Read{}
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			break

		case defs.PROPOSE_AND_READ:
			// TODO: do something with this
			pr := &defs.ProposeAndRead{}
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			break

		case defs.STATS:
			r.M.Lock()
			b, _ := json.Marshal(r.Stats)
			r.M.Unlock()
			writer.Write(b)
			writer.Flush()

		default:
			p, exists := r.RPC.Get(msgType)
			if exists {
				obj := p.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				go func(obj fastrpc.Serializable) {
					time.Sleep(r.Dt.WaitDuration(addr))
					p.Chan <- obj
				}(obj)
			} else {
				r.Fatal("Error: received unknown client message ", msgType)
			}
		}
	}

	conn.Close()
	r.Println("Client down", conn.RemoteAddr())
}

func Leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func NextBallotOf(rid, oldBallot int32, repNum int) int32 {
	return (oldBallot/int32(repNum)+1)*int32(repNum) + rid
}
