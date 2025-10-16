package eppaxos

import (
	"bytes"
	//"encoding/binary"
	//"io"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	// 提供并发安全的 map，用来保存 instanceDesc
	cmap "github.com/orcaman/concurrent-map"

	"runtime/pprof"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

const MAX_INSTANCE = 10 * 1024 * 1024

const MAX_DEPTH_DEP = 10
const TRUE = uint8(1)
const FALSE = uint8(0)
const ADAPT_TIME_SEC = 10

const COMMIT_GRACE_PERIOD = 10 * 1e9 // 10 second(s)

// const (
//
//	REPLICA_ID_BITS  = 4
//	INSTANCE_ID_BITS = 28
//	INSTANCE_ID_MASK = (1 << INSTANCE_ID_BITS) - 1
//
// )
const MaxN = 5
const BF_K = 4
const BF_M_N = 32.0

const HT_INIT_SIZE = 200000

// utility to build map key from replica+instance
func instKey(replica, instance int32) string {
	return fmt.Sprintf("%d.%d", replica, instance)
}

/*
// allocate a new instDesc from pool
func (r *Replica) allocDesc() *instDesc {
	d := r.descPool.Get().(*instDesc)
	if d.msgs == nil {
		d.msgs = make(chan interface{}, 8)
	}
	if d.stopChan == nil {
		d.stopChan = make(chan *sync.WaitGroup, 4)
	}
	d.active = true
	d.seq = false
	return d
}

// free descriptor back to pool
func (r *Replica) freeDesc(d *instDesc) {
	select {
	case <-d.msgs:
	default:
	}
	r.descPool.Put(d)
}

// get or create descriptor, possibly start goroutine, and deliver message
func (r *Replica) getInstDescSeq(replica, instance int32, msg interface{}, seq bool) *instDesc {
	key := instKey(replica, instance)
	var d *instDesc

	r.cmdDescs.Upsert(key, nil, func(exists bool, v, _ interface{}) interface{} {
		if exists {
			d = v.(*instDesc)
			return d
		}
		d = r.allocDesc()
		d.id = instanceId{replica, instance}
		if replica < int32(len(r.InstanceSpace)) && instance < int32(len(r.InstanceSpace[replica])) {
			d.inst = r.InstanceSpace[replica][instance]
		}
		d.seq = seq || (r.routineCount >= MaxDescRoutines)
		if !d.seq {
			go r.handleInstDesc(d)
			r.routineCount++
		}
		return d
	})

	if msg != nil {
		if d.seq {
			r.handleInstMsg(msg, d)
		} else {
			d.msgs <- msg
		}
	}
	return d
}

// goroutine main loop
func (r *Replica) handleInstDesc(d *instDesc) {
	for d.active {
		select {
		case wg := <-d.stopChan:
			d.active = false
			wg.Done()
			r.freeDesc(d)
			return
		case m := <-d.msgs:
			// log each message processed by this instance goroutine
			r.handleInstMsg(m, d)
		}
	}
	r.freeDesc(d)
}

// dispatch message to existing EPaxos handlers
func (r *Replica) handleInstMsg(m interface{}, d *instDesc) {
	switch msg := m.(type) {
	case *Prepare:
		r.handlePrepare(msg)
	case *PreAccept:
		r.handlePreAccept(msg)
	case *Accept:
		r.handleAccept(msg)
	case *Commit:
		r.handleCommit(msg)
	case *PreAcceptReply:
		r.handlePreAcceptReply(msg)
	case *AcceptReply:
		r.handleAcceptReply(msg)
	case *PrepareReply:
		r.handlePrepareReply(msg)
	case *TryPreAccept:
		r.handleTryPreAccept(msg)
	case *TryPreAcceptReply:
		r.handleTryPreAcceptReply(msg)
	case string:
		if msg == "deliver" {
			r.deliverInstance(d)
		}
	}
}*/

// main differences with the original code base
// - fix N=3 case
// - add vbal variable (TLA spec. is wrong)
// - remove checkpoints (need to fix them first)
// - remove short commits (with N>7 propagating reach dependencies is necessary)
// - must run with thriftiness on (recovery is incorrect otherwise)
// - when conflicts are transitive skip waiting prior commuting commands

type Replica struct {
	*replica.Replica
	prepareChan           chan fastrpc.Serializable
	preAcceptChan         chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	preAcceptReplyChan    chan fastrpc.Serializable
	preAcceptOKChan       chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	tryPreAcceptChan      chan fastrpc.Serializable
	tryPreAcceptReplyChan chan fastrpc.Serializable
	prepareRPC            uint8
	prepareReplyRPC       uint8
	preAcceptRPC          uint8
	preAcceptReplyRPC     uint8
	acceptRPC             uint8
	acceptReplyRPC        uint8
	commitRPC             uint8
	tryPreAcceptRPC       uint8
	tryPreAcceptReplyRPC  uint8
	// the space of all instances (used and not yet used)
	InstanceSpace [][]*Instance
	// highest active instance numbers that this replica knows about
	crtInstance []int32
	// highest reach instance per replica that this replica knows about
	CommittedUpTo []int32
	// instance up to which all commands have been executed (including iteslf)
	ExecedUpTo       []int32
	exec             *Exec
	conflicts        []map[state.Key]*InstPair
	conflictMap      []map[state.Key][]int32
	keyHistory       map[state.Key][]InstanceRef
	maxSeqPerKey     map[state.Key]int32
	maxSeq           int32
	latestCPReplica  int32
	latestCPInstance int32
	// for synchronizing when sending replies to clients
	// from multiple go-routines
	clientMutex        *sync.Mutex
	instancesToRecover chan *instanceId
	// does this replica think it is the leader
	IsLeader      bool
	maxRecvBallot int32
	batchWait     int
	transconf     bool

	// ---------------- 以下为并发 instance 管理新增字段 ----------------
	// 保存 (replica,instance) -> *instDesc 的并发安全表，负责路由消息到对应 goroutine。
	cmdDescs cmap.ConcurrentMap
	// 已经执行过的实例集合，避免重复执行。
	delivered cmap.ConcurrentMap
	// 用于在依赖全部满足后异步触发后继 instance 的执行。
	deliverChan chan *instanceId
	// 当前活跃的 instance goroutine 数，用于限流。
	routineCount int32
	// 对 instDesc 进行复用，减少 GC 压力。
	descPool    sync.Pool
	conflictMu  sync.RWMutex
	committedMu sync.RWMutex
	// batcher aggregates outgoing small RPCs (e.g., PreAcceptReply) per peer
	batcher  *Batcher
	Waitlist map[instanceId][]*PreAcceptReply
}

type InstPair struct {
	last      int32
	lastWrite int32
}

type InstanceRef struct {
	replica  int32
	instance int32
	Write    int32
}

type Instance struct {
	Cmds []state.Command
	//bal, vbal int32
	Status int8
	//Seq            int32
	Deps [MaxN]int32
	lb   *LeaderBookkeeping
	//Index, Lowlink int
	//bfilter        any
	//proposeTime    int64
	//id           *instanceId
	reach        [MaxN]bool
	priorityOKs  bool
	preAcceptOKs int32
	//allCommitted   bool
	time time.Time
	//replicaInstance int32
}

type instanceId struct {
	replica  int32
	instance int32
}

// ------------------- goroutine-based instance descriptor -------------------
// instDesc holds runtime data for an EPaxos instance when we migrate to the
// per-instance goroutine model (inspired by swift). Each descriptor handles
// all protocol messages that belong to that (replica, instance) pair.
// The fields are intentionally minimal for the first migration step and can
// be extended (e.g., successors list, message sets) when we port more logic.
const MaxDescRoutines = 4096 // upper bound of concurrently running instance goroutines

type instDesc struct {
	// immutable identity
	id instanceId

	// pointer to the authoritative Instance struct stored in r.InstanceSpace
	inst *Instance

	// per-instance inbox; all protocol messages will be delivered here
	msgs chan interface{}

	// allows external code to gracefully stop this goroutine; each element is
	// a *sync.WaitGroup that will be Done() just before the goroutine exits.
	stopChan chan *sync.WaitGroup

	// liveness & scheduling helpers
	active bool // set to false when the goroutine should terminate
	seq    bool // true ⇒ run in caller thread instead of separate goroutine
}

type LeaderBookkeeping struct {
	clientProposals   []*defs.GPropose
	ballot            int32
	allEqual          bool
	preAcceptOKs      int
	acceptOKs         int
	nacks             int
	originalDeps      [MaxN]int32
	committedDeps     [MaxN]int32
	prepareReplies    []*PrepareReply
	preparing         bool
	tryingToPreAccept bool
	possibleQuorum    []bool
	tpaReps           int
	tpaAccepted       bool
	lastTriedBallot   int32
	cmds              []state.Command
	status            int8
	seq               int32
	deps              [MaxN]int32
	leaderResponded   bool
}

func New(alias string, id int, peerAddrList []string, exec, beacon, durable bool, batchWait int, transconf bool, failures int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{
		replica.New(alias, id, failures, peerAddrList, conf.Thrifty, exec, false, conf, logger),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*2),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		make([][]*Instance, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		nil,
		make([]map[state.Key]*InstPair, len(peerAddrList)),
		make([]map[state.Key][]int32, len(peerAddrList)),
		make(map[state.Key][]InstanceRef),
		make(map[state.Key]int32),
		0,
		0,
		-1,
		new(sync.Mutex),
		make(chan *instanceId, defs.CHAN_BUFFER_SIZE),
		false,
		-1,
		batchWait,
		transconf,
		// 初始化并发容器
		cmap.New(),
		cmap.New(),
		make(chan *instanceId, defs.CHAN_BUFFER_SIZE),
		0,
		sync.Pool{
			New: func() interface{} {
				return &instDesc{}
			},
		},
		sync.RWMutex{},
		sync.RWMutex{},
		nil,
		make(map[instanceId][]*PreAcceptReply),
	}

	// initialize batcher with buffer 4096; free callbacks nil for now
	r.batcher = NewBatcher(r, 4096, nil, nil)

	r.Beacon = beacon
	r.Durable = durable

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, MAX_INSTANCE) // FIXME
		r.crtInstance[i] = -1
		r.ExecedUpTo[i] = -1
		r.CommittedUpTo[i] = -1
		r.conflicts[i] = make(map[state.Key]*InstPair, HT_INIT_SIZE)
		r.conflictMap[i] = make(map[state.Key][]int32, HT_INIT_SIZE)
	}

	r.exec = &Exec{r}

	//cpMarker = make([]state.Command, 0)

	//register RPCs
	r.prepareRPC = r.RPC.Register(new(Prepare), r.prepareChan)
	r.prepareReplyRPC = r.RPC.Register(new(PrepareReply), r.prepareReplyChan)
	r.preAcceptRPC = r.RPC.Register(new(PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = r.RPC.Register(new(PreAcceptReply), r.preAcceptReplyChan)
	r.acceptRPC = r.RPC.Register(new(Accept), r.acceptChan)
	r.acceptReplyRPC = r.RPC.Register(new(AcceptReply), r.acceptReplyChan)
	r.commitRPC = r.RPC.Register(new(Commit), r.commitChan)
	r.tryPreAcceptRPC = r.RPC.Register(new(TryPreAccept), r.tryPreAcceptChan)
	r.tryPreAcceptReplyRPC = r.RPC.Register(new(TryPreAcceptReply), r.tryPreAcceptReplyChan)

	r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"], r.Stats.M["totalCommitTime"], r.Stats.M["totalBatching"], r.Stats.M["totalBatchingSize"] = 0, 0, 0, 0, 0, 0, 0

	go r.run()

	return r
}

/*
	func (r *Replica) recordInstanceMetadata(inst *Instance) {
		if !r.Durable {
			return
		}

		b := make([]byte, 9+r.N*4)
		binary.LittleEndian.PutUint32(b[0:4], uint32(inst.bal))
		binary.LittleEndian.PutUint32(b[0:4], uint32(inst.vbal))
		b[4] = byte(inst.Status)
		//binary.LittleEndian.PutUint32(b[5:9], uint32(inst.Seq))
		l := 9
		for _, dep := range inst.Deps {
			binary.LittleEndian.PutUint32(b[l:l+4], uint32(dep))
			l += 4
		}
		r.StableStore.Write(b[:])
	}

	func (r *Replica) recordCommands(cmds []state.Command) {
		if !r.Durable {
			return
		}

		if cmds == nil {
			return
		}
		for i := 0; i < len(cmds); i++ {
			cmds[i].Marshal(io.Writer(r.StableStore))
		}
	}

	func (r *Replica) sync() {
		if !r.Durable {
			return
		}

		r.StableStore.Sync()
	}
*/
var fastClockChan chan bool
var slowClockChan chan bool

/*
func (r *Replica) fastClock() {
	r.Printf("fastClock")
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond)
		fastClockChan <- true
	}
}
func (r *Replica) slowClock() {
	r.Printf("slowClock")
	for !r.Shutdown {
		time.Sleep(150 * time.Millisecond)
		slowClockChan <- true
	}
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	r.Println("r.PreferredPeerOrder", r.PreferredPeerOrder)
}*/

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

func (r *Replica) run() {
	r.PrintDebug("loop top", time.Now())
	r.ConnectToPeers()

	r.ComputeClosestPeers()

	//if r.Exec {
	//	go r.executeCommands()
	//}

	//slowClockChan = make(chan bool, 1)
	//fastClockChan = make(chan bool, 1)
	//go r.slowClock()

	//if r.BatchingEnabled() {
	//	go r.fastClock()
	//}

	//if r.Beacon {
	//	go r.stopAdapting()
	//}

	onOffProposeChan := r.ProposeChan

	// heartbeat ticker to observe run() liveness and queue sizes
	hbTicker := time.NewTicker(5 * time.Millisecond)
	defer hbTicker.Stop()

	var lastBeat atomic.Value // stores time.Time
	lastBeat.Store(time.Now())
	// watchdog: if no heartbeat for > 30ms, dump goroutines once
	go func() {
		wdTicker := time.NewTicker(5 * time.Millisecond)
		defer wdTicker.Stop()
		for range wdTicker.C {
			if t, _ := lastBeat.Load().(time.Time); !t.IsZero() && time.Since(t) > 30*time.Millisecond {
				var buf bytes.Buffer
				pprof.Lookup("goroutine").WriteTo(&buf, 2)
				//r.PrintDebug("goroutine dump due to missed heartbeats >30ms", buf.String())
				// avoid连续刷栈，等下一次心跳重置
				lastBeat.Store(time.Now())
			}
		}
	}()

	go r.WaitForClientConnections()

	for !r.Shutdown {

		select {
		case <-hbTicker.C:
			// periodic heartbeat: shows run loop is alive and current queue lengths
			now := time.Now()
			lastBeat.Store(now)
			//r.PrintDebug("run heartbeat", "time", now, "preAcceptReplyLen", len(r.preAcceptReplyChan))

		case propose := <-onOffProposeChan:
			//r.PrintDebug("received propose time", time.Now())
			r.handlePropose(propose)
			if r.BatchingEnabled() {
				onOffProposeChan = nil
			}

		case <-fastClockChan:
			onOffProposeChan = r.ProposeChan

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*Prepare)
			r.handlePrepare(prepare)

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*PrepareReply)
			r.handlePrepareReply(prepareReply)

		case preAcceptReplyS := <-r.preAcceptReplyChan:
			r.PrintDebug("received preAcceptReply time", time.Now(), "time since recv", time.Since(preAcceptReplyS.(*PreAcceptReply).RecvTs))
			preAcceptReply := preAcceptReplyS.(*PreAcceptReply)
			r.handlePreAcceptReply(preAcceptReply)

		//case acceptReplyS := <-r.acceptReplyChan:
		//	acceptReply := acceptReplyS.(*AcceptReply)
		//	r.handleAcceptReply(acceptReply)

		case tryPreAcceptS := <-r.tryPreAcceptChan:
			tryPreAccept := tryPreAcceptS.(*TryPreAccept)
			r.handleTryPreAccept(tryPreAccept)

		case tryPreAcceptReplyS := <-r.tryPreAcceptReplyChan:
			tryPreAcceptReply := tryPreAcceptReplyS.(*TryPreAcceptReply)
			r.handleTryPreAcceptReply(tryPreAcceptReply)

		case beacon := <-r.BeaconChan:
			r.ReplyBeacon(beacon)

		case <-slowClockChan:
			if r.Beacon {
				r.Printf("weird %d; conflicted %d; slow %d; fast %d\n", r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"])
				for q := int32(0); q < int32(r.N); q++ {
					if q == r.Id {
						continue
					}
					r.SendBeacon(q)
				}
			}

		case iid := <-r.instancesToRecover:
			r.startRecoveryForInstance(iid.replica, iid.instance)

			//case iid := <-r.deliverChan:
			// route deliver trigger
			//	r.getInstDescSeq(iid.replica, iid.instance, "deliver", false)
		}
	}
}

/*
	func isInitialBallot(ballot int32, replica int32) bool {
		return ballot == replica
	}
*/
func (r *Replica) makeBallot(replica int32, instance int32) {
	lb := r.InstanceSpace[replica][instance].lb
	n := r.Id
	if r.Id != replica {
		n += int32(r.N)
	}
	if r.IsLeader {
		for n < r.maxRecvBallot {
			n += int32(r.N)
		}
	}
	lb.lastTriedBallot = n
}

func (r *Replica) replyPrepare(replicaId int32, reply *PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) updateCommitted(replica int32) {
	r.committedMu.RLock()
	defer r.committedMu.RUnlock()
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == EXECUTED) {
		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
	}
}

func (r *Replica) updateConflicts(cmds []state.Command, replica int32, instance int32) {
	r.PrintDebug("updateConflicts", "replica", replica, "instance", instance)
	r.conflictMu.Lock()
	defer r.conflictMu.Unlock()
	for i := 0; i < len(cmds); i++ {
		//r.PrintDebug("updateConflicts time", time.Now())
		if dpair, present := r.conflicts[replica][cmds[i].K]; present {
			//r.PrintDebug("replica", replica, "instance", instance, "cmds", i, "key", cmds[i].K, "dpair.last", dpair.last, "dpair.lastWrite", dpair.lastWrite)
			if dpair.last < instance {
				r.conflicts[replica][cmds[i].K].last = instance
			}
			if dpair.lastWrite < instance && cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		} else {
			//r.PrintDebug("replica", replica, "instance", instance, "cmds", i, "key", cmds[i].K)
			r.conflicts[replica][cmds[i].K] = &InstPair{
				last:      instance,
				lastWrite: -1,
			}
			if cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		}

		r.conflictMap[replica][cmds[i].K] = append(r.conflictMap[replica][cmds[i].K], instance)

	}
}

func (r *Replica) updateAttributes(cmds []state.Command, deps [MaxN]int32, replica int32) ([MaxN]int32, bool) {
	r.conflictMu.RLock()
	defer r.conflictMu.RUnlock()
	changed := false
	for q := 0; q < r.N; q++ {
		if r.Id != replica && int32(q) == replica {
			continue
		}
		for i := 0; i < len(cmds); i++ {
			if dpair, present := (r.conflicts[q])[cmds[i].K]; present {
				//r.PrintDebug("updateAttributes q", q, "replica", replica, "cmds", i, "key", cmds[i].K, "dpair.last", dpair.last, "dpair.lastWrite", dpair.lastWrite)
				d := dpair.lastWrite
				if cmds[i].Op != state.GET {
					d = dpair.last
				}

				if d > deps[q] {
					//r.PrintDebug("updateAttributes q", q, "replica", replica, "cmds", i, "key", cmds[i].K, "d", d, "deps[q]", deps[q])
					deps[q] = d
					for j := 0; j < len(r.InstanceSpace[q][d].Deps); j++ {
						if r.InstanceSpace[q][d].Deps[j] > deps[j] {
							deps[j] = r.InstanceSpace[q][d].Deps[j]
						}
					}
					changed = true
				}
			}
		}
	}

	return deps, changed
}

func (r *Replica) updatePriority(pareply *PreAcceptReply, cmds []state.Command, deps [MaxN]int32, replica int32, instance int32) (int32, bool) {
	//r.PrintDebug("updatePriority", "deps", deps, "replica", replica, "instance", instance)
	r.conflictMu.RLock()
	defer r.conflictMu.RUnlock()

	for i := 0; i < len(cmds); i++ {
		if dpair, present := r.conflicts[r.Id][cmds[i].K]; present {

			d := dpair.lastWrite
			if cmds[i].Op != state.GET {
				d = dpair.last
			}
			r.PrintDebug("updatePriority q", r.Id, "cmds", i, "key", cmds[i].K, "state.GET", state.GET, "dpair.last", dpair.last, "dpair.lastWrite", dpair.lastWrite, "d", d, "deps", deps)
			if d > deps[r.Id] {
				if r.InstanceSpace[r.Id][d].Deps[replica] >= instance {
					continue
				}

				//r.PrintDebug("updatePriority3", "j", j, "r.InstanceSpace[q][j].Deps[replica]", r.InstanceSpace[q][j].Deps[replica], "instance", instance, "state.GET", state.GET)

				//r.PrintDebug("conflictmap", conflictmap)
				//r.PrintDebug("q", q, "j", j, "r.InstanceSpace[q][j].Deps", r.InstanceSpace[q][j].Deps)
				priority := false
				for k := 0; k < len(r.InstanceSpace[r.Id][d].Cmds); k++ {
					//r.PrintDebug("222k", k, "r.InstanceSpace[r.Id][d].Cmds[k].K", r.InstanceSpace[r.Id][d].Cmds[k].K, "cmds[i].K", cmds[i].K, "r.InstanceSpace[r.Id][d].Cmds[k].Op", r.InstanceSpace[r.Id][d].Cmds[k].Op, "cmds[i].Op", cmds[i].Op)
					if r.InstanceSpace[r.Id][d].Cmds[k].K == cmds[i].K {
						if r.InstanceSpace[r.Id][d].Cmds[k].Op != state.GET || cmds[i].Op != state.GET {
							priority = true
							break
						}
					}
				}
			flag:
				for k := replica + 1; k < int32(r.N); k++ {
					r.PrintDebug("333k", k, "r.InstanceSpace[r.Id][d].Deps[k]", r.InstanceSpace[r.Id][d].Deps[k], time.Now())
					for j := deps[k] + 1; j <= r.InstanceSpace[r.Id][d].Deps[k]; j++ {
						r.PrintDebug("444k", k, "j", j)
						if r.InstanceSpace[k][j] == nil || len(r.InstanceSpace[k][j].Cmds) == 0 {
							r.PrintDebug("reach == false", "k", k, "j", j)
							r.Waitlist[instanceId{k, j}] = append(r.Waitlist[instanceId{k, j}], pareply)
							return deps[r.Id], false
						}
						for l := 0; l < len(r.InstanceSpace[k][j].Cmds); l++ {
							r.PrintDebug("555k", k, "j", j, "l", l, "r.InstanceSpace[k][j].Cmds[l].Op", r.InstanceSpace[k][j].Cmds[l].Op, "cmds[i].Op", cmds[i].Op)
							if r.InstanceSpace[k][j].Cmds[l].K == cmds[i].K {
								if r.InstanceSpace[k][j].Cmds[l].Op != state.GET || cmds[i].Op != state.GET {
									priority = false
									break flag
								}
							}
						}
					}
				}
				if priority {
					//r.PrintDebug("updatePriority4", "replica", replica, "instance", instance, "d", d, "deps[replica]", deps[replica])
					if deps[r.Id] < d {
						deps[r.Id] = d
					}
				}
			}
		}
	}
	return deps[r.Id], true
}

/*
	func (r *Replica) updatePriority(cmds []state.Command, deps []int32, replica int32, instance int32) []int32 {
		r.PrintDebug("updatePriority", "deps", deps, "replica", replica, "instance", instance)
		r.conflictMu.RLock()
		defer r.conflictMu.RUnlock()
		conflictmap := make([][]int32, r.N)
		for i := 0; i < len(cmds); i++ {
			for q := r.N - 1; q >= 0; q-- {
				if r.Id != replica && int32(q) == replica {
					continue
				}
				if dpair, present := (r.conflicts[q])[cmds[i].K]; present {

					d := dpair.lastWrite
					if cmds[i].Op != state.GET {
						d = dpair.last
					}
					r.PrintDebug("updatePriority q", q, "cmds", i, "key", cmds[i].K, "state.GET", state.GET, "dpair.last", dpair.last, "dpair.lastWrite", dpair.lastWrite, "d", d, "deps[q]", deps[q])
					if d > deps[q] {
						if int32(q) > replica { //优先级更高
							for j := deps[q] + 1; j <= d; j++ {
								if r.InstanceSpace[q][j] == nil {
									continue
								}
								r.PrintDebug("updatePriority2", "j", j, "r.InstanceSpace[q][j].Deps[replica]", r.InstanceSpace[q][j].Deps[replica], "instance", instance, "state.GET", state.GET)

								for k := 0; k < len(r.InstanceSpace[q][j].Cmds); k++ {
									if r.InstanceSpace[q][j].Cmds[k].K == cmds[i].K {
										if r.InstanceSpace[q][j].Cmds[k].Op != state.GET || cmds[i].Op != state.GET {
											conflictmap[q] = append(conflictmap[q], j)
											break
										}
									}
								}
								//r.PrintDebug("updatePriority3", "j", j, "r.InstanceSpace[q][j].Deps[replica]", r.InstanceSpace[q][j].Deps[replica], "instance", instance)
								//r.InstanceSpace[q][j].Deps[replica] = instance
							}
						} else if int32(q) < replica && int32(q) == r.Id { //优先级更低
							for j := deps[q] + 1; j <= d; j++ {
								if r.InstanceSpace[q][j] == nil || r.InstanceSpace[q][j].Deps[replica] >= instance {
									continue
								}

								r.PrintDebug("updatePriority3", "j", j, "r.InstanceSpace[q][j].Deps[replica]", r.InstanceSpace[q][j].Deps[replica], "instance", instance, "state.GET", state.GET)

								//r.PrintDebug("conflictmap", conflictmap)
								//r.PrintDebug("q", q, "j", j, "r.InstanceSpace[q][j].Deps", r.InstanceSpace[q][j].Deps)
								priority := false

								for k := 0; k < len(r.InstanceSpace[q][j].Cmds); k++ {
									if r.InstanceSpace[q][j].Cmds[k].K == cmds[i].K {
										if r.InstanceSpace[q][j].Cmds[k].Op != state.GET || cmds[i].Op != state.GET {
											priority = true
											break
										}
									}
								}

								for k := replica + 1; k < int32(r.N); k++ {
									if conflictmap[k] != nil {
										for _, l := range conflictmap[k] {
											r.PrintDebug("l", l, "r.InstanceSpace[q][j].Deps[k]", r.InstanceSpace[q][j].Deps[k])
											if r.InstanceSpace[q][j].Deps[k] == l {
												priority = false
												break
											}
										}
									}
									if !priority {
										break
									}
								}
								if priority {
									r.PrintDebug("updatePriority4", "j", j, "r.InstanceSpace[q][j].Deps[replica]", r.InstanceSpace[q][j].Deps[replica], "instance", instance, "d", d, "deps[q]", deps[q])
									deps[q] = j
								}

							}
						}
					}
				}
			}
		}
		return deps
	}
*/
func (r *Replica) mergeAttributes(deps1 [MaxN]int32, deps2 [MaxN]int32) ([MaxN]int32, bool) {
	equal := true
	for q := 0; q < r.N; q++ {
		if int32(q) == r.Id {
			continue
		}
		if deps1[q] != deps2[q] {
			equal = false
			if deps2[q] > deps1[q] {
				deps1[q] = deps2[q]
			}
		}
	}
	return deps1, equal
}

func equal(deps1 [MaxN]int32, deps2 [MaxN]int32) bool {
	for i := 0; i < len(deps1); i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

func (r *Replica) handlePropose(propose *defs.GPropose) {
	//TODO!! Handle client retries
	r.PrintDebug("handlePropose time", time.Now())
	batchSize := len(r.ProposeChan) + 1
	//batchSize := 1
	/*r.M.Lock()
	r.Stats.M["totalBatching"]++
	r.Stats.M["totalBatchingSize"] += batchSize
	r.M.Unlock()*/

	r.crtInstance[r.Id]++
	//r.PrintDebug("r.Id", r.Id, "instance", r.crtInstance[r.Id])
	cmds := make([]state.Command, batchSize)
	proposals := make([]*defs.GPropose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	//deps := make([]int32, r.N)

	comDeps := [MaxN]int32{}
	for i := 0; i < r.N; i++ {
		comDeps[i] = -1
	}

	inst := r.newInstance(r.Id, r.crtInstance[r.Id], cmds, PREACCEPTED, [MaxN]int32{})
	inst.lb = r.newLeaderBookkeeping(proposals, inst.Deps, comDeps, inst.Deps, r.Id, cmds, PREACCEPTED, -1)
	r.InstanceSpace[r.Id][r.crtInstance[r.Id]] = inst

	for q := 0; q < r.N; q++ {
		inst.Deps[q] = int32(-1)
	}
	inst.Deps, _ = r.updateAttributes(cmds, inst.Deps, r.Id)
	inst.Deps[r.Id] = r.crtInstance[r.Id] - 1
	inst.reach[r.Id] = true
	//r.PrintDebug("deps", deps)
	r.updateConflicts(cmds, r.Id, r.crtInstance[r.Id])

	//r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
	//r.recordCommands(cmds)
	//r.sync()
	//r.PrintDebug("startPhase1 time", time.Now())
	//r.PrintDebug("r.PeerLatencies", r.PeerLatencies)
	r.PrintDebug("start pareply.Replica", r.Id, "pareply.Instance", r.crtInstance[r.Id], "inst.Deps", inst.Deps, "r.CommittedUpTo", r.CommittedUpTo)
	reply := &PreAcceptReply{
		Replica:  int8(r.Id),
		Instance: r.crtInstance[r.Id],
		Command:  inst.Cmds,
		Deps:     inst.Deps,
	}
	//r.PrintDebug("r.Latencies", r.Latencies)

	for i := 0; i < r.N; i++ {
		if i != int(r.Id) {
			r.sendPreAcceptReply(int32(i), reply)
		}
	}
}

func (r *Replica) handlePreAcceptReply(pareply *PreAcceptReply) {
	r.PrintDebug("handlePreAcceptReply time", time.Now())
	//r.PrintDebug("handlePreAcceptReply")
	//r.PrintDebug("pareply", pareply)
	inst := r.InstanceSpace[int32(pareply.Replica)][pareply.Instance]

	if inst == nil {
		//r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(int32(pareply.Replica), pareply.Instance)
		r.InstanceSpace[int32(pareply.Replica)][pareply.Instance] = inst
	}

	if len(pareply.Command) > 0 {

		reply := &PreAcceptReply{
			Replica:  pareply.Replica,
			Instance: pareply.Instance,
		}

		CMDReach := true

		if int32(pareply.Replica) > r.Id {
			inst.Deps[r.Id], CMDReach = r.updatePriority(pareply, pareply.Command, pareply.Deps, int32(pareply.Replica), pareply.Instance)

			reply.SenderDep = inst.Deps[r.Id]
		}

		if !CMDReach {
			return
		}

		if pareply.Instance > r.crtInstance[int32(pareply.Replica)] {
			r.crtInstance[int32(pareply.Replica)] = pareply.Instance
		}

		for i := 0; i < r.N; i++ {
			if inst.Deps[i] == -1 {
				inst.Deps[i] = pareply.Deps[i]
			}
		}

		inst.Cmds = pareply.Command
		inst.Status = PREACCEPTED_EQ
		r.updateConflicts(pareply.Command, int32(pareply.Replica), pareply.Instance)

		waitlist := r.Waitlist[instanceId{int32(pareply.Replica), pareply.Instance}]
		delete(r.Waitlist, instanceId{int32(pareply.Replica), pareply.Instance})
		for _, w := range waitlist {
			r.PrintDebug("handlePreAcceptReply waitlist", w)
			r.PrintDebug("pareply.Deps", pareply.Deps, "pareply.Replica", pareply.Replica, "pareply.Instance", pareply.Instance)
			r.handlePreAcceptReply(w)
		}

		//r.PrintDebug("sendPreAcceptReply")
		//r.PrintDebug("reply", reply)
		r.PrintDebug("handlePreAcceptReply time5", time.Now())
		if !r.Thrifty {
			for i := 0; i < r.N; i++ {
				if i != int(r.Id) {
					r.sendPreAcceptReply(int32(i), reply)
				}
			}
		} else {
			flag := false
			for _, vec := range r.PeerLatencies {
				if vec == nil {
					flag = true
					continue
				}
			}
			if flag {
				for i := 0; i < r.N; i++ {
					if i != int(r.Id) {
						r.sendPreAcceptReply(int32(i), reply)
					}
				}
			} else {
				if int32(pareply.Replica) > r.Id {
					for i := 0; i < r.N; i++ {
						if i != int(r.Id) {
							r.sendPreAcceptReply(int32(i), reply)
						}
					}
				} else {
					if int32(pareply.Replica) < int32((r.N-1)/2) {
						for i := 0; i < r.N; i++ {
							if i == int(r.Id) {
								continue
							}

							close := 0
							for j := int32(pareply.Replica) + 1; j < int32(len(r.PeerLatencies[int32(i)])); j++ {
								if r.PeerLatencies[int32(i)][j]+r.PeerLatencies[int32(pareply.Replica)][j] < r.PeerLatencies[int32(i)][r.Id]+r.PeerLatencies[int32(pareply.Replica)][r.Id] {
									close++
								}
							}
							//r.PrintDebug("pareply.Replica", pareply.Replica, "close", close, "i", i)
							if int32(pareply.Replica)+int32(close) < int32((r.N-1)/2) {
								r.sendPreAcceptReply(int32(i), reply)
							}
						}
					}
				}
			}
		}
	}

	inst.reach[r.Id] = true
	inst.reach[pareply.Replica] = true
	inst.reach[pareply.Sender] = true
	if pareply.Sender < pareply.Replica {
		inst.Deps[pareply.Sender] = pareply.SenderDep
	}

	r.PrintDebug("pareply.Sender", pareply.Sender)
	r.PrintDebug("inst.reach", inst.reach)
	r.PrintDebug("inst.Deps", inst.Deps)

	if inst.Status >= COMMITTED || inst.Cmds == nil {
		return
	}

	r.Commit(inst, pareply.Replica, pareply.Instance)

}

func (r *Replica) Commit(inst *Instance, Replica int8, Instance int32) {

	inst.preAcceptOKs = 0
	for i := 0; i < r.N; i++ {
		if inst.reach[i] {
			inst.preAcceptOKs++
		}
	}

	inst.priorityOKs = true
	for i := 0; i < int(int32(Replica)); i++ {
		if !inst.reach[i] {
			inst.priorityOKs = false
		}
	}

	//r.PrintDebug("handlePreAcceptReply7")

	//r.PrintDebug("pareply.CommittedUpTo", pareply.CommittedUpTo)

	r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
	r.PrintDebug("inst.Deps", inst.Deps)
	var allCommitted bool = true
	for q := 0; q < r.N; q++ {
		if r.CommittedUpTo[q] < inst.Deps[q] {
			allCommitted = false
		}
	}

	precondition := inst.priorityOKs && allCommitted && inst.preAcceptOKs > int32(r.N/2)
	r.PrintDebug("handlePreAcceptReply2 time", time.Now())
	r.PrintDebug("pareply.Replica", Replica, "pareply.Instance", Instance, "priorityOKs", inst.priorityOKs, "allCommitted", allCommitted, "preAcceptOKs", inst.preAcceptOKs, "precondition", precondition)
	if precondition && inst.Status <= PREACCEPTED_EQ {
		r.PrintDebug("pareply.Replica", Replica, "pareply.Instance", Instance, "final deps", inst.Deps)
		if r.Id == int32(Replica) {
			r.PrintDebug("latency time", time.Since(inst.time), time.Now())
		}
		inst.Status = COMMITTED
		r.updateCommitted(int32(Replica))

		if int32(Replica) == r.Id {

			key := instKey(int32(Replica), Instance)
			r.delivered.Set(key, struct{}{})

			for idx := 0; idx < len(inst.lb.clientProposals); idx++ {
				prop := inst.lb.clientProposals[idx]
				var retVal state.Value
				if prop.Command.Op != state.PUT {
					retVal = prop.Command.Execute(r.State) // GET / SCAN
				} else {
					prop.Command.Execute(r.State) // PUT 落库
					retVal = state.NIL()          // 只需 ACK
				}

				r.ReplyProposeTS(
					&defs.ProposeReplyTS{
						OK:        TRUE,
						CommandId: prop.CommandId,
						Value:     retVal,
						Timestamp: prop.Timestamp},
					prop.Reply,
					prop.Mutex)
			}
		}
		for p := 0; p < r.N; p++ {
			//只需要检查每个 replica 第一个没 commit 的就行
			r.PrintDebug("p", p, "inst.Deps[p]", inst.Deps[p], "r.crtInstance[p]", r.crtInstance[p])
			for j := inst.Deps[p] + 1; j <= r.crtInstance[p]; j++ {

				if r.InstanceSpace[p][j] != nil && r.InstanceSpace[p][j].preAcceptOKs > int32(r.N/2) && r.InstanceSpace[p][j].priorityOKs && r.InstanceSpace[p][j].Deps[int32(Replica)] == Instance && r.InstanceSpace[p][j].Status <= PREACCEPTED_EQ {
					r.PrintDebug("retry preaccept")
					r.Commit(r.InstanceSpace[p][j], int8(p), j)
					break
				}

			}
		}

	}
}

/**********************************************************************

                     RECOVERY ACTIONS

***********************************************************************/

func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	r.IsLeader = true
	r.Println("I am the leader")
	return nil
}

func (r *Replica) startRecoveryForInstance(replica int32, instance int32) {
	r.PrintDebug("startRecoveryForInstance", replica, instance)
	inst := r.InstanceSpace[replica][instance]
	if inst == nil {
		inst = r.newInstanceDefault(replica, instance)
		r.InstanceSpace[replica][instance] = inst
	} else if inst.Status >= COMMITTED && inst.Cmds != nil {
		r.Printf("No need to recover %d.%d", replica, instance)
		return
	}

	// no TLA guidance here (some difference with the original implementation)
	var proposals []*defs.GPropose = nil
	if inst.lb != nil {
		proposals = inst.lb.clientProposals
	}
	inst.lb = r.newLeaderBookkeepingDefault()
	lb := inst.lb
	lb.clientProposals = proposals
	//lb.ballot = inst.vbal
	//lb.seq = inst.Seq
	lb.cmds = inst.Cmds
	//lb.deps = inst.Deps
	lb.status = inst.Status
	r.makeBallot(replica, instance)

	//inst.bal = lb.lastTriedBallot
	//inst.vbal = lb.lastTriedBallot
	/*preply := &PrepareReply{
	r.Id,
	replica,
	instance,
	int32(0),
	int32(0),
	inst.Status,
	inst.Cmds,
	int32(0),
	inst.Deps}*/

	//lb.prepareReplies = append(lb.prepareReplies, preply)
	lb.leaderResponded = r.Id == replica

	//r.bcastPrepare(replica, instance)
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	r.PrintDebug("handlePrepare", prepare.Replica, prepare.Instance, prepare.Ballot)
	inst := r.InstanceSpace[prepare.Replica][prepare.Instance]
	var preply *PrepareReply

	if prepare.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = prepare.Ballot
	}

	if inst == nil {
		r.InstanceSpace[prepare.Replica][prepare.Instance] = r.newInstanceDefault(prepare.Replica, prepare.Instance)
		inst = r.InstanceSpace[prepare.Replica][prepare.Instance]
	}

	/*preply = &PrepareReply{
	r.Id,
	prepare.Replica,
	prepare.Instance,
	int32(0),
	int32(0),
	inst.Status,
	inst.Cmds,
	int32(0),
	inst.Deps}*/
	r.replyPrepare(prepare.LeaderId, preply)
	r.SendMsg(prepare.LeaderId, r.prepareReplyRPC, preply)
}

func (r *Replica) handlePrepareReply(preply *PrepareReply) {
	r.PrintDebug("handlePrepareReply", preply.Replica, preply.Instance, preply.Ballot)
	inst := r.InstanceSpace[preply.Replica][preply.Instance]
	lb := inst.lb

	if preply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = preply.Ballot
	}

	if inst == nil || lb == nil || !lb.preparing {
		return
	}

	if preply.Ballot != lb.lastTriedBallot {
		lb.nacks++
		return
	}

	lb.prepareReplies = append(lb.prepareReplies, preply)
	if len(lb.prepareReplies) < r.Replica.SlowQuorumSize() {
		return
	}

	lb.preparing = false

	// Deal with each sub-cases in order of the (corrected) TLA specification
	// only replies from the highest ballot are taken into account
	// 1 -> reach/executed
	// 2 -> accepted
	// 3 -> pre-accepted > f (not including the leader) and allEqual
	// 4 -> pre-accepted >= f/2 (not including the leader) and allEqual
	// 5 -> pre-accepted > 0 and (disagreeing or leader replied or pre-accepted < f/2)
	// 6 -> none of the above
	preAcceptCount := 0
	subCase := 0
	allEqual := true
	for _, element := range lb.prepareReplies {
		if element.VBallot >= lb.ballot {
			lb.ballot = element.VBallot
			lb.cmds = element.Command
			lb.seq = element.Seq
			//lb.deps = element.Deps
			lb.status = element.Status
		}
		if element.AcceptorId == element.Replica {
			lb.leaderResponded = true
		}
		if element.Status == PREACCEPTED_EQ || element.Status == PREACCEPTED {
			preAcceptCount++
		}
	}

	if lb.status >= COMMITTED { // 1
		subCase = 1
	} else if lb.status == ACCEPTED { // 2
		subCase = 2
	} else if lb.status == PREACCEPTED || lb.status == PREACCEPTED_EQ {
		for _, element := range lb.prepareReplies {
			if element.VBallot == lb.ballot && element.Status >= PREACCEPTED {
				//_, equal := r.mergeAttributes(lb.deps, element.Deps)
				//if !equal {
				//	allEqual = false
				//	break
				//}
			}
		}
		if preAcceptCount >= r.Replica.SlowQuorumSize()-1 && !lb.leaderResponded && allEqual {
			subCase = 3
		} else if preAcceptCount >= r.Replica.SlowQuorumSize()-1 && !lb.leaderResponded && allEqual {
			subCase = 4
		} else if preAcceptCount > 0 && (lb.leaderResponded || !allEqual || preAcceptCount < r.Replica.SlowQuorumSize()-1) {
			subCase = 5
		} else {
			panic("Cannot occur")
		}
	} else if lb.status == NONE {
		subCase = 6
	} else {
		panic("Status unknown")
	}

	// if subCase != 5 {
	// 	dlog.Printf("In %d.%d, sub-case %d\n", preply.Replica, preply.Instance, subCase)
	// } else {
	// 	dlog.Printf("In %d.%d, sub-case %d with (leaderResponded=%t, allEqual=%t, enough=%t)\n",
	// 		preply.Replica, preply.Instance, subCase, lb.leaderResponded, allEqual, preAcceptCount < r.Replica.SlowQuorumSize()-1)
	// }

	inst.Cmds = lb.cmds
	//inst.bal = lb.lastTriedBallot
	//inst.vbal = lb.lastTriedBallot
	//inst.Seq = lb.seq
	//inst.Deps = lb.deps
	inst.Status = lb.status

	if subCase == 1 {
		// nothing to do
	} else if subCase == 2 || subCase == 3 {
		inst.Status = ACCEPTED
		lb.status = ACCEPTED
		//r.bcastAccept(preply.Replica, preply.Instance)
	} else if subCase == 4 {
		lb.tryingToPreAccept = true
		//r.bcastTryPreAccept(preply.Replica, preply.Instance)
	} else { // subCase 5 and 6
		//cmd := state.NOOP()
		if inst.lb.cmds != nil {
			//cmd = inst.lb.cmds
		}
		r.PrintDebug("startPhase1 in handlePreAccept")
		//r.startPhase1(cmd, preply.Replica, preply.Instance, lb.lastTriedBallot, lb.clientProposals)
	}
}

func (r *Replica) handleTryPreAccept(tpa *TryPreAccept) {
	r.PrintDebug("handleTryPreAccept", tpa.Replica, tpa.Instance, tpa.Ballot)
	inst := r.InstanceSpace[tpa.Replica][tpa.Instance]

	if inst == nil {
		r.InstanceSpace[tpa.Replica][tpa.Instance] = r.newInstanceDefault(tpa.Replica, tpa.Instance)
		inst = r.InstanceSpace[tpa.Replica][tpa.Instance]
	}

	/*if inst.bal > tpa.Ballot {
		//r.Printf("Smaller ballot %d < %d\n", tpa.Ballot, inst.bal)
		return
	}
	inst.bal = tpa.Ballot
	*/
	confRep := int32(0)
	confInst := int32(0)
	confStatus := NONE
	if inst.Status == NONE { // missing in TLA spec.
		if conflict, cr, ci := r.findPreAcceptConflicts(tpa.Command, tpa.Replica, tpa.Instance, tpa.Seq, tpa.Deps); conflict {
			confRep = cr
			confInst = ci
		} else {
			if tpa.Instance > r.crtInstance[tpa.Replica] {
				r.crtInstance[tpa.Replica] = tpa.Instance
			}
			inst.Cmds = tpa.Command
			//inst.Seq = tpa.Seq
			//inst.Deps = tpa.Deps
			inst.Status = PREACCEPTED
		}
	}

	rtpa := &TryPreAcceptReply{r.Id, tpa.Replica, tpa.Instance, int32(0), int32(0), confRep, confInst, confStatus}

	// r.replyTryPreAccept(tpa.LeaderId, rtpa)
	r.SendMsg(tpa.LeaderId, r.tryPreAcceptReplyRPC, rtpa)

}

func (r *Replica) findPreAcceptConflicts(cmds []state.Command, replica int32, instance int32, seq int32, deps []int32) (bool, int32, int32) {
	inst := r.InstanceSpace[replica][instance]
	if inst != nil && len(inst.Cmds) > 0 {
		if inst.Status >= ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}
		//if equal(inst.Deps, deps) {
		// already PRE-ACCEPTED, no point looking for conflicts again
		//	return false, replica, instance
		//}
	}
	for q := int32(0); q < int32(r.N); q++ {
		for i := r.ExecedUpTo[q]; i <= r.crtInstance[q]; i++ { // FIXME this is not enough imho.
			if i == -1 {
				//do not check placeholder
				continue
			}
			if replica == q && instance == i {
				// no point checking past instance in replica's row, since replica would have
				// set the dependencies correctly for anything started after instance
				break
			}
			if i == deps[q] {
				//the instance cannot be a dependency for itself
				continue
			}
			inst := r.InstanceSpace[q][i]
			if inst == nil || inst.Cmds == nil || len(inst.Cmds) == 0 {
				continue
			}
			if inst.Deps[replica] >= instance {
				// instance q.i depends on instance replica.instance, it is not a conflict
				continue
			}
			if r.LRead || state.ConflictBatch(inst.Cmds, cmds) {
				if i > deps[q] ||
					(i < deps[q] && (q != replica || inst.Status > PREACCEPTED_EQ)) {
					// this is a conflict
					return true, q, i
				}
			}
		}
	}
	return false, -1, -1
}

func (r *Replica) handleTryPreAcceptReply(tpar *TryPreAcceptReply) {
	r.PrintDebug("handleTryPreAcceptReply", tpar.Replica, tpar.Instance, tpar.Ballot)
	inst := r.InstanceSpace[tpar.Replica][tpar.Instance]

	if tpar.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = tpar.Ballot
	}

	if inst == nil {
		r.InstanceSpace[tpar.Replica][tpar.Instance] = r.newInstanceDefault(tpar.Replica, tpar.Instance)
		inst = r.InstanceSpace[tpar.Replica][tpar.Instance]
	}

	lb := inst.lb
	if lb == nil || !lb.tryingToPreAccept {
		return
	}

	if tpar.Ballot != lb.lastTriedBallot {
		return
	}

	lb.tpaReps++

	if tpar.VBallot == lb.lastTriedBallot {
		//lb.preAcceptOKs++
		if lb.preAcceptOKs >= r.N/2 {
			//it's safe to start Accept phase
			lb.status = ACCEPTED
			lb.tryingToPreAccept = false
			lb.acceptOKs = 0

			inst.Cmds = lb.cmds
			//inst.Seq = lb.seq
			//inst.Deps = lb.deps
			inst.Status = lb.status
			//inst.vbal = lb.lastTriedBallot
			//inst.bal = lb.lastTriedBallot

			//r.bcastAccept(tpar.Replica, tpar.Instance)
			return
		}
	} else {
		lb.nacks++
		lb.possibleQuorum[tpar.AcceptorId] = false
		lb.possibleQuorum[tpar.ConflictReplica] = false
	}

	lb.tpaAccepted = lb.tpaAccepted || (tpar.ConflictStatus >= ACCEPTED) // TLA spec. (page 39)

	if lb.tpaReps >= r.Replica.SlowQuorumSize()-1 && lb.tpaAccepted {
		//abandon recovery, restart from phase 1
		lb.tryingToPreAccept = false
		r.PrintDebug("startPhase1 in handleTryPreAcceptReply")
		//r.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
		return
	}

	// the code below is not checked in TLA (liveness)
	notInQuorum := 0
	for q := 0; q < r.N; q++ {
		if !lb.possibleQuorum[tpar.AcceptorId] {
			notInQuorum++
		}
	}

	if notInQuorum == r.N/2 {
		//this is to prevent defer cycles
		if present, dq, _ := deferredByInstance(tpar.Replica, tpar.Instance); present {
			if lb.possibleQuorum[dq] {
				//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
				//abandon recovery, restart from phase 1
				lb.tryingToPreAccept = false
				r.makeBallot(tpar.Replica, tpar.Instance)
				r.PrintDebug("startPhase1 in handleTryPreAcceptReply")
				//r.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
				return
			}
		}
	}

	if lb.tpaReps >= r.N/2 {
		//defer recovery and update deferred information
		updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
		lb.tryingToPreAccept = false
	}
}

// helper functions and structures to prevent defer cycles while recovering

var deferMap = make(map[uint64]uint64)

func updateDeferred(dr int32, di int32, r int32, i int32) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	deferMap[aux] = daux
}

func deferredByInstance(q int32, i int32) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}

func (r *Replica) newInstanceDefault(replica int32, instance int32) *Instance {
	return r.newInstance(replica, instance, nil, NONE, [MaxN]int32{})
}

func (r *Replica) newInstance(replica int32, instance int32, cmds []state.Command, status int8, deps [MaxN]int32) *Instance {
	for q := 0; q < r.N; q++ {
		deps[q] = int32(-1)
	}
	return &Instance{cmds, status, deps, nil, [MaxN]bool{}, false, 0, time.Now()}
}

func (r *Replica) newLeaderBookkeepingDefault() *LeaderBookkeeping {
	return r.newLeaderBookkeeping(nil, r.newNilDeps(), r.newNilDeps(), r.newNilDeps(), 0, nil, NONE, -1)
}

func (r *Replica) newLeaderBookkeeping(p []*defs.GPropose, originalDeps [MaxN]int32, committedDeps [MaxN]int32, deps [MaxN]int32, lastTriedBallot int32, cmds []state.Command, status int8, seq int32) *LeaderBookkeeping {
	return &LeaderBookkeeping{p, -1, true, 0, 0, 0, originalDeps, committedDeps, nil, true, false, make([]bool, r.N), 0, false, lastTriedBallot, cmds, status, seq, deps, false}
}

func (r *Replica) newNilDeps() [MaxN]int32 {
	nildeps := [MaxN]int32{}
	for i := 0; i < r.N; i++ {
		nildeps[i] = -1
	}
	return nildeps
}

// deliverInstance tries to execute the instance if it is COMMITTED and all dependencies are executed.
func (r *Replica) deliverInstance(d *instDesc) {
	//start := time.Now()
	//r.PrintDebug("deliverInstance.time", time.Now())
	if d.inst == nil {
		// lazy fetch
		d.inst = r.InstanceSpace[d.id.replica][d.id.instance]
		if d.inst == nil {
			return
		}
	}
	r.PrintDebug("deliverInstance", "pareply.Replica", d.id.replica, "pareply.Instance", d.id.instance, "status", d.inst.Status)
	r.PrintDebug("inst.Deps", d.inst.Deps)
	r.PrintDebug("ExecedUpTo", r.ExecedUpTo)

	key := instKey(d.id.replica, d.id.instance)
	if r.delivered.Has(key) {
		r.PrintDebug("deliverInstance already delivered", "d.id.replica", d.id.replica, "d.id.instance", d.id.instance)
		return
	}
	// check dependencies delivered
	/*for q := 0; q < r.N; q++ {
		dep := d.inst.Deps[q]
		if dep >= 0 {
			if !r.delivered.Has(instKey(int32(q), dep)) {
				r.PrintDebug("deliverInstance dependency not delivered", "d.id.replica", d.id.replica, "d.id.instance", d.id.instance, "q", q, "dep", dep)
				return
			}
		}
	}


						r.ReplyProposeTS(
						&defs.ProposeReplyTS{
							OK:        TRUE,
							CommandId: inst.lb.clientProposals[i].CommandId,
							Value:     state.NIL(),
							Timestamp: inst.lb.clientProposals[i].Timestamp},
						inst.lb.clientProposals[i].Reply,
						inst.lb.clientProposals[i].Mutex)

	*/

	//if ok := r.exec.executeCommand(d.id.replica, d.id.instance); ok {
	r.delivered.Set(key, struct{}{})
	for idx := 0; idx < len(d.inst.Cmds); idx++ {
		val := d.inst.Cmds[idx].Execute(r.State)
		if d.inst.Cmds[idx].Op != state.PUT {
			r.ReplyProposeTS(
				&defs.ProposeReplyTS{
					OK:        TRUE,
					CommandId: d.inst.lb.clientProposals[idx].CommandId,
					Value:     val,
					Timestamp: d.inst.lb.clientProposals[idx].Timestamp},
				d.inst.lb.clientProposals[idx].Reply,
				d.inst.lb.clientProposals[idx].Mutex)
		}
	}

	d.inst.Status = EXECUTED
	if d.id.instance == r.ExecedUpTo[d.id.replica]+1 {
		r.ExecedUpTo[d.id.replica] = d.id.instance
		// ensure CommittedUpTo also progresses so successors see updated state
	}

	// ② 遍历 StateSpace，找到 "deps[replica] == instance" 的待执行实例
	for p := int32(0); p < int32(r.N); p++ {
		r.PrintDebug("d.id.replica", d.id.replica, "d.id.instance", d.id.instance, "p", p, "r.ExecedUpTo[p]", r.ExecedUpTo[p], "inst.Deps[p]", d.inst.Deps[p], "r.crtInstance[p]", r.crtInstance[p])
		for j := d.inst.Deps[p] + 1; j <= r.crtInstance[p]; j++ {
			//for j := r.ExecedUpTo[p] + 1; j <= r.crtInstance[p]; j++ {
			inst := r.InstanceSpace[p][j]
			//if inst == nil || inst.Deps == nil {
			//	break
			//}

			// 只要已提交，且依赖等于当前实例，就尝试推进执行
			if inst.Status == COMMITTED {
				r.PrintDebug("COMMITTED deliverInstance", "p", p, "j", j)
				select {
				case r.deliverChan <- &instanceId{p, j}:
				default:
				}
				break
			}
		}
	}
	//}
	//r.PrintDebug("deliverInstance cost", time.Since(start))
}

func (r *Replica) sendPreAcceptReply(peer int32, msg *PreAcceptReply) {
	if r.batcher != nil {
		r.batcher.Send(peer, r.preAcceptReplyRPC, msg)
	} else {
		r.SendMsg(peer, r.preAcceptReplyRPC, msg)
	}
}
