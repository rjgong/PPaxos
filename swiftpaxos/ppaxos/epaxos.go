package ppaxos

import (
	"encoding/binary"
	"io"
	"sync"
	"time"

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

//const (
//	REPLICA_ID_BITS  = 4
//	INSTANCE_ID_BITS = 28
//	INSTANCE_ID_MASK = (1 << INSTANCE_ID_BITS) - 1
//)

const BF_K = 4
const BF_M_N = 32.0

const HT_INIT_SIZE = 200000

// main differences with the original code base
// - fix N=3 case
// - add vbal variable (TLA spec. is wrong)
// - remove checkpoints (need to fix them first)
// - remove short commits (with N>7 propagating reach dependencies is necessary)
// - must run with thriftiness on (recovery is incorrect otherwise)
// - when conflicts are transitive skip waiting prior commuting commands

var cpMarker []state.Command
var cpcounter = 0

type Replica struct {
	*replica.Replica
	Q2NChan         chan fastrpc.Serializable
	N2QChan         chan fastrpc.Serializable
	SyncChan        chan fastrpc.Serializable
	commitChan      chan fastrpc.Serializable
	Q2NReplyChan    chan fastrpc.Serializable
	N2QReplyChan    chan fastrpc.Serializable
	preAcceptOKChan chan fastrpc.Serializable
	NoticeChan      chan fastrpc.Serializable
	Q2QChan         chan fastrpc.Serializable
	Q2QReplyChan    chan fastrpc.Serializable
	Q2NRPC          uint8
	Q2NReplyRPC     uint8
	N2QRPC          uint8
	N2QReplyRPC     uint8
	SyncRPC         uint8
	NoticeRPC       uint8
	commitRPC       uint8
	Q2QRPC          uint8
	Q2QReplyRPC     uint8
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
	ReachUpTo     []int32
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
	Cmds           []state.Command
	bal, vbal      int32
	Status         int8
	Seq            int32
	Deps           []int32
	lb             *LeaderBookkeeping
	Index, Lowlink int
	bfilter        any
	proposeTime    int64
	id             *instanceId
	reach          []bool
	committed      []bool
	allDeps        [][]int32
	QMajority      bool
}

type instanceId struct {
	replica  int32
	instance int32
}

type LeaderBookkeeping struct {
	clientProposals   []*defs.GPropose
	ballot            int32
	allEqual          bool
	preAcceptOKs      int
	acceptOKs         int
	nacks             int
	originalDeps      []int32
	committedDeps     []int32
	prepareReplies    []*Q2NReply
	preparing         bool
	tryingToPreAccept bool
	possibleQuorum    []bool
	tpaReps           int
	tpaAccepted       bool
	lastTriedBallot   int32
	cmds              []state.Command
	status            int8
	seq               int32
	deps              []int32
	leaderResponded   bool
}

func New(alias string, id int, peerAddrList []string, exec, beacon, durable bool, batchWait int, transconf bool, failures int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{
		replica.New(alias, id, failures, peerAddrList, true, exec, false, conf, logger),
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
		make([]int32, len(peerAddrList)),
	}

	r.Beacon = beacon
	r.Durable = durable

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, MAX_INSTANCE) // FIXME
		r.crtInstance[i] = -1
		r.ExecedUpTo[i] = -1
		r.CommittedUpTo[i] = -1
		r.conflicts[i] = make(map[state.Key]*InstPair, HT_INIT_SIZE)
		r.ReachUpTo[i] = -1
	}

	r.exec = &Exec{r}

	cpMarker = make([]state.Command, 0)

	//register RPCs
	r.Q2NRPC = r.RPC.Register(new(Q2N), r.Q2NChan)
	r.Q2NReplyRPC = r.RPC.Register(new(Q2NReply), r.Q2NReplyChan)
	r.N2QRPC = r.RPC.Register(new(N2Q), r.N2QChan)
	r.N2QReplyRPC = r.RPC.Register(new(N2QReply), r.N2QReplyChan)
	r.SyncRPC = r.RPC.Register(new(Sync), r.SyncChan)
	r.NoticeRPC = r.RPC.Register(new(Notice), r.NoticeChan)
	r.commitRPC = r.RPC.Register(new(Commit), r.commitChan)
	r.Q2QRPC = r.RPC.Register(new(Q2Q), r.Q2QChan)
	r.Q2QReplyRPC = r.RPC.Register(new(Q2QReply), r.Q2QReplyChan)

	r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"], r.Stats.M["totalCommitTime"], r.Stats.M["totalBatching"], r.Stats.M["totalBatchingSize"] = 0, 0, 0, 0, 0, 0, 0

	go r.run()

	return r
}

func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	b := make([]byte, 9+r.N*4)
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.bal))
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.vbal))
	b[4] = byte(inst.Status)
	binary.LittleEndian.PutUint32(b[5:9], uint32(inst.Seq))
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

var fastClockChan chan bool
var slowClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond)
		fastClockChan <- true
	}
}
func (r *Replica) slowClock() {
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
}

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

func (r *Replica) run() {
	r.ConnectToPeers()

	r.ComputeClosestPeers()

	if r.Exec {
		go r.executeCommands()
	}

	slowClockChan = make(chan bool, 1)
	fastClockChan = make(chan bool, 1)
	go r.slowClock()

	if r.BatchingEnabled() {
		go r.fastClock()
	}

	if r.Beacon {
		go r.stopAdapting()
	}

	onOffProposeChan := r.ProposeChan

	go r.WaitForClientConnections()

	for !r.Shutdown {

		select {

		case propose := <-onOffProposeChan:
			r.handlePropose(propose)
			if r.BatchingEnabled() {
				onOffProposeChan = nil
			}

		case <-fastClockChan:
			onOffProposeChan = r.ProposeChan

		case Q2NS := <-r.Q2NChan:
			Q2N := Q2NS.(*Q2N)
			r.handleQ2N(Q2N)

		case N2QS := <-r.N2QChan:
			N2Q := N2QS.(*N2Q)
			r.handleN2Q(N2Q)

		case SyncS := <-r.SyncChan:
			Sync := SyncS.(*Sync)
			r.handleSync(Sync)

		case commitS := <-r.commitChan:
			commit := commitS.(*Commit)
			r.handleCommit(commit)

		case Q2NReplyS := <-r.Q2NReplyChan:
			Q2NReply := Q2NReplyS.(*Q2NReply)
			r.handleQ2NReply(Q2NReply)

		case N2QReplyS := <-r.N2QReplyChan:
			//r.PrintDebug("received time", time.Now())
			N2QReply := N2QReplyS.(*N2QReply)
			r.handleN2QReply(N2QReply)

		case NoticeS := <-r.NoticeChan:
			Notice := NoticeS.(*Notice)
			r.handleNotice(Notice)

		case Q2QS := <-r.Q2QChan:
			Q2Q := Q2QS.(*Q2Q)
			r.handleQ2Q(Q2Q)

		case Q2QReplyS := <-r.Q2QReplyChan:
			Q2QReply := Q2QReplyS.(*Q2QReply)
			r.handleQ2QReply(Q2QReply)

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
		}
	}
}

func (r *Replica) executeCommands() {
	const SLEEP_TIME_NS = 1e6
	problemInstance := make([]int32, r.N)
	timeout := make([]uint64, r.N)
	for q := 0; q < r.N; q++ {
		problemInstance[q] = -1
		timeout[q] = 0
	}

	for !r.Shutdown {
		executed := false
		for q := int32(0); q < int32(r.N); q++ {
			for inst := r.ExecedUpTo[q] + 1; inst <= r.crtInstance[q]; inst++ {

				if r.InstanceSpace[q][inst] != nil && r.InstanceSpace[q][inst].Status == EXECUTED {
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
					deps := r.InstanceSpace[q][inst].Deps
					for p := 0; p < r.N; p++ {
						for j := deps[p] + 1; j <= r.crtInstance[p]; j++ {
							if r.InstanceSpace[p][j] == nil || r.InstanceSpace[p][j].Deps == nil || r.InstanceSpace[p][j].allDeps == nil || r.InstanceSpace[p][j].Status >= COMMITTED {
								continue
							}
							allDeps := false
							for i := 0; i < r.N; i++ {
								if r.InstanceSpace[p][j].allDeps[i] != nil {
									allDeps = true
									break
								}
							}
							if !allDeps {
								continue
							}
							if r.InstanceSpace[p][j].Deps[q] == inst {
								r.PrintDebug("executeCommands", "p", p, "j", j)
								priorityOKs := true
								for i := 0; i < r.N/2; i++ {
									if !r.InstanceSpace[p][j].reach[i] {
										priorityOKs = false
									}
								}

								quorumCommitted := true
								if priorityOKs {
									quorumCommitted, _ = r.updatePriority(r.InstanceSpace[p][j].allDeps, int32(p))
								}

								allCommitted := true
								r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
								r.PrintDebug("r.InstanceSpace[p][j].Deps", r.InstanceSpace[p][j].Deps)
								for q := 0; q < r.N; q++ {
									if r.CommittedUpTo[q] < r.InstanceSpace[p][j].Deps[q] {
										allCommitted = false
									}
								}

								precondition := priorityOKs && quorumCommitted && allCommitted && ((r.InstanceSpace[p][j].QMajority && p < r.N/2) || p >= r.N/2)
								r.PrintDebug("r.InstanceSpace[p][j].id.replica", p, "r.InstanceSpace[p][j].id.instance", j, "priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted, "inst.QMajority", r.InstanceSpace[p][j].QMajority, "precondition", precondition)
								if precondition {
									r.InstanceSpace[p][j].Status = COMMITTED
									if int32(p) == r.Id {
										r.PrintDebug("r.InstanceSpace[p][j].Replica == r.Id")
										if r.InstanceSpace[p][j].lb.clientProposals != nil && !r.Dreply {
											for i := 0; i < len(r.InstanceSpace[p][j].lb.clientProposals); i++ {
												r.ReplyProposeTS(
													&defs.ProposeReplyTS{
														OK:        TRUE,
														CommandId: r.InstanceSpace[p][j].lb.clientProposals[i].CommandId,
														Value:     state.NIL(),
														Timestamp: r.InstanceSpace[p][j].lb.clientProposals[i].Timestamp},
													r.InstanceSpace[p][j].lb.clientProposals[i].Reply,
													r.InstanceSpace[p][j].lb.clientProposals[i].Mutex)
											}
										}
									}
									r.recordInstanceMetadata(r.InstanceSpace[p][j])
									r.sync()

									r.updateCommitted(int32(p))

									r.M.Lock()
									r.Stats.M["fast"]++
									if r.InstanceSpace[p][j].proposeTime != 0 {
										r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - r.InstanceSpace[p][j].proposeTime)
									}
									r.M.Unlock()
								}

							}
						}
					}
					continue
				}
				if r.InstanceSpace[q][inst] == nil || r.InstanceSpace[q][inst].Status < COMMITTED || r.InstanceSpace[q][inst].Cmds == nil {
					if inst == problemInstance[q] {
						timeout[q] += SLEEP_TIME_NS
						if timeout[q] >= COMMIT_GRACE_PERIOD {
							//for k := problemInstance[q]; k <= r.crtInstance[q]; k++ {
							//r.instancesToRecover <- &instanceId{q, k}
							//}
							timeout[q] = 0
						}
					} else {
						problemInstance[q] = inst
						timeout[q] = 0
					}
					break
				}
				if ok := r.exec.executeCommand(int32(q), inst); ok {
					r.PrintDebug("successfully executed instance", inst, "replica", q)
					executed = true
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
					deps := r.InstanceSpace[q][inst].Deps
					for p := 0; p < r.N; p++ {
						for j := deps[p] + 1; j <= r.crtInstance[p]; j++ {
							if r.InstanceSpace[p][j] == nil || r.InstanceSpace[p][j].Deps == nil || r.InstanceSpace[p][j].allDeps == nil || r.InstanceSpace[p][j].reach == nil || r.InstanceSpace[p][j].Status >= COMMITTED {
								continue
							}
							allDeps := false
							for i := 0; i < r.N; i++ {
								if r.InstanceSpace[p][j].allDeps[i] != nil {
									allDeps = true
									break
								}
							}
							if !allDeps {
								continue
							}
							if r.InstanceSpace[p][j].Deps[q] == inst {
								r.PrintDebug("executeCommands2", "p", p, "j", j)
								priorityOKs := true
								for i := 0; i < r.N/2; i++ {
									if !r.InstanceSpace[p][j].reach[i] {
										priorityOKs = false
									}
								}

								quorumCommitted := true
								if priorityOKs {
									quorumCommitted, _ = r.updatePriority(r.InstanceSpace[p][j].allDeps, int32(p))
								}

								allCommitted := true
								r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
								r.PrintDebug("r.InstanceSpace[p][j].Deps", r.InstanceSpace[p][j].Deps, p, j)
								for q := 0; q < r.N; q++ {
									if r.CommittedUpTo[q] < r.InstanceSpace[p][j].Deps[q] {
										allCommitted = false
									}
								}

								precondition := priorityOKs && quorumCommitted && allCommitted && ((r.InstanceSpace[p][j].QMajority && p < r.N/2) || p >= r.N/2)
								r.PrintDebug("r.InstanceSpace[p][j].id.replica", p, "r.InstanceSpace[p][j].id.instance", j, "priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted, "inst.QMajority", r.InstanceSpace[p][j].QMajority, "precondition", precondition)
								if precondition {
									r.InstanceSpace[p][j].Status = COMMITTED
									if int32(p) == r.Id {
										r.PrintDebug("r.InstanceSpace[p][j].Replica == r.Id")
										if r.InstanceSpace[p][j].lb.clientProposals != nil && !r.Dreply {
											for i := 0; i < len(r.InstanceSpace[p][j].lb.clientProposals); i++ {
												r.ReplyProposeTS(
													&defs.ProposeReplyTS{
														OK:        TRUE,
														CommandId: r.InstanceSpace[p][j].lb.clientProposals[i].CommandId,
														Value:     state.NIL(),
														Timestamp: r.InstanceSpace[p][j].lb.clientProposals[i].Timestamp},
													r.InstanceSpace[p][j].lb.clientProposals[i].Reply,
													r.InstanceSpace[p][j].lb.clientProposals[i].Mutex)
											}
										}
									}
									r.recordInstanceMetadata(r.InstanceSpace[p][j])
									r.sync()

									r.updateCommitted(int32(p))

									r.M.Lock()
									r.Stats.M["fast"]++
									if r.InstanceSpace[p][j].proposeTime != 0 {
										r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - r.InstanceSpace[p][j].proposeTime)
									}
									r.M.Unlock()
								}

							}
						}
					}
					//r.PrintDebug("end of executeCommands")
				}
			}
		}
		if !executed {
			r.M.Lock()
			r.M.Unlock() // FIXME for cache coherence
			time.Sleep(SLEEP_TIME_NS)
		}
	}
}

func isInitialBallot(ballot int32, replica int32) bool {
	return ballot == replica
}

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

func (r *Replica) replyPrepare(replicaId int32, reply *Q2NReply) {
	r.SendMsg(replicaId, r.Q2NReplyRPC, reply)
}

func (r *Replica) replyPreAccept(replicaId int32, reply *N2QReply) {
	r.SendMsg(replicaId, r.N2QReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *Notice) {
	r.SendMsg(replicaId, r.NoticeRPC, reply)
}

func (r *Replica) replyTryPreAccept(replicaId int32, reply *Q2QReply) {
	r.SendMsg(replicaId, r.Q2QReplyRPC, reply)
}

func (r *Replica) bcastPrepare(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Q2N bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	args := &Q2N{replica, instance, lb.cmds, lb.deps}

	n := r.N - 1
	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.Q2NRPC, args)
		sent++
	}
}

func (r *Replica) bcastPreAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("N2Q bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	pa := new(N2Q)
	pa.Replica = replica
	pa.Instance = instance
	pa.Command = lb.cmds

	for q := 0; q < r.N; q++ {
		if int32(q) == r.Id {
			continue
		}

		r.SendMsg(int32(q), r.N2QRPC, pa)

	}
}

func (r *Replica) bcastTryPreAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("N2Q bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	tpa := new(Q2Q)
	tpa.Replica = replica
	tpa.Instance = instance
	tpa.Command = lb.cmds
	tpa.Deps = lb.deps

	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.Q2QRPC, tpa)
	}
}

func (r *Replica) bcastAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Sync bcast failed:", err)
		}
	}()

	lb := r.InstanceSpace[replica][instance].lb
	ea := new(Sync)
	ea.AcceptorId = r.Id
	ea.Replica = replica
	ea.Instance = instance
	ea.Deps = lb.deps

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.SyncRPC, ea)
		sent++
		if sent >= n {
			break
		}
	}
}

func (r *Replica) bcastCommit(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Commit bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	ec := new(Commit)
	ec.LeaderId = r.Id
	ec.Replica = replica
	ec.Instance = instance
	ec.Command = lb.cmds
	ec.Seq = lb.seq
	ec.Deps = lb.deps
	ec.Ballot = lb.ballot

	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, ec)
	}
}

func (r *Replica) updateCommitted(replica int32) {
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == EXECUTED) {
		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
		r.PrintDebug("r.CommittedUpTo[replica]", r.CommittedUpTo[replica])
	}
}

func (r *Replica) updateConflicts(cmds []state.Command, replica int32, instance int32) {
	//r.PrintDebug("updateConflicts", "replica", replica, "instance", instance, "seq", seq)
	for i := 0; i < len(cmds); i++ {
		if dpair, present := r.conflicts[replica][cmds[i].K]; present {
			r.PrintDebug("replica", replica, "instance", instance, "cmds", i, "key", cmds[i].K, "dpair.last", dpair.last, "dpair.lastWrite", dpair.lastWrite)
			if dpair.last < instance {
				r.conflicts[replica][cmds[i].K].last = instance
			}
			if dpair.lastWrite < instance && cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		} else {
			r.PrintDebug("replica", replica, "instance", instance, "cmds", i, "key", cmds[i].K)
			r.conflicts[replica][cmds[i].K] = &InstPair{
				last:      instance,
				lastWrite: -1,
			}
			if cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		}
	}
}

func (r *Replica) updateAttributes(cmds []state.Command, deps []int32, replica int32) ([]int32, bool) {
	changed := false
	for q := 0; q < r.N; q++ {
		/*if r.Id != replica && int32(q) == replica {
			continue
		}*/
		for i := 0; i < len(cmds); i++ {
			if dpair, present := (r.conflicts[q])[cmds[i].K]; present {
				r.PrintDebug("updateAttributes q", q, "replica", replica, "cmds", i, "key", cmds[i].K, "dpair.last", dpair.last, "dpair.lastWrite", dpair.lastWrite)
				d := dpair.lastWrite
				if cmds[i].Op != state.GET {
					d = dpair.last
				}
				if d > deps[q] {
					r.PrintDebug("updateAttributes q", q, "replica", replica, "cmds", i, "key", cmds[i].K, "d", d, "deps[q]", deps[q])
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

func (r *Replica) updatePriority(allDeps [][]int32, replica int32) (bool, []int32) {
	r.PrintDebug("updatePriority", "replica", replica, "allDeps", allDeps)
	deps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		deps[i] = -1
	}
	deps1 := make([]int32, len(allDeps[0]))
	copy(deps1, allDeps[0])
	deps2 := make([]int32, len(allDeps[0]))
	copy(deps2, allDeps[0])
	for q := 1; q < r.N/2; q++ {
		for i := 0; i < r.N; i++ {
			if allDeps[q][i] > deps1[i] {
				r.PrintDebug("allDeps[q][i]", allDeps[q][i], "deps1[i]", deps1[i])
				deps1[i] = allDeps[q][i]
			} else if allDeps[q][i] < deps2[i] {
				r.PrintDebug("allDeps[q][i]", allDeps[q][i], "deps2[i]", deps2[i])
				deps2[i] = allDeps[q][i]
			}
		}
	}
	for i := 0; i < r.N; i++ {
		if i <= int(replica) {
			deps[i] = deps1[i]
		} else {
			deps[i] = deps2[i]
		}
	}
	r.PrintDebug("deps1", deps1)
	r.PrintDebug("deps2", deps2)
	for i := 0; i < r.N/2; i++ {
		for j := 0; j < r.N; j++ {
			if j <= int(replica) {
				r.PrintDebug("i", i, "j", j, "allDeps[i][j]", allDeps[i][j], "deps1[i]", deps1[j])
				if deps1[j] == -1 {
					continue
				}
				if r.InstanceSpace[j][deps1[j]] == nil || allDeps[i][j] < deps1[j] && !r.InstanceSpace[j][deps1[j]].committed[i] {
					r.PrintDebug("i", i, "j", j, "allDeps[i][j]", allDeps[i][j], "deps1[j]", deps1[j], "r.InstanceSpace[j][deps1[j]].committed[i]", r.InstanceSpace[j][deps1[j]].committed[i])
					return false, deps
				}
			} else {
				r.PrintDebug("i", i, "j", j, "allDeps[i][j]", allDeps[i][j], "deps2[i]", deps2[j])
				if deps2[j] == -1 {
					continue
				}
				if r.InstanceSpace[j][deps2[j]] == nil || allDeps[i][j] > deps2[j] && !r.InstanceSpace[j][deps2[j]].committed[i] {
					r.PrintDebug("i", i, "j", j, "allDeps[i][j]", allDeps[i][j], "deps2[j]", deps2[j], "r.InstanceSpace[j][deps2[j]].committed[i]", r.InstanceSpace[j][deps2[j]].committed[i])
					return false, deps
				}
			}
		}
	}

	return true, deps
}

func (r *Replica) mergeAttributes(seq1 int32, deps1 []int32, seq2 int32, deps2 []int32) (int32, []int32, bool) {
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
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
	return seq1, deps1, equal
}

func equal(deps1 []int32, deps2 []int32) bool {
	for i := 0; i < len(deps1); i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

func (r *Replica) handlePropose(propose *defs.GPropose) {
	//TODO!! Handle client retries
	//r.PrintDebug("handlePropose time", time.Now())
	batchSize := len(r.ProposeChan) + 1
	r.M.Lock()
	r.Stats.M["totalBatching"]++
	r.Stats.M["totalBatchingSize"] += batchSize
	r.M.Unlock()

	r.crtInstance[r.Id]++
	r.PrintDebug("r.Id", r.Id, "instance", r.crtInstance[r.Id])
	cmds := make([]state.Command, batchSize)
	proposals := make([]*defs.GPropose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}
	r.startPhase1(cmds, r.Id, r.crtInstance[r.Id], r.Id, proposals)

	cpcounter += len(cmds)

}

func (r *Replica) startPhase1(cmds []state.Command, replica int32, instance int32, ballot int32, proposals []*defs.GPropose) {
	// init command attributes
	seq := int32(0)
	deps := make([]int32, r.N)
	for q := 0; q < r.N; q++ {
		deps[q] = -1
	}

	deps, _ = r.updateAttributes(cmds, deps, replica)

	comDeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		comDeps[i] = -1
	}

	inst := r.newInstance(replica, instance, cmds, ballot, ballot, PREACCEPTED, seq, deps)
	inst.lb = r.newLeaderBookkeeping(proposals, deps, comDeps, deps, ballot, cmds, PREACCEPTED, -1)
	inst.Deps = deps
	if r.Id < int32(r.N/2) {
		inst.allDeps[replica] = deps
	}
	r.InstanceSpace[replica][instance] = inst
	r.PrintDebug("deps", deps)
	r.updateConflicts(cmds, replica, instance)

	r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
	r.recordCommands(cmds)
	r.sync()

	N2Q := &N2Q{
		replica,
		instance,
		inst.Deps[r.Id],
		inst.Cmds,
	}

	Q2Q := &Q2Q{
		replica,
		instance,
		inst.Cmds,
		inst.Deps,
	}

	Q2N := &Q2N{
		replica,
		instance,
		inst.Cmds,
		inst.Deps,
	}

	if r.Id >= int32(r.N/2) { // 如果不在 quorum 中
		for i := 0; i < int(r.N/2); i++ {
			r.SendMsg(int32(i), r.N2QRPC, N2Q)
		}
	} else {
		for i := 0; i < int(r.N/2); i++ {
			if i != int(r.Id) {
				r.SendMsg(int32(i), r.Q2QRPC, Q2Q)
			}
		}
		for i := int(r.N / 2); i < r.N; i++ {
			r.SendMsg(int32(i), r.Q2NRPC, Q2N)
		}
	}
}

func (r *Replica) handleN2Q(N2Q *N2Q) {
	r.PrintDebug("handleN2Q", "&", N2Q.Replica, N2Q.Instance, "N2Q.Deps", N2Q.Deps)
	inst := r.InstanceSpace[N2Q.Replica][N2Q.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(N2Q.Replica, N2Q.Instance)
		if N2Q.Instance > r.crtInstance[N2Q.Replica] {
			r.crtInstance[N2Q.Replica] = N2Q.Instance
		}
		r.InstanceSpace[N2Q.Replica][N2Q.Instance] = inst
		inst.Cmds = N2Q.Command
		inst.Status = PREACCEPTED_EQ
		inst.reach[N2Q.Replica] = true
		inst.reach[r.Id] = true
		inst.committed[r.Id] = true

		if r.ReachUpTo[N2Q.Replica]+1 == N2Q.Instance {
			r.PrintDebug("r.ReachUpTo[N2Q.Replica]+1 == N2Q.Instance", r.ReachUpTo[N2Q.Replica], N2Q.Instance)
			for i := N2Q.Instance; i <= r.crtInstance[N2Q.Replica]; i++ {
				r.PrintDebug("i", i)
				if r.InstanceSpace[N2Q.Replica][i] != nil {
					inst = r.InstanceSpace[N2Q.Replica][i]
					r.ReachUpTo[N2Q.Replica] = i

					deps := make([]int32, r.N)
					for q := 0; q < r.N; q++ {
						deps[q] = -1
					}
					inst.Deps, _ = r.updateAttributes(inst.Cmds, deps, N2Q.Replica)
					r.PrintDebug("inst.Deps", inst.Deps)
					inst.allDeps[r.Id] = inst.Deps
					r.updateConflicts(inst.Cmds, N2Q.Replica, i)
					r.PrintDebug("inst.Deps2", inst.Deps)
					r.recordInstanceMetadata(r.InstanceSpace[N2Q.Replica][i])
					r.recordCommands(inst.Cmds)
					r.sync()

					r.PrintDebug("inst.reach", inst.reach)
					r.PrintDebug("inst.allDeps", inst.allDeps)
					priorityOKs := true
					for i := 0; i < r.N/2; i++ {
						if !inst.reach[i] {
							priorityOKs = false
						}
					}

					quorumCommitted := true
					if priorityOKs {
						quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, N2Q.Replica)
					}

					allCommitted := true
					r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
					r.PrintDebug("inst.Deps", inst.Deps)
					for q := 0; q < r.N; q++ {
						if r.CommittedUpTo[q] < inst.Deps[q] {
							allCommitted = false
						}
					}
					r.PrintDebug("priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted)

					if priorityOKs && quorumCommitted && allCommitted {

						inst.Status = COMMITTED

						r.recordInstanceMetadata(inst)
						r.sync()

						r.updateCommitted(N2Q.Replica)
						r.recordCommands(inst.Cmds)

						r.M.Lock()
						r.Stats.M["fast"]++
						if inst.proposeTime != 0 {
							r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
						}
						r.M.Unlock()
					}

					r.PrintDebug("inst.reach", inst.reach)
					r.PrintDebug("inst.allDeps", inst.allDeps)

					reply := &N2QReply{
						AcceptorId: r.Id,
						Replica:    N2Q.Replica,
						Instance:   i,
						Command:    inst.Cmds,
						Deps:       inst.Deps,
					}

					Sync := &Sync{
						r.Id,
						N2Q.Replica,
						i,
						inst.Cmds,
						inst.Deps,
					}

					r.PrintDebug("sendN2QReply")

					for i := 0; i < int(r.N/2); i++ {
						if i != int(r.Id) {
							r.SendMsg(int32(i), r.SyncRPC, Sync)
						}
					}

					for i := int(r.N / 2); i < r.N; i++ {
						r.SendMsg(int32(i), r.N2QReplyRPC, reply)
					}
				} else {
					break
				}
			}
		}

	} else {
		inst.committed[r.Id] = true
		r.PrintDebug("inst.reach", inst.reach)
		r.PrintDebug("inst.allDeps", inst.allDeps)
		priorityOKs := true
		for i := 0; i < r.N/2; i++ {
			if !inst.reach[i] {
				priorityOKs = false
			}
		}

		quorumCommitted := true
		if priorityOKs {
			quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, N2Q.Replica)
		}

		allCommitted := true
		r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
		r.PrintDebug("inst.Deps", inst.Deps)
		for q := 0; q < r.N; q++ {
			if r.CommittedUpTo[q] < inst.Deps[q] {
				allCommitted = false
			}
		}
		r.PrintDebug("priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted)

		if priorityOKs && quorumCommitted && allCommitted {

			inst.Status = COMMITTED

			r.recordInstanceMetadata(inst)
			r.sync()

			r.updateCommitted(N2Q.Replica)
			r.recordCommands(inst.Cmds)

			r.M.Lock()
			r.Stats.M["fast"]++
			if inst.proposeTime != 0 {
				r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
			}
			r.M.Unlock()
		}

		r.PrintDebug("inst.reach", inst.reach)
		r.PrintDebug("inst.allDeps", inst.allDeps)

		reply := &N2QReply{
			AcceptorId: r.Id,
			Replica:    N2Q.Replica,
			Instance:   N2Q.Instance,
			Command:    N2Q.Command,
			Deps:       inst.Deps,
		}

		Sync := &Sync{
			r.Id,
			N2Q.Replica,
			N2Q.Instance,
			N2Q.Command,
			inst.Deps,
		}

		r.PrintDebug("sendN2QReply")

		for i := 0; i < int(r.N/2); i++ {
			if i != int(r.Id) {
				r.SendMsg(int32(i), r.SyncRPC, Sync)
			}
		}

		for i := int(r.N / 2); i < r.N; i++ {
			r.SendMsg(int32(i), r.N2QReplyRPC, reply)
		}
	}
}

func (r *Replica) handleN2QReply(N2QReply *N2QReply) {
	r.PrintDebug("handleN2QReply", "N2QReply.AcceptorId", N2QReply.AcceptorId, "&", N2QReply.Replica, N2QReply.Instance, "N2QReply.Deps", N2QReply.Deps)
	//r.PrintDebug("pareply", pareply)
	//r.PrintDebug("time", time.Now())
	inst := r.InstanceSpace[N2QReply.Replica][N2QReply.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(N2QReply.Replica, N2QReply.Instance)

		r.InstanceSpace[N2QReply.Replica][N2QReply.Instance] = inst
		if N2QReply.Instance > r.crtInstance[N2QReply.Replica] {
			r.crtInstance[N2QReply.Replica] = N2QReply.Instance
		}

		inst.Cmds = N2QReply.Command
		inst.Status = PREACCEPTED_EQ
		inst.reach[r.Id] = true
		r.updateConflicts(N2QReply.Command, N2QReply.Replica, N2QReply.Instance)
		r.recordInstanceMetadata(r.InstanceSpace[N2QReply.Replica][N2QReply.Instance])
		r.recordCommands(N2QReply.Command)
		r.sync()

	}

	priorityOKs := true
	inst.allDeps[N2QReply.AcceptorId] = N2QReply.Deps
	inst.reach[N2QReply.AcceptorId] = true
	r.PrintDebug("inst.reach", inst.reach)
	r.PrintDebug("inst.allDeps", inst.allDeps)
	for i := 0; i < r.N/2; i++ {
		if !inst.reach[i] {
			priorityOKs = false
		}
	}

	quorumCommitted := true
	if priorityOKs {
		quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, N2QReply.Replica)
	}

	allCommitted := true
	r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
	r.PrintDebug("inst.Deps", inst.Deps)
	for q := 0; q < r.N; q++ {
		if r.CommittedUpTo[q] < inst.Deps[q] {
			allCommitted = false
		}
	}

	precondition := priorityOKs && quorumCommitted && allCommitted
	r.PrintDebug("N2QReply.Replica", N2QReply.Replica, "N2QReply.Instance", N2QReply.Instance, "priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted, "precondition", precondition)
	if precondition {
		inst.Status = COMMITTED
		if N2QReply.Replica == r.Id {
			r.PrintDebug("N2QReply.Replica == r.Id")
			if inst.lb.clientProposals != nil && !r.Dreply {
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.ReplyProposeTS(
						&defs.ProposeReplyTS{
							OK:        TRUE,
							CommandId: inst.lb.clientProposals[i].CommandId,
							Value:     state.NIL(),
							Timestamp: inst.lb.clientProposals[i].Timestamp},
						inst.lb.clientProposals[i].Reply,
						inst.lb.clientProposals[i].Mutex)
				}
			}
		}
		r.recordInstanceMetadata(inst)
		r.sync()

		r.updateCommitted(N2QReply.Replica)
		r.recordCommands(inst.Cmds)

		r.M.Lock()
		r.Stats.M["fast"]++
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.M.Unlock()
	}

}

func (r *Replica) handleSync(Sync *Sync) {

	r.PrintDebug("handleSync", "Sync.AcceptorId", Sync.AcceptorId, "&", Sync.Replica, Sync.Instance, "Sync.Deps", Sync.Deps)
	inst := r.InstanceSpace[Sync.Replica][Sync.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(Sync.Replica, Sync.Instance)
		r.InstanceSpace[Sync.Replica][Sync.Instance] = inst
		if Sync.Instance > r.crtInstance[Sync.Replica] {
			r.crtInstance[Sync.Replica] = Sync.Instance
		}

		inst.Cmds = Sync.Command
		inst.Status = PREACCEPTED_EQ
		inst.reach[Sync.Replica] = true
		r.updateConflicts(inst.Cmds, Sync.Replica, Sync.Instance)
		r.recordInstanceMetadata(inst)
		r.recordCommands(inst.Cmds)
		r.sync()
	}
	r.PrintDebug("inst != nil")
	inst.allDeps[Sync.AcceptorId] = Sync.Deps
	inst.reach[Sync.AcceptorId] = true
	inst.committed[Sync.AcceptorId] = true

	r.PrintDebug("inst.reach", inst.reach)
	r.PrintDebug("inst.allDeps", inst.allDeps)
	priorityOKs := true
	for i := 0; i < r.N/2; i++ {
		if !inst.reach[i] {
			priorityOKs = false
		}
	}

	quorumCommitted := true
	if priorityOKs {
		quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, Sync.Replica)
	}

	allCommitted := true
	r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
	r.PrintDebug("inst.Deps", inst.Deps)
	for q := 0; q < r.N; q++ {
		if r.CommittedUpTo[q] < inst.Deps[q] {
			allCommitted = false
		}
	}

	r.PrintDebug("priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted)
	if priorityOKs && quorumCommitted && allCommitted {

		inst.Status = COMMITTED

		r.recordInstanceMetadata(inst)
		r.sync()

		r.updateCommitted(Sync.Replica)
		r.recordCommands(inst.Cmds)

		r.M.Lock()
		r.Stats.M["fast"]++
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.M.Unlock()
	}

	reply := &Notice{
		r.Id,
		Sync.Replica,
		Sync.Instance,
		Sync.Command,
	}

	// r.replyAccept(accept.LeaderId, reply)
	for i := int(r.N / 2); i < r.N; i++ {
		r.SendMsg(int32(i), r.NoticeRPC, reply)
	}
}

func (r *Replica) handleNotice(Notice *Notice) {
	r.PrintDebug("handleNotice", "Notice.AcceptorId", Notice.AcceptorId, "&", Notice.Replica, Notice.Instance)

	inst := r.InstanceSpace[Notice.Replica][Notice.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(Notice.Replica, Notice.Instance)

		r.InstanceSpace[Notice.Replica][Notice.Instance] = inst
		if Notice.Instance > r.crtInstance[Notice.Replica] {
			r.crtInstance[Notice.Replica] = Notice.Instance
		}
		inst.Cmds = Notice.Command
		inst.Status = PREACCEPTED_EQ
		inst.reach[r.Id] = true
		r.updateConflicts(Notice.Command, Notice.Replica, Notice.Instance)
		r.recordInstanceMetadata(r.InstanceSpace[Notice.Replica][Notice.Instance])
		r.recordCommands(Notice.Command)
		r.sync()

	}

	inst.committed[Notice.AcceptorId] = true
	r.PrintDebug("inst.committed", inst.committed)

	quorumCommitted := true

	for i := 0; i < r.N/2; i++ {
		if !inst.committed[i] {
			quorumCommitted = false
		}
	}

	if quorumCommitted {
		for p := 0; p < r.N; p++ {
			for j := inst.Deps[p] + 1; j <= r.crtInstance[p]; j++ {
				if r.InstanceSpace[p][j] == nil || r.InstanceSpace[p][j].Deps == nil || r.InstanceSpace[p][j].Status >= COMMITTED {
					continue
				}
				if r.InstanceSpace[p][j].Deps[Notice.Replica] == Notice.Instance {
					r.PrintDebug("handleNotice", "replica", p, "instance", j, "deps", inst.Deps[p], "crtInstance", r.crtInstance[p])
					r.handleN2QReply(&N2QReply{
						AcceptorId: int32(p),
						Replica:    int32(p),
						Instance:   j,
						Command:    r.InstanceSpace[p][j].Cmds,
						Deps:       r.InstanceSpace[p][j].Deps,
					})
				}
			}
		}
	}
}

func (r *Replica) handleQ2N(Q2N *Q2N) {
	r.PrintDebug("handleQ2N", "&", Q2N.Replica, Q2N.Instance, "Q2N.Deps", Q2N.Deps)
	inst := r.InstanceSpace[Q2N.Replica][Q2N.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(Q2N.Replica, Q2N.Instance)
		r.InstanceSpace[Q2N.Replica][Q2N.Instance] = inst
		if Q2N.Instance > r.crtInstance[Q2N.Replica] {
			r.crtInstance[Q2N.Replica] = Q2N.Instance
		}
		r.PrintDebug("inst.Deps1", inst.Deps)
		inst.QMajority = true
		inst.Cmds = Q2N.Command
		inst.Status = PREACCEPTED_EQ
		inst.reach[Q2N.Replica] = true
		inst.reach[r.Id] = true
		r.updateConflicts(inst.Cmds, Q2N.Replica, Q2N.Instance)
		r.recordInstanceMetadata(inst)
		r.recordCommands(inst.Cmds)
		r.sync()
	}
	inst.allDeps[Q2N.Replica] = Q2N.Deps
	r.PrintDebug("inst.Deps2", inst.Deps)
	r.PrintDebug("inst.reach", inst.reach)
	r.PrintDebug("inst.allDeps", inst.allDeps)
	//inst.Deps = r.updatePriority(inst.Deps, N2QReply.Deps, N2QReply.Replica)

	priorityOKs := true
	for i := 0; i < r.N/2; i++ {
		if !inst.reach[i] {
			priorityOKs = false
		}
	}
	r.PrintDebug("inst.Deps3", inst.Deps)
	quorumCommitted := true
	if priorityOKs {
		quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, Q2N.Replica)
	}

	allCommitted := true
	r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
	r.PrintDebug("inst.Deps", inst.Deps)
	for q := 0; q < r.N; q++ {
		if r.CommittedUpTo[q] < inst.Deps[q] {
			allCommitted = false
		}
	}

	precondition := priorityOKs && quorumCommitted && allCommitted
	r.PrintDebug("Q2N.Replica", Q2N.Replica, "Q2N.Instance", Q2N.Instance, "priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted, "precondition", precondition)
	if precondition {
		inst.Status = COMMITTED
		if Q2N.Replica == r.Id {
			if inst.lb.clientProposals != nil && !r.Dreply {
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.ReplyProposeTS(
						&defs.ProposeReplyTS{
							OK:        TRUE,
							CommandId: inst.lb.clientProposals[i].CommandId,
							Value:     state.NIL(),
							Timestamp: inst.lb.clientProposals[i].Timestamp},
						inst.lb.clientProposals[i].Reply,
						inst.lb.clientProposals[i].Mutex)
				}
			}
		}
		r.recordInstanceMetadata(inst)
		r.sync()

		r.updateCommitted(Q2N.Replica)
		r.recordCommands(inst.Cmds)

		r.M.Lock()
		r.Stats.M["fast"]++
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.M.Unlock()

	}

	reply := &Q2NReply{
		r.Id,
		Q2N.Replica,
		Q2N.Instance,
		Q2N.Command,
		Q2N.Deps,
	}

	r.PrintDebug("sendQ2NReply")

	for i := 0; i < r.N/2; i++ {
		r.SendMsg(int32(i), r.Q2NReplyRPC, reply)
	}
}

func (r *Replica) handleQ2NReply(Q2NReply *Q2NReply) {
	r.PrintDebug("handleQ2NReply", "Q2NReply.AcceptorId", Q2NReply.AcceptorId, "&", Q2NReply.Replica, Q2NReply.Instance, "Q2NReply.Deps", Q2NReply.Deps)

	inst := r.InstanceSpace[Q2NReply.Replica][Q2NReply.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(Q2NReply.Replica, Q2NReply.Instance)

		r.InstanceSpace[Q2NReply.Replica][Q2NReply.Instance] = inst
		if Q2NReply.Instance > r.crtInstance[Q2NReply.Replica] {
			r.crtInstance[Q2NReply.Replica] = Q2NReply.Instance
		}
		inst.allDeps[Q2NReply.Replica] = Q2NReply.Deps
		inst.Cmds = Q2NReply.Command
		inst.QMajority = true
		inst.Status = PREACCEPTED_EQ
		inst.reach[Q2NReply.Replica] = true
		inst.reach[r.Id] = true
		inst.committed[Q2NReply.Replica] = true
		inst.committed[r.Id] = true
		r.updateConflicts(inst.Cmds, Q2NReply.Replica, Q2NReply.Instance)
		r.recordInstanceMetadata(inst)
		r.recordCommands(inst.Cmds)
		r.sync()
	} else {
		r.PrintDebug("inst != nil")
		inst.allDeps[Q2NReply.Replica] = Q2NReply.Deps
		inst.reach[Q2NReply.AcceptorId] = true
		inst.QMajority = true
		r.PrintDebug("inst.reach", inst.reach)
		r.PrintDebug("inst.allDeps", inst.allDeps)

		priorityOKs := true
		for i := 0; i < r.N/2; i++ {
			if !inst.reach[i] {
				priorityOKs = false
			}
		}

		quorumCommitted := true
		if priorityOKs {
			quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, Q2NReply.Replica)
		}

		allCommitted := true
		r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
		r.PrintDebug("inst.Deps", inst.Deps)
		for q := 0; q < r.N; q++ {
			if r.CommittedUpTo[q] < inst.Deps[q] {
				allCommitted = false
			}
		}

		precondition := priorityOKs && quorumCommitted && allCommitted
		r.PrintDebug("Q2NReply.Replica", Q2NReply.Replica, "Q2NReply.Instance", Q2NReply.Instance, "priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted, "precondition", precondition)
		if precondition {
			inst.Status = COMMITTED
			if Q2NReply.Replica == r.Id {
				if inst.lb.clientProposals != nil && !r.Dreply {
					for i := 0; i < len(inst.lb.clientProposals); i++ {
						r.ReplyProposeTS(
							&defs.ProposeReplyTS{
								OK:        TRUE,
								CommandId: inst.lb.clientProposals[i].CommandId,
								Value:     state.NIL(),
								Timestamp: inst.lb.clientProposals[i].Timestamp},
							inst.lb.clientProposals[i].Reply,
							inst.lb.clientProposals[i].Mutex)
					}
				}
			}
			r.recordInstanceMetadata(inst)
			r.sync()

			r.updateCommitted(Q2NReply.Replica)
			r.recordCommands(inst.Cmds)

			r.M.Lock()
			r.Stats.M["fast"]++
			if inst.proposeTime != 0 {
				r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
			}
			r.M.Unlock()
		}
	}
}

func (r *Replica) handleQ2Q(Q2Q *Q2Q) {
	r.PrintDebug("handleQ2Q", "&", Q2Q.Replica, Q2Q.Instance, "Q2Q.Deps", Q2Q.Deps)
	inst := r.InstanceSpace[Q2Q.Replica][Q2Q.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(Q2Q.Replica, Q2Q.Instance)
		r.InstanceSpace[Q2Q.Replica][Q2Q.Instance] = inst
		if Q2Q.Instance > r.crtInstance[Q2Q.Replica] {
			r.crtInstance[Q2Q.Replica] = Q2Q.Instance
		}
		inst.allDeps[Q2Q.Replica] = Q2Q.Deps
		inst.Cmds = Q2Q.Command
		inst.Status = PREACCEPTED_EQ
		inst.reach[Q2Q.Replica] = true
		inst.reach[r.Id] = true
		inst.committed[Q2Q.Replica] = true
		inst.committed[r.Id] = true

		if r.ReachUpTo[Q2Q.Replica]+1 == Q2Q.Instance {
			r.PrintDebug("r.ReachUpTo[Q2Q.Replica]+1 == Q2Q.Instance", r.ReachUpTo[Q2Q.Replica], Q2Q.Instance)
			for i := Q2Q.Instance; i <= r.crtInstance[Q2Q.Replica]; i++ {
				r.PrintDebug("i", i)
				if r.InstanceSpace[Q2Q.Replica][i] != nil {
					inst = r.InstanceSpace[Q2Q.Replica][i]
					r.ReachUpTo[Q2Q.Replica] = i

					deps := make([]int32, r.N)
					for q := 0; q < r.N; q++ {
						deps[q] = -1
					}
					inst.Deps, _ = r.updateAttributes(inst.Cmds, deps, Q2Q.Replica)
					r.PrintDebug("inst.Deps", inst.Deps)
					inst.allDeps[r.Id] = inst.Deps
					r.updateConflicts(inst.Cmds, Q2Q.Replica, i)
					r.PrintDebug("inst.Deps2", inst.Deps)
					r.recordInstanceMetadata(r.InstanceSpace[Q2Q.Replica][i])
					r.recordCommands(inst.Cmds)
					r.sync()

					r.PrintDebug("inst.reach", inst.reach)
					r.PrintDebug("inst.allDeps", inst.allDeps)
					priorityOKs := true
					for i := 0; i < r.N/2; i++ {
						if !inst.reach[i] {
							priorityOKs = false
						}
					}

					quorumCommitted := true
					if priorityOKs {
						quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, Q2Q.Replica)
					}

					allCommitted := true
					r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
					r.PrintDebug("inst.Deps", inst.Deps)
					for q := 0; q < r.N; q++ {
						if r.CommittedUpTo[q] < inst.Deps[q] {
							allCommitted = false
						}
					}
					r.PrintDebug("priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted)

					if priorityOKs && quorumCommitted && allCommitted {

						inst.Status = COMMITTED

						r.recordInstanceMetadata(inst)
						r.sync()

						r.updateCommitted(Q2Q.Replica)
						r.recordCommands(inst.Cmds)

						r.M.Lock()
						r.Stats.M["fast"]++
						if inst.proposeTime != 0 {
							r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
						}
						r.M.Unlock()
					}

					r.PrintDebug("inst.reach", inst.reach)
					r.PrintDebug("inst.allDeps", inst.allDeps)

					reply := &Q2QReply{
						r.Id,
						Q2Q.Replica,
						Q2Q.Instance,
						Q2Q.Command,
						inst.Deps,
					}

					r.PrintDebug("sendQ2QReply")

					for i := 0; i < r.N; i++ {
						r.SendMsg(int32(i), r.Q2QReplyRPC, reply)
					}
				} else {
					break
				}
			}
		}
	} else {
		r.PrintDebug("inst != nil")
		inst.allDeps[Q2Q.Replica] = Q2Q.Deps
		inst.reach[Q2Q.Replica] = true
		r.PrintDebug("inst.allDeps", inst.allDeps)
		r.PrintDebug("inst.reach", inst.reach)
		//inst.Deps = r.updatePriority(inst.Deps, N2QReply.Deps, N2QReply.Replica)

		priorityOKs := true
		for i := 0; i < r.N/2; i++ {
			if !inst.reach[i] {
				priorityOKs = false
			}
		}

		quorumCommitted := true
		if priorityOKs {
			quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, Q2Q.Replica)
		}

		allCommitted := true
		r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
		r.PrintDebug("inst.Deps", inst.Deps)
		for q := 0; q < r.N; q++ {
			if r.CommittedUpTo[q] < inst.Deps[q] {
				allCommitted = false
			}
		}

		precondition := priorityOKs && quorumCommitted && allCommitted
		r.PrintDebug("Q2Q.Replica", Q2Q.Replica, "Q2Q.Instance", Q2Q.Instance, "priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted, "precondition", precondition)
		if precondition {
			inst.Status = COMMITTED
			if Q2Q.Replica == r.Id {
				if inst.lb.clientProposals != nil && !r.Dreply {
					for i := 0; i < len(inst.lb.clientProposals); i++ {
						r.ReplyProposeTS(
							&defs.ProposeReplyTS{
								OK:        TRUE,
								CommandId: inst.lb.clientProposals[i].CommandId,
								Value:     state.NIL(),
								Timestamp: inst.lb.clientProposals[i].Timestamp},
							inst.lb.clientProposals[i].Reply,
							inst.lb.clientProposals[i].Mutex)
					}
				}
			}
			r.recordInstanceMetadata(inst)
			r.sync()

			r.updateCommitted(Q2Q.Replica)
			r.recordCommands(inst.Cmds)

			r.M.Lock()
			r.Stats.M["fast"]++
			if inst.proposeTime != 0 {
				r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
			}
			r.M.Unlock()
		}
		reply := &Q2QReply{
			r.Id,
			Q2Q.Replica,
			Q2Q.Instance,
			Q2Q.Command,
			inst.Deps,
		}

		r.PrintDebug("sendQ2QReply")

		for i := 0; i < r.N; i++ {
			r.SendMsg(int32(i), r.Q2QReplyRPC, reply)
		}
	}

}

func (r *Replica) handleQ2QReply(Q2QReply *Q2QReply) {
	r.PrintDebug("handleQ2QReply", "Q2QReply.AcceptorId", Q2QReply.AcceptorId, "&", Q2QReply.Replica, Q2QReply.Instance, "Q2QReply.Deps", Q2QReply.Deps)
	inst := r.InstanceSpace[Q2QReply.Replica][Q2QReply.Instance]

	if inst == nil {
		r.PrintDebug("inst == nil")
		inst = r.newInstanceDefault(Q2QReply.Replica, Q2QReply.Instance)
		r.InstanceSpace[Q2QReply.Replica][Q2QReply.Instance] = inst
		if Q2QReply.Instance > r.crtInstance[Q2QReply.Replica] {
			r.crtInstance[Q2QReply.Replica] = Q2QReply.Instance
		}

		inst.allDeps[Q2QReply.AcceptorId] = Q2QReply.Deps
		inst.Cmds = Q2QReply.Command
		inst.Status = PREACCEPTED_EQ
		inst.reach[Q2QReply.Replica] = true
		inst.reach[r.Id] = true
		inst.reach[Q2QReply.AcceptorId] = true
		inst.committed[Q2QReply.Replica] = true
		inst.committed[r.Id] = true
		inst.committed[Q2QReply.AcceptorId] = true
		r.updateConflicts(inst.Cmds, Q2QReply.Replica, Q2QReply.Instance)
		r.recordInstanceMetadata(inst)
		r.recordCommands(inst.Cmds)
		r.sync()
	} else {
		r.PrintDebug("inst != nil")
		inst.allDeps[Q2QReply.AcceptorId] = Q2QReply.Deps
		inst.reach[Q2QReply.AcceptorId] = true
		inst.committed[Q2QReply.Replica] = true
		inst.committed[Q2QReply.AcceptorId] = true
		inst.committed[r.Id] = true
		r.PrintDebug("inst.allDeps", inst.allDeps)
		r.PrintDebug("inst.reach", inst.reach)
		//inst.Deps = r.updatePriority(inst.Deps, N2QReply.Deps, N2QReply.Replica)

		priorityOKs := false

		for i := r.N / 2; i < r.N; i++ {
			if inst.reach[i] {
				priorityOKs = true
				break
			}
		}

		for i := 0; i < r.N/2; i++ {
			if !inst.reach[i] {
				priorityOKs = false
				break
			}
		}

		quorumCommitted := true
		if priorityOKs {
			quorumCommitted, inst.Deps = r.updatePriority(inst.allDeps, Q2QReply.Replica)
		}

		allCommitted := true
		r.PrintDebug("r.CommittedUpTo", r.CommittedUpTo)
		r.PrintDebug("inst.Deps", inst.Deps)
		for q := 0; q < r.N; q++ {
			if r.CommittedUpTo[q] < inst.Deps[q] {
				allCommitted = false
			}
		}
		precondition := priorityOKs && quorumCommitted && allCommitted
		r.PrintDebug("Q2QReply.Replica", Q2QReply.Replica, "Q2QReply.Instance", Q2QReply.Instance, "priorityOKs", priorityOKs, "quorumCommitted", quorumCommitted, "allCommitted", allCommitted, "precondition", precondition)
		if precondition {
			inst.Status = COMMITTED
			if Q2QReply.Replica == r.Id {
				if inst.lb.clientProposals != nil && !r.Dreply {
					for i := 0; i < len(inst.lb.clientProposals); i++ {
						r.ReplyProposeTS(
							&defs.ProposeReplyTS{
								OK:        TRUE,
								CommandId: inst.lb.clientProposals[i].CommandId,
								Value:     state.NIL(),
								Timestamp: inst.lb.clientProposals[i].Timestamp},
							inst.lb.clientProposals[i].Reply,
							inst.lb.clientProposals[i].Mutex)
					}
				}
			}
			r.recordInstanceMetadata(inst)
			r.sync()

			r.updateCommitted(Q2QReply.Replica)
			r.recordCommands(inst.Cmds)

			r.M.Lock()
			r.Stats.M["fast"]++
			if inst.proposeTime != 0 {
				r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
			}
			r.M.Unlock()
		}
	}
}

func (r *Replica) handleCommit(commit *Commit) {
	r.PrintDebug("handleCommit", commit.Replica, commit.Instance, commit.Ballot)
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance > r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance
	}

	if commit.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = commit.Ballot
	}

	if inst == nil {
		r.InstanceSpace[commit.Replica][commit.Instance] = r.newInstanceDefault(commit.Replica, commit.Instance)
		inst = r.InstanceSpace[commit.Replica][commit.Instance]
	}

	if inst.Status >= COMMITTED {
		return
	}

	if commit.Ballot < inst.bal {
		return
	}

	// FIXME timeout on client side?
	if commit.Replica == r.Id {
		if len(commit.Command) == 1 && commit.Command[0].Op == state.NONE && inst.lb.clientProposals != nil {
			for _, p := range inst.lb.clientProposals {
				r.Printf("In %d.%d, re-proposing %s \n", commit.Replica, commit.Instance, p.Command.String())
				r.ProposeChan <- p
			}
			inst.lb.clientProposals = nil
		}
	}

	inst.bal = commit.Ballot
	inst.vbal = commit.Ballot
	inst.Cmds = commit.Command
	inst.Seq = commit.Seq
	inst.Deps = commit.Deps
	inst.Status = COMMITTED

	r.PrintDebug("updateConflicts in handleCommit")
	r.updateConflicts(commit.Command, commit.Replica, commit.Instance)
	r.updateCommitted(commit.Replica)
	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	r.recordCommands(commit.Command)

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
	lb.ballot = inst.vbal
	lb.seq = inst.Seq
	lb.cmds = inst.Cmds
	lb.deps = inst.Deps
	lb.status = inst.Status
	r.makeBallot(replica, instance)

	inst.bal = lb.lastTriedBallot
	inst.vbal = lb.lastTriedBallot
	preply := &Q2NReply{
		r.Id,
		replica,
		instance,
		inst.Cmds,
		inst.Deps,
	}

	lb.prepareReplies = append(lb.prepareReplies, preply)
	lb.leaderResponded = r.Id == replica

	r.bcastPrepare(replica, instance)
}

func (r *Replica) findPreAcceptConflicts(cmds []state.Command, replica int32, instance int32, seq int32, deps []int32) (bool, int32, int32) {
	inst := r.InstanceSpace[replica][instance]
	if inst != nil && len(inst.Cmds) > 0 {
		if inst.Status >= ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}
		if inst.Seq == seq && equal(inst.Deps, deps) {
			// already PRE-ACCEPTED, no point looking for conflicts again
			return false, replica, instance
		}
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
					(i < deps[q] && inst.Seq >= seq && (q != replica || inst.Status > PREACCEPTED_EQ)) {
					// this is a conflict
					return true, q, i
				}
			}
		}
	}
	return false, -1, -1
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
	deps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		deps[i] = -1
	}
	return r.newInstance(replica, instance, nil, -1, -1, NONE, -1, deps)
}

func (r *Replica) newInstance(replica int32, instance int32, cmds []state.Command, cballot int32, lballot int32, status int8, seq int32, deps []int32) *Instance {
	reach := make([]bool, r.N)
	reach[replica] = true
	return &Instance{cmds, cballot, lballot, status, seq, deps, nil, 0, 0, nil, time.Now().UnixNano(), &instanceId{replica, instance}, reach, make([]bool, r.N), make([][]int32, r.N), false}
}

func (r *Replica) newLeaderBookkeepingDefault() *LeaderBookkeeping {
	return r.newLeaderBookkeeping(nil, r.newNilDeps(), r.newNilDeps(), r.newNilDeps(), 0, nil, NONE, -1)
}

func (r *Replica) newLeaderBookkeeping(p []*defs.GPropose, originalDeps []int32, committedDeps []int32, deps []int32, lastTriedBallot int32, cmds []state.Command, status int8, seq int32) *LeaderBookkeeping {
	return &LeaderBookkeeping{p, -1, true, 0, 0, 0, originalDeps, committedDeps, nil, true, false, make([]bool, r.N), 0, false, lastTriedBallot, cmds, status, seq, deps, false}
}

func (r *Replica) newNilDeps() []int32 {
	nildeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		nildeps[i] = -1
	}
	return nildeps
}
