package eppaxos

import (
	"sort"

	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func (e *Exec) executeCommand(replica int32, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.Status == EXECUTED {
		return true
	}
	if inst.Status != COMMITTED {
		return false
	}

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	// find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	ret := e.strongconnect(root, &index)
	// reset all indexes in the stack
	for j := 0; j < len(stack); j++ {
		stack[j].Index = 0
	}
	return ret
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	if v.Cmds == nil {
		return false
	}

	for q := int32(0); q < int32(e.r.N); q++ {
		inst := v.Deps[q]
		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
			if e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil {
				return false
			}

			if e.r.transconf {
				for _, alpha := range v.Cmds {
					for _, beta := range e.r.InstanceSpace[q][i].Cmds {
						if !state.Conflict(&alpha, &beta) {
							continue
						}
					}
				}
			}

			if e.r.InstanceSpace[q][i].Status == EXECUTED {
				continue
			}

			for e.r.InstanceSpace[q][i].Status != COMMITTED {
				return false
			}

			w := e.r.InstanceSpace[q][i]

			if w.Index == 0 {
				if !e.strongconnect(w, index) {
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else if e.inStack(w) {
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for idx := 0; idx < len(w.Cmds); idx++ {
				shouldRespond := e.r.Dreply && w.lb != nil && w.lb.clientProposals != nil
				if w.Cmds[idx].Op == state.NONE {
					// nothing to do
				} else if shouldRespond {
					val := w.Cmds[idx].Execute(e.r.State)
					e.r.ReplyProposeTS(
						&defs.ProposeReplyTS{
							TRUE,
							w.lb.clientProposals[idx].CommandId,
							val,
							w.lb.clientProposals[idx].Timestamp},
						w.lb.clientProposals[idx].Reply,
						w.lb.clientProposals[idx].Mutex)
				} else if w.Cmds[idx].Op == state.PUT {
					w.Cmds[idx].Execute(e.r.State)
				}
			}
			w.Status = EXECUTED
		}
		stack = stack[0:l]
	}

	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq || (na[i].Seq == na[j].Seq && na[i].id.replica < na[j].id.replica) || (na[i].Seq == na[j].Seq && na[i].id.replica == na[j].id.replica && na[i].proposeTime < na[j].proposeTime)
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}
