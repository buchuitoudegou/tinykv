// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	peerMap := make(map[uint64]*Progress)
	for _, peerId := range c.peers {
		peerMap[peerId] = &Progress{
			Next:  0,
			Match: 0,
		}
	}
	raftLog := newLog(c.Storage)
	raftLog.applied = c.Applied
	retRaft := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          raftLog,
		Prs:              peerMap,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             []pb.Message{},
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
	hardState, _, err := c.Storage.InitialState()
	if err == nil {
		retRaft.Term = hardState.Term
		retRaft.Vote = hardState.Vote
		retRaft.RaftLog.committed = hardState.Commit
	}
	return retRaft
}

func (r *Raft) randomizeElectionTimeout() int {
	return rand.Intn(20) + 10
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msg := pb.Message{
		Term:    r.Term,
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgAppend,
		Commit:  r.RaftLog.committed,
	}
	lstIndex := r.RaftLog.LastIndex()
	if lstIndex >= r.Prs[to].Next {
		begIdx, endIdx := r.Prs[to].Next, lstIndex
		prevLogIndex := begIdx - 1
		prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
		sendEntries := r.RaftLog.GetEntries(int(begIdx), int(endIdx))
		msg.Index = prevLogIndex
		msg.LogTerm = prevLogTerm
		for i := range sendEntries {
			msg.Entries = append(msg.Entries, &sendEntries[i])
		}
		r.msgs = append(r.msgs, msg)
		return true
	} else {
		// no need to replicate log
		// update other data
		prevLogIndex := r.RaftLog.LastIndex()
		prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
		msg.Index = prevLogIndex
		msg.LogTerm = prevLogTerm
		r.msgs = append(r.msgs, msg)
		return true
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		// Index:   r.RaftLog.LastIndex(),
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		{
			r.electionElapsed++
			if r.electionElapsed == r.electionTimeout {
				// become candidate and broadcast vote requests
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgHup,
					From:    r.id,
					To:      r.id,
				})
			}
		}
	case StateCandidate:
		{
			r.electionElapsed++
			if r.electionElapsed == r.electionTimeout {
				// start new election term
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgHup,
					From:    r.id,
					To:      r.id,
				})
			}
		}
	case StateLeader:
		{
			r.heartbeatElapsed++
			if r.heartbeatElapsed == r.heartbeatTimeout {
				for id := range r.Prs {
					if id != r.id {
						r.sendHeartbeat(id)
					}
				}
				r.heartbeatElapsed = 0
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.votes = make(map[uint64]bool) // clear the votes
	r.Term = term
	r.Lead = lead
	r.Vote = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.votes = make(map[uint64]bool) // clear the votes
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = 0
	r.votes[r.id] = true // vote for itself
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.Lead = r.id
	r.Vote = 0
	for id := range r.Prs {
		if id != r.id {
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
			r.Prs[id].Match = 0
		} else {
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	noobEntry := pb.Entry{
		Index:     0,
		Term:      r.Term,
		EntryType: pb.EntryType_EntryNormal,
		Data:      nil,
	}
	voteMsg := ""
	for v := range r.votes {
		if r.votes[v] {
			voteMsg += fmt.Sprintf("%d,", v)
		}
	}
	fmt.Printf("%d become leader, vote by %s at term %d\n", r.id, voteMsg, r.Term)
	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&noobEntry},
	})
}

func (r *Raft) broadcastVoteRequest() {
	lstLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	for key := range r.Prs {
		if key == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			To:      key,
			From:    r.id,
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: lstLogTerm,
		})
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	rejectVoteMsg := pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		Reject:  true,
	}
	switch r.State {
	case StateFollower:
		{
			switch m.MsgType {
			case pb.MessageType_MsgHup:
				{
					if len(r.Prs) == 1 {
						r.becomeCandidate()
						r.becomeLeader() // win immediately
						break
					}
					r.becomeCandidate()
					r.electionElapsed = 0
					r.electionTimeout = r.randomizeElectionTimeout()
					// broadcast
					r.broadcastVoteRequest()
				}
			case pb.MessageType_MsgHeartbeat:
				r.handleHeartbeat(m)
			case pb.MessageType_MsgRequestVote:
				{
					// request a vote
					if r.Term > m.Term || (r.Term == m.Term && r.Vote != 0 && r.Vote != m.From) {
						// 1. current term is larger
						// 2. has voted for the other node in this term
						r.msgs = append(r.msgs, rejectVoteMsg)
						break
					}
					if m.Term > r.Term {
						r.becomeFollower(m.Term, 0) // larger term found
					}
					curLstLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
					if (curLstLogTerm < m.LogTerm) || (curLstLogTerm == m.LogTerm && m.Index >= r.RaftLog.LastIndex()) {
						// at least up-to-date:
						// 1. ends with different term, then the larger term is more up-to-date
						// 2. ends with same term, then the longer logs is more up-to-date
						r.msgs = append(r.msgs, pb.Message{
							From:    r.id,
							To:      m.From,
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							Term:    r.Term,
							Reject:  false, // grant vote
						})
						r.Vote = m.From
					} else {
						r.msgs = append(r.msgs, rejectVoteMsg)
					}
				}
			case pb.MessageType_MsgRequestVoteResponse:
			case pb.MessageType_MsgHeartbeatResponse:
			case pb.MessageType_MsgBeat:
			case pb.MessageType_MsgAppend:
				r.handleAppendEntries(m)
			case pb.MessageType_MsgPropose:
			}
		}
	case StateCandidate:
		{
			switch m.MsgType {
			case pb.MessageType_MsgHup:
				{
					if len(r.Prs) == 1 {
						r.becomeCandidate()
						r.becomeLeader() // win immediately
						break
					}
					r.becomeCandidate()
					r.electionElapsed = 0
					r.electionTimeout = r.randomizeElectionTimeout()
					// broadcast
					r.broadcastVoteRequest()
				}
			case pb.MessageType_MsgRequestVote:
				{
					if r.Term < m.Term {
						// set to term and become follower
						r.becomeFollower(m.Term, 0)
						r.msgs = append(r.msgs, pb.Message{
							From:    r.id,
							To:      m.From,
							MsgType: pb.MessageType_MsgRequestVoteResponse,
							Term:    r.Term,
							Reject:  false, // grant vote
						})
						r.Vote = m.From
					} else {
						r.msgs = append(r.msgs, rejectVoteMsg)
					}
				}
			case pb.MessageType_MsgRequestVoteResponse:
				{
					if !m.Reject {
						// grant vote
						r.votes[m.From] = true
					} else {
						r.votes[m.From] = false
						if m.Term > r.Term {
							r.becomeFollower(m.Term, 0)
							break
						}
					}
					// count the vote
					count := 0
					reject := 0
					for _, grant := range r.votes {
						if grant {
							count++
						} else {
							reject++
						}
					}
					if count > len(r.Prs)/2 {
						// win election
						r.becomeLeader()
					}
					if reject > len(r.Prs)/2 {
						r.becomeFollower(r.Term, 0)
					}
				}
			case pb.MessageType_MsgHeartbeat:
				r.handleHeartbeat(m)
			case pb.MessageType_MsgHeartbeatResponse:
			case pb.MessageType_MsgBeat:
			case pb.MessageType_MsgAppend:
				{
					r.handleAppendEntries(m)
				}
			case pb.MessageType_MsgPropose:
			}
		}
	case StateLeader:
		{
			switch m.MsgType {
			case pb.MessageType_MsgHup:
			case pb.MessageType_MsgHeartbeat:
				r.handleHeartbeat(m)
			case pb.MessageType_MsgHeartbeatResponse:
				{
					// handle heart beat response
					if m.Term > r.Term {
						r.becomeFollower(m.Term, m.From)
						break
					}
					lstLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
					if m.Index != r.RaftLog.LastIndex() || m.LogTerm != lstLogTerm {
						r.sendAppend(m.From)
					}
				}
			case pb.MessageType_MsgRequestVote:
				{
					if m.Term > r.Term {
						// larger term is found
						r.becomeFollower(m.Term, 0)
						curLstLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
						if curLstLogTerm < m.LogTerm || (curLstLogTerm == m.LogTerm && r.RaftLog.LastIndex() <= m.Index) {
							// at least up-to-date:
							// 1. ends with different terms, larger term is more up-to-date
							// 2. ends with same term, longer log is more up-to-date
							r.msgs = append(r.msgs, pb.Message{
								From:    r.id,
								To:      m.From,
								MsgType: pb.MessageType_MsgRequestVoteResponse,
								Term:    r.Term,
								Reject:  false, // grant vote
							})
							r.Vote = m.From
							break
						}
					}
					r.msgs = append(r.msgs, rejectVoteMsg)
				}
			case pb.MessageType_MsgRequestVoteResponse:
			case pb.MessageType_MsgBeat:
				{
					for id := range r.Prs {
						if id != r.id {
							r.sendHeartbeat(id)
						}
					}
				}
			case pb.MessageType_MsgAppend:
				{
					r.handleAppendEntries(m)
				}
			case pb.MessageType_MsgAppendResponse:
				{
					if m.Term > r.Term {
						// new term
						r.becomeFollower(m.Term, 0)
						break
					}
					if !m.Reject {
						// success
						r.Prs[m.From].Match = m.Index
						r.Prs[m.From].Next = r.Prs[m.From].Match + 1
					} else {
						skipTerm, _ := r.RaftLog.Term(r.Prs[m.From].Next - 1) // prevLogTerm
						for i := r.Prs[m.From].Next - 1; i >= 1; i-- {
							thisTerm, _ := r.RaftLog.Term(i)
							if thisTerm != skipTerm {
								r.Prs[m.From].Next = i
								break
							}
							if i == 1 {
								// replicate the whole log
								r.Prs[m.From].Next = 1
							}
						}
						r.sendAppend(m.From) // re-sync the log
						break
					}
					// find commit index
					voteToCommit := make(map[int]int)
					for id := range r.Prs {
						curIdx := r.Prs[id].Match
						if _, ok := voteToCommit[int(curIdx)]; ok {
							// value exists
							voteToCommit[int(curIdx)]++
						} else {
							// value not exists
							voteToCommit[int(curIdx)] = 1
						}
					}
					commitIdx := r.RaftLog.committed
					for candidateIdx := range voteToCommit {
						curLogTerm, _ := r.RaftLog.Term(uint64(candidateIdx))
						if curLogTerm != r.Term {
							// only commit logs of current term
							continue
						}
						voteCount := voteToCommit[candidateIdx]
						for otherIdx := range voteToCommit {
							if otherIdx > candidateIdx {
								voteCount++
							}
						}
						if voteCount > len(r.Prs)/2 && commitIdx < uint64(candidateIdx) {
							commitIdx = uint64(candidateIdx)
						}
					}
					if commitIdx != r.RaftLog.committed {
						// commit new entry
						fmt.Printf("node %d commit log %d\n", r.id, commitIdx)
						r.RaftLog.committed = uint64(commitIdx)
						for id := range r.Prs {
							if id != r.id {
								r.sendAppend(id) // update commit
							}
						}
					}
				}
			case pb.MessageType_MsgPropose:
				{
					// todo: append an entry to RaftLog
					for i := range m.Entries {
						r.RaftLog.AppendEntry(r.RaftLog.NextIndex(), pb.Entry{
							Index:     r.RaftLog.NextIndex(),
							Term:      r.Term,
							EntryType: m.Entries[i].EntryType,
							Data:      m.Entries[i].Data,
						})
					}
					r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
					r.Prs[r.id].Match = r.RaftLog.LastIndex()
					for id := range r.Prs {
						if id != r.id {
							r.sendAppend(id)
						}
					}
					if len(r.Prs) == 1 {
						// commit immediately
						r.RaftLog.committed = r.RaftLog.LastIndex()
					}
				}
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	replyMsg := pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
	}
	switch r.State {
	case StateFollower:
		{
			if r.Term > m.Term {
				// reject
				replyMsg.Reject = true
				r.msgs = append(r.msgs, replyMsg)
				return
			}
			if r.Term != m.Term || r.Lead != m.From {
				r.becomeFollower(m.Term, m.From)
			}
			if m.Index > r.RaftLog.LastIndex() {
				// middle log missed
				replyMsg.Reject = true
				replyMsg.Index = r.RaftLog.LastIndex()
				r.msgs = append(r.msgs, replyMsg)
				return
			}
			logTerm, _ := r.RaftLog.Term(m.Index)
			if logTerm != m.LogTerm {
				replyMsg.Reject = true
				// delete all entries from prevLogIndex
				// r.RaftLog.DeleteFromIdx(int(m.Index) - 1)
				replyMsg.Index = r.RaftLog.LastIndex()
				r.msgs = append(r.msgs, replyMsg)
				return
			}
			// legal
			for i := range m.Entries {
				r.RaftLog.AppendEntry(m.Entries[i].Index, *m.Entries[i])
			}
			if m.Commit > r.RaftLog.committed {
				// commit min(leaderCommit, last new entry)
				// last new entry: the entry replicated from the leader
				// entries after it is possibly illegal
				if len(m.Entries) > 0 {
					lstEntriesIdx := m.Entries[len(m.Entries)-1].Index
					r.RaftLog.committed = min(m.Commit, min(r.RaftLog.LastIndex(), lstEntriesIdx))
				} else {
					// no new entry
					// prevLogIdx is the newest
					r.RaftLog.committed = min(m.Commit, m.Index)
				}
				fmt.Printf("node %d commit up to %d\n", r.id, r.RaftLog.committed)
			}
			replyMsg.Reject = false
			replyMsg.Index = r.RaftLog.LastIndex()
			r.msgs = append(r.msgs, replyMsg)
		}
	case StateLeader:
		{
			replyMsg.Reject = true
			if r.Term < m.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.msgs = append(r.msgs, replyMsg)
		}
	case StateCandidate:
		{
			replyMsg.Reject = true
			if r.Term <= m.Term {
				// 1. leader of new term
				// 2. leader of this term
				r.becomeFollower(m.Term, m.From)
			}
			r.msgs = append(r.msgs, replyMsg)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	heartbeatResp := pb.Message{
		To:      m.From,
		From:    r.id,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		Reject:  m.Term >= r.Term,
	}
	switch r.State {
	case StateFollower:
		{
			if m.Term > r.Term {
				r.Term = m.Term
				if r.Lead != m.From {
					r.becomeFollower(m.Term, m.From)
				}
			}
			if m.Term >= r.Term {
				// valid hearbeat
				r.electionElapsed = 0 // make zero
				heartbeatResp.Index = r.RaftLog.LastIndex()
				heartbeatResp.LogTerm, _ = r.RaftLog.Term(r.RaftLog.LastIndex())
			}
			r.msgs = append(r.msgs, heartbeatResp)
		}
	case StateCandidate:
		{
			if m.Term >= r.Term {
				// follow the leader
				r.becomeFollower(m.Term, m.From)
				r.electionElapsed = 0
				heartbeatResp.Index = r.RaftLog.LastIndex()
				heartbeatResp.LogTerm, _ = r.RaftLog.Term(r.RaftLog.LastIndex())
			}
			r.msgs = append(r.msgs, heartbeatResp)
		}
	case StateLeader:
		{
			if m.Term > r.Term {
				// follow the newer leader
				// note that the terms can't be the same since
				// every term has only one leader
				r.becomeFollower(m.Term, m.From)
				r.electionElapsed = 0
				heartbeatResp.Index = r.RaftLog.LastIndex()
			}
			r.msgs = append(r.msgs, heartbeatResp)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
