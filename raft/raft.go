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
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"go.uber.org/zap"
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

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate. This
	// is to avoid followers to timeout at the same time.
	randomizedElectionTimeout int

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

	RaftLgr *zap.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Parse config.
	lgr, _ := zap.NewProduction()
	prs := make(map[uint64]*Progress, 0)
	votes := make(map[uint64]bool, 0)
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  0,
		}
		votes[peer] = false
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		votes:            votes,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLgr:          lgr,
	}

	// All raft instances start as followers.
	r.becomeFollower(r.Term, None)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	if pr == nil {
		return false
	}

	m := pb.Message{}
	m.From = r.id
	m.To = to

	// See if there is any entries to send. For now, we do not
	// care about snapshots, so just return in case of error.
	term, errt := r.RaftLog.Term(pr.Next - 1)
	if errt != nil {
		return false
	}

	// TODO: Try get entries from log to send.
	ents := make([]*pb.Entry, 0)

	m.MsgType = pb.MessageType_MsgAppend
	m.Index = pr.Next - 1
	m.LogTerm = term
	m.Entries = ents
	m.Commit = r.RaftLog.committed

	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}

	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

// tickElection is run by followers and candidates after r.electionTimeout.
// mimic logic in etcd raft:
//  1. node must be `promotable` to be able to participate in election.
//  2. election elapsed must be greater than a randomized election timeout
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) promotable() bool {
	return r.Prs[r.id] != nil
}

// tickHeartbeat is run by leaders to send a MessageType_MsgHeartbeat to
// their followers after r.heartbeatTimeout.
func (r *Raft) tickHeartbeat() {
	r.electionElapsed++
	r.heartbeatElapsed++

	// Only leader can do heartbeats.
	if r.State != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Lead = lead
	r.State = StateFollower
	r.reset(term)

	r.RaftLgr.Sugar().Infof("[%d] became follower at term [%d]", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		r.RaftLgr.Fatal("invalid transition [leader -> candidate]")
	}

	r.reset(r.Term + 1)

	r.Vote = r.id
	r.State = StateCandidate
	r.RaftLgr.Sugar().Infof("[%d] became candidate at term [%d]", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		r.RaftLgr.Fatal("invalid transition [follower -> leader]")
	}

	r.Lead = r.id
	r.State = StateLeader
	r.reset(r.Term)

	// Newly elected leader need to append a noop entry on its term.
	// noopEnt := pb.Entry{Data: nil}

	r.RaftLgr.Sugar().Infof("[%d] became leader at term [%d]", r.id, r.Term)
}

// reset resets some state of current raft instance when instance
// state changes.
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch {
	case m.Term == 0:
		// Do nothing with local messages.
	case m.Term > r.Term:
		// A general principle is that, when receiving a message with a
		// higher term, the current raft instance should convert to or
		// remain as a follower.
		r.RaftLgr.Sugar().Infof("%x [term: %d] received a [%s] message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)

		// When receiving message with higher term from a leader, follow
		// that leader, follow no one otherwise.
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// For now, when receiving messages with lower terms, we simply
		// ignore them.
		r.RaftLgr.Sugar().Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
	}

	// All messages that are handled in the following block are guaranteed
	// to have m.Term >= r.Term.

	// TODO: add logic handle messages by message type.

	canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.campaign()
		case pb.MessageType_MsgRequestVote:
			if canVote {
				r.RaftLgr.Sugar().Infof("%x [vote: %x] cast vote for %x at term %d",
					r.id, r.Vote, m.From, r.Term)

				r.electionElapsed = 0
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    m.Term,
					Reject:  false,
				})
			} else {
				r.RaftLgr.Sugar().Infof("%x [vote: %x] rejected vote for %x at term %d",
					r.id, r.Vote, m.From, r.Term)

				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    m.Term,
					Reject:  true,
				})
			}
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		default:
			r.RaftLgr.Info("Message type that follower will not handle", zap.Any("type", m.MsgType))
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.campaign()
		case pb.MessageType_MsgRequestVote:
			if canVote {
				r.RaftLgr.Sugar().Infof("%x [vote: %x] cast vote for %x at term %d",
					r.id, r.Vote, m.From, r.Term)
				r.electionElapsed = 0
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Term: m.Term, Reject: false})
			} else {
				r.RaftLgr.Sugar().Infof("%x [vote: %x] rejected vote for %x at term %d",
					r.id, r.Vote, m.From, r.Term)
			}
		case pb.MessageType_MsgRequestVoteResponse:
			if m.Reject {
				r.votes[m.From] = false
			} else {
				r.votes[m.From] = true
			}

			tally := 0
			for _, v := range r.votes {
				if v {
					tally++
				}
			}
			if tally > len(r.Prs)/2 {
				// On winning the election, leader needs to send out `append` messages
				// to commit previous entries and notifying followers of its leadership.
				r.becomeLeader()
				r.bcastAppend()
			}
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		default:
			r.RaftLgr.Info("Message type that candidate will not handle", zap.Any("type", m.MsgType))
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.RaftLgr.Error("Ignoring MessageType_MsgHup because current role is already leader", zap.Any("id", r.id))
		case pb.MessageType_MsgBeat:
			r.bcastHeartbeat()
		default:
			r.RaftLgr.Info("Message type that leader will not handle", zap.Any("type", m.MsgType))
		}
	}
	return nil
}

func (r *Raft) campaign() {
	// Sanity check, better safe than sorry.
	if !r.promotable() {
		r.RaftLgr.Warn("campaign called on unpromotable node", zap.Any("id", r.id))
	}

	// No pre-vote process, head straight to vote. In raft, only candidates
	// can participate in election, so a state transition is needed.
	r.becomeCandidate()
	voteMsg := pb.MessageType_MsgRequestVote
	term := r.Term

	// When start election, always vote for self.
	r.votes[r.id] = true

	// Short cut: wins the election if there's only one node in the cluster.
	if len(r.votes) == 1 && r.votes[r.id] {
		r.becomeLeader()
	}

	// Iterate voter list and send vote request, but don't send vote message to self
	// since a candidate will always vote for itself.
	for voter := range r.votes {
		if voter == r.id {
			continue
		}

		r.RaftLgr.Sugar().Infof("[%d] sending vote request to [%d] at term [%d]", r.id, voter, term)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: voteMsg,
			To:      voter,
			From:    r.id,
			Term:    term,
		})
	}
}

func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
