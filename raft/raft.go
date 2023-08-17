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
	"sync"
	"time"

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

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
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

	maxMsgSize uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	lgr, _ := zap.NewProduction()
	if err := c.validate(); err != nil {
		lgr.Fatal("invalid config", zap.Error(err))
	}

	// Some states need to be restored from raft log, so
	// raft log need to be created first.
	raftLog := newLog(c.Storage)
	hs, _, err := c.Storage.InitialState()
	if err != nil {
		lgr.Fatal("failed to get initial state", zap.Error(err))
	}

	// Parse config.
	prs := make(map[uint64]*Progress, 0)
	votes := make(map[uint64]bool, 0)
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  raftLog.LastIndex() + 1,
		}
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		Prs:              prs,
		votes:            votes,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLgr:          lgr,
		maxMsgSize:       1024 * 1024,
	}

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}

	// All raft instances start as followers.
	r.becomeFollower(r.Term, None)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	return r.maybeSendAppend(to, true)
}

func (r *Raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
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

	// Try get entries from log to send.
	entss, err := r.RaftLog.entriesRange(pr.Next, r.maxMsgSize)
	if err != nil && len(entss) == 0 && !sendIfEmpty {
		return false
	}

	ents := messageStrucSliceToPtrSlice(entss)

	m.MsgType = pb.MessageType_MsgAppend
	m.Index = pr.Next - 1
	m.LogTerm = term
	m.Entries = ents
	m.Commit = r.RaftLog.committed
	m.Term = r.Term

	// When we do have entries to send to followers, we do an optimistic
	// update of the progress.
	if n := len(ents); n != 0 {
		li := ents[n-1].Index
		pr.Next = li + 1
	}

	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
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
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
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
	r.votes[r.id] = true
	r.State = StateCandidate
	r.RaftLgr.Sugar().Infof("[%d] became candidate at term [%d]", r.id, r.Term)
}

// appendEntry appends incoming entries to local raft log.
func (r *Raft) appendEntry(es ...*pb.Entry) bool {
	// Incoming entries' indices are relative to the slice, need to
	// transform them to absolute indices in the raft log.
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}

	// New entries might append/truncate existing entries in the log,
	// so might need to update the "last" index after the append.
	ess := messagePtrToStrucSlice(es)
	li = r.RaftLog.append(ess...)
	r.Prs[r.id].Match = li
	r.Prs[r.id].Next = li + 1

	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybecommit()
	return true
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *Raft) maybecommit() {
	// Find the largest index that a quorum of members have acknowledged,
	// and then try to advance to commit index to this largest index.
	//
	// Here is the logic for computing the largest index:
	//  1. Collect all peers' match index into a slice;
	//  2. Sort the slice, by index in ascending order;
	//  3. Suppose we have n nodes in the cluster, move from
	// 	   the right end of the slice by (n/2) + 1 steps, the
	//     value at that position is the largest commit index we need.
	//
	// For example, say we have five nodes in the quorum. From the perspective
	// of the leader, the match indices of the five nodes are:
	//  	[1, 3, 2, 2, 3]
	// sort this slice we would have:
	//		[1, 2, 2, 3, 3].
	// Move from the right end by (5/2)+1 = 3 steps, we get 2, which is the
	// largest commit index. This is very intuitive, we can see that over half
	// of the members have match index >= 2.
	// etcd/raft uses a much more complicated implementation, but for now we just
	// stick to this simple one.
	matchIndx := make([]uint64, 0)
	for _, pr := range r.Prs {
		matchIndx = append(matchIndx, pr.Match)
	}
	insertionSort(matchIndx)

	n := len(r.Prs)
	mci := matchIndx[n-(n/2+1)]

	r.RaftLog.maybeCommit(mci, r.Term)
}

// Copy directly from etcd/raft implementation.
func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		r.RaftLgr.Fatal("invalid transition [follower -> leader]")
	}

	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

	// Newly elected leader need to append and commit a noop entry on its term.
	noopEnt := pb.Entry{Data: nil}
	if !r.appendEntry(&noopEnt) {
		r.RaftLgr.Sugar().Panic("Empty entry was dropped.")
	}

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
	r.votes = map[uint64]bool{}
	for pr := range r.Prs {
		r.Prs[pr] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
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
			if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
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
		case pb.MessageType_MsgPropose:
			if r.Lead == None {
				r.RaftLgr.Sugar().Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
				return ErrProposalDropped
			} else {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleHeartbeat(m)
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
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					Term:    m.Term,
					Reject:  false,
				})
			} else {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    m.Term,
					Reject:  true,
				})
				r.RaftLgr.Sugar().Infof("%x [vote: %x] rejected vote for %x at term %d",
					r.id, r.Vote, m.From, r.Term)
			}
		case pb.MessageType_MsgRequestVoteResponse:
			_, ok := r.votes[m.From]
			if !ok {
				r.votes[m.From] = !m.Reject
			}

			tally := 0
			lost := 0
			for pr := range r.Prs {
				v, voted := r.votes[pr]
				if !voted {
					continue
				}

				if v {
					tally++
				} else {
					lost++
				}
			}
			if tally > len(r.Prs)/2 {
				// On winning the election, leader needs to send out `append` messages
				// to commit previous entries and notifying followers of its leadership.
				r.becomeLeader()
				r.bcastAppend()
			} else if lost > len(r.Prs)/2 {
				// At this point, the candidate has known that a higher term has occurred.
				r.becomeFollower(r.Term, None)
			}
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgPropose:
			// Candidate cannot process propose requests since it has no leader.
			r.RaftLgr.Sugar().Infof("%x [term: %d] no leader at term; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		default:
			r.RaftLgr.Info("Message type that candidate will not handle", zap.Any("type", m.MsgType))
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.RaftLgr.Error("Ignoring MessageType_MsgHup because current role is already leader", zap.Any("id", r.id))
		case pb.MessageType_MsgRequestVote:
			// When execution hits this branch, the messages's term must be
			// equally to current leader's term, in this case the leader can
			// certainly reject the vote request.
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    m.Term,
				Reject:  true,
			})
		case pb.MessageType_MsgBeat:
			r.bcastHeartbeat()
		case pb.MessageType_MsgPropose:
			// Propose messages containing no entries are not allowed.
			if len(m.Entries) == 0 {
				r.RaftLgr.Sugar().Panic("MessageType_MsgPropose contains no entries", zap.Any("id", r.id))
			}

			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			if r.Prs[r.id] == nil {
				return ErrProposalDropped
			}

			// Leader cannot process requests while leadership transfer is in progress.
			if r.leadTransferee != None {
				r.RaftLgr.Error(fmt.Sprintf("%x [term %d] transfer leadership to %x is in progress; dropping proposal",
					r.id, r.Term, r.leadTransferee))
				return ErrProposalDropped
			}

			// Append the proposal to the local log before broadcast to followers.
			if !r.appendEntry(m.Entries...) {
				return ErrProposalDropped
			}
			r.bcastAppend()
			return nil
		case pb.MessageType_MsgAppendResponse:
			if pr := r.Prs[m.From]; pr == nil {
				r.RaftLgr.Sugar().Warnf("[%x] no progress available for [%x]", r.id, m.From)
				return nil
			}

			if m.Reject {
				r.RaftLgr.Warn(fmt.Sprintf("%x [term %d] received MessageType_MsgAppend rejection from %x for index %d",
					r.id, r.Term, m.From, m.Index))

				// On failure, the leader might has the wrong progress info about
				// the follower who just rejected. In this case, extract the hint
				// from the follower's return message and update progress accordingly
				// and then resend the append message.
				r.Prs[m.From].Next = min(m.Index, m.RejectHint+1)
				r.sendAppend(m.From)
			} else {
				// Check if more update is needed, this is to avoid
				// infinite loop of sending append messages.
				if r.Prs[m.From].Match >= m.Index {
					return nil
				}

				// Update progress.
				if r.Prs[m.From].Match < m.Index {
					r.Prs[m.From].Match = m.Index
				}
				if r.Prs[m.From].Next < m.Index+1 {
					r.Prs[m.From].Next = m.Index + 1
				}

				// Update commit index if possible.
				reach := 0
				for _, pr := range r.Prs {
					if pr.Match > r.RaftLog.committed {
						reach++
					}
				}

				// If succeeded in updating the commit index, need
				// to update the commit index to all followers.
				if reach > len(r.Prs)/2 {
					r.maybecommit()
					r.bcastAppend()
				}
			}
		case pb.MessageType_MsgHeartbeatResponse:
			pr, exi := r.Prs[m.From]
			if !exi {
				r.RaftLgr.Panic("no progress available for this peer", zap.Any("id", m.From))
			}

			// Update progress if needed.
			if pr.Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
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
	if len(r.Prs) == 1 && r.votes[r.id] {
		r.becomeLeader()
	}

	// Iterate voter list and send vote request, but don't send vote message to self
	// since a candidate will always vote for itself.
	for voter := range r.Prs {
		if voter == r.id {
			continue
		}

		r.RaftLgr.Sugar().Infof("[%d] sending vote request to [%d] at term [%d]", r.id, voter, term)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: voteMsg,
			To:      voter,
			From:    r.id,
			Term:    term,
			LogTerm: r.RaftLog.lastTerm(),
			Index:   r.RaftLog.LastIndex(),
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

// bcastAppend broadcasts leader's entries to all followers.
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// handleAppendEntries handle AppendEntries RPC request for
// followers and candidates.
func (r *Raft) handleAppendEntries(m pb.Message) {
	// If the incoming message's index is smaller than current member's
	// commit index, respond with current member's commit index.
	if m.Index < r.RaftLog.committed {
		r.msgs = append(r.msgs, pb.Message{
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.committed,
		})
		return
	}

	// Try to append entries to local log, respond with the lastest raft log
	// index to the leader for it to update the progress when succeed.
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, messagePtrToStrucSlice(m.Entries)...); ok {
		r.msgs = append(r.msgs, pb.Message{
			To:      m.From,
			From:    r.id,
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			Index:   mlastIndex,
		})
	} else {
		r.RaftLgr.Warn(fmt.Sprintf("%x [term %d] rejected MessageType_MsgAppend from %x for index %d",
			r.id, r.Term, m.From, m.Index))
		r.msgs = append(r.msgs, pb.Message{
			To:         m.From,
			From:       r.id,
			MsgType:    pb.MessageType_MsgAppendResponse,
			Term:       r.Term,
			Index:      m.Index,
			Reject:     true,
			RejectHint: r.RaftLog.LastIndex(),
		})
	}
}

func (r *Raft) loadState(hs pb.HardState) {
	if hs.Commit < r.RaftLog.committed || hs.Commit > r.RaftLog.LastIndex() {
		r.RaftLgr.Sugar().Fatalf("%x state.commit %d is out of range [%d, %d]", r.id, hs.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}

	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
	r.Vote = hs.Vote
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.commitTo(m.Commit)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
	})
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
