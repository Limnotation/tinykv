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
	"math"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"go.uber.org/zap"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated.
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	// For now, we assume that `stabled + 1` is equivalent to `unstable.offset`,
	// and `entries[stabled + 1:]` is equivalent to `unstable.entries` in etcd/raft.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	logger *zap.Logger

	// maxNextEntsSize is the maximum number aggregate byte size of the messages
	// returned from calls to nextEnts.
	maxNextEntsSize uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	logger, _ := zap.NewProduction()

	log := &RaftLog{
		storage:         storage,
		logger:          logger,
		maxNextEntsSize: math.MaxUint64,
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.logger.Fatal("failed to get first index from storage", zap.Error(err))
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.logger.Fatal("failed to get last index from storage", zap.Error(err))
	}

	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	log.stabled = lastIndex

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	ents, err := l.entriesRange(l.firstIndex(), math.MaxUint64)
	if err == nil {
		return ents
	}
	if err == ErrCompacted {
		return l.allEntries()
	}

	l.logger.Fatal("failed to get all entries", zap.Error(err))
	return nil
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}

	return l.entries[l.stabled+1-l.entries[0].Index:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// If the applied index is smaller than the index of snapshot,
	// would return all committed entries adter the index of snapshot.
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			l.logger.Fatal("failed to get entries from storage", zap.Error(err))
		}
		return ents
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 && (l.stabled < l.entries[len(l.entries)-1].Index) {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}

	i, err := l.storage.LastIndex()
	if err != nil {
		l.logger.Fatal("failed to get last index from storage", zap.Error(err))
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return 0, err
	}

	// The valid term range is [index of dummy entry, last index]
	dummyIndex := firstIndex - 1
	if i > l.LastIndex() || i < dummyIndex {
		return 0, nil
	}

	// Search in the unstable entries first.
	if len(l.entries) > 0 && (l.stabled < l.entries[len(l.entries)-1].Index) && i > l.stabled {
		return l.entries[i-l.stabled-1].Term, nil
	}

	// Search in the storage in case no find in unstable entries.
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}

	l.logger.Sugar().Panicf("unexpected error %v", err)
	return 0, err
}

// append appends entries to local raft log and return
// the index of the last entry in the raft log. At this
// point, the `index` field of each entry is already its
// absolute index in the raft log.
func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Sugar().Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}

	after := ents[0].Index
	switch {
	case after == l.LastIndex()+1:
		l.entries = append(l.entries, ents...)
	case after <= l.stabled:
		l.logger.Sugar().Infof("replace the unstable entries from index %d", after)
		// This might be wrong, need checked later.
		l.stabled = after - 1
		l.entries = ents
	default:
		l.logger.Sugar().Infof("truncate the unstable entries from index %d", after)
		l.entries = append([]pb.Entry{}, l.entries[:after]...)
		l.entries = append(l.entries, ents...)
	}

	return l.LastIndex()
}

// maybeCommit try commit raft log's committed index.
func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			l.logger.Sugar().Fatalf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}

		l.committed = tocommit
	}
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Sugar().Panicf("unexpected error %v", err)
	return 0
}

func (l *RaftLog) entriesRange(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}

	return l.slice(i, l.LastIndex()+1, maxsize)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *RaftLog) slice(lo, hi, maxsize uint64) ([]pb.Entry, error) {
	if err := l.mustCheckOutofBounds(lo, hi); err != nil {
		return nil, err
	}

	if lo == hi {
		return nil, nil
	}

	var ents []pb.Entry
	if lo <= l.stabled {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.stabled+1))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Sugar().Fatalf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.stabled+1))
		} else if err != nil {
			l.logger.Sugar().Panicf("unexpected error %v", err)
		}

		// No size limitation yet.

		ents = storedEnts
	}
	if hi > l.stabled && len(l.entries) > 0 {
		unstableLo := max(lo, l.stabled+1)
		unstableHi := hi
		l.mustCheckOutofBoundsUnstable(unstableLo, unstableHi)

		unstable := l.entries[unstableLo-l.entries[0].Index : unstableHi-l.entries[0].Index]
		if len(ents) > 0 {
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}

	return ents, nil
}

func (l *RaftLog) mustCheckOutofBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Sugar().Fatalf("invalid slice %d > %d", lo, hi)
	}

	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.LastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		l.logger.Sugar().Fatalf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}

func (l *RaftLog) mustCheckOutofBoundsUnstable(lo, hi uint64) {
	if lo > hi {
		l.logger.Sugar().Fatalf("invalid unstable.slice %d > %d", lo, hi)
	}

	upper := l.LastIndex()
	if lo < l.stabled+1 || hi > upper+1 {
		l.logger.Sugar().Fatalf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, l.stabled+1, upper)
	}
}

// firstIndex returns the index of first available entry in the raft log.
func (l *RaftLog) firstIndex() uint64 {
	// If raft log does have a pending snapshot, it should be in the init
	// process. At this point of time, `entries` must be nil.
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}

	index, err := l.storage.FirstIndex()
	if err != nil {
		l.logger.Sugar().Panicf("failed to get the first index from storage: %v", err)
	}
	return index
}

// maybeAppend tries to append entries to raft log.
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (uint64, bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi := index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Sugar().Fatalf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

// matchTerm return true if the raft log has entry at
// index `i` which has term equal to `term`.
func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				l.logger.Sugar().Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) lastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		l.logger.Sugar().Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.LastIndex())
}
