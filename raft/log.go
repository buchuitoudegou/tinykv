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
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
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
	entries []pb.Entry // starts from index 1

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	curLogIdx uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	newRaft := &RaftLog{
		storage:         storage,
		committed:       0,
		applied:         0,
		stabled:         0,
		entries:         []pb.Entry{},
		pendingSnapshot: nil,
		curLogIdx:       0,
	}
	// todo: copy entries from storage
	begIdx, err1 := storage.FirstIndex()
	endIdx, err2 := storage.LastIndex()
	if err2 == nil {
		newRaft.curLogIdx = endIdx
		newRaft.stabled = endIdx
	}
	if err1 == nil && err2 == nil && begIdx <= endIdx {
		newRaft.entries, _ = storage.Entries(begIdx, endIdx+1)
	}
	return newRaft
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.GetEntries(int(l.stabled)+1, int(l.LastIndex()))
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.GetEntries(int(l.applied)+1, int(l.committed))
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.curLogIdx
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if l.curLogIdx == 0 || i == 0 || i > l.LastIndex() {
		return 0, fmt.Errorf("index out of range")
	}
	if len(l.entries) > 0 && i >= l.entries[0].Index {
		// get term from entries in memory
		idx := i - l.entries[0].Index
		return l.entries[idx].Term, nil
	}
	// try get entry's term from storage
	if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
		// get last log term from snapshot
		return l.pendingSnapshot.Metadata.Term, nil
	}
	// get entry from storage
	// WARNING: the storage may probably compact the logs
	ents := l.GetEntries(int(i), int(i))
	if len(ents) > 0 {
		return ents[0].Term, nil
	} else {
		return 0, nil
	}
}

func (l *RaftLog) NextIndex() uint64 {
	return l.curLogIdx + 1
}

func (l *RaftLog) DeleteFromIdx(idx int) {
	if idx < 0 {
		return
	}
	if idx == 0 {
		// delete all
		l.entries = []pb.Entry{}
		l.curLogIdx = 0
	}
	if len(l.entries) == 0 {
		// decrease curLogIdx
		if int(l.curLogIdx) >= idx {
			l.curLogIdx = uint64(idx)
		}
	} else {
		if int(l.entries[0].Index) > idx {
			// try deleting index in storage
			// noop
		} else {
			gap := idx - int(l.entries[0].Index)
			if gap < len(l.entries) {
				if gap < 0 {
					// delete all from entries
					l.curLogIdx = l.entries[0].Index - 1
					l.entries = []pb.Entry{}
				} else {
					l.curLogIdx = l.entries[gap].Index
					l.entries = l.entries[:gap+1]
				}
			}
		}
	}
}

func (l *RaftLog) GetEntries(begIdx int, endIdx int) []pb.Entry {
	ret := []pb.Entry{}
	var err error
	if len(l.entries) == 0 {
		if l.curLogIdx < uint64(endIdx) {
			ret, err = l.storage.Entries(uint64(begIdx), l.curLogIdx+1)
		} else {
			ret, err = l.storage.Entries(uint64(begIdx), uint64(endIdx)+1)
		}
		if err != nil {
			return []pb.Entry{}
		}
		return ret
	} else if begIdx < int(l.entries[0].Index) {
		// WARNING: getting entries from storage is unreliable!
		ret, err = l.storage.Entries(uint64(begIdx), min(l.entries[0].Index+1, uint64(endIdx)+1))
		if err != nil {
			ret = []pb.Entry{}
		}
	}
	beg := begIdx - int(l.entries[0].Index)
	end := endIdx - int(l.entries[0].Index)
	for i := beg; i >= 0 && i <= end && i < len(l.entries); i++ {
		ret = append(ret, l.entries[i])
	}
	return ret
}

func (l *RaftLog) AppendEntry(index uint64, newEntry pb.Entry) {
	logTerm, _ := l.Term(index)
	if index == l.NextIndex() {
		l.entries = append(l.entries, newEntry)
		l.curLogIdx = index
	} else if index < l.NextIndex() && index >= 1 && newEntry.Term != logTerm {
		// 1. entry of the index already exists
		// 2. new entry has different term (i.e. different entry)
		// then modify the original entry and delete its followings
		i := index - l.entries[0].Index
		l.entries[i] = newEntry
		if index <= l.stabled {
			l.stabled = index - 1
		}
		if index <= l.applied {
			l.applied = index - 1
		}
		// delete the following entries
		l.entries = l.entries[:i+1]
		l.curLogIdx = newEntry.Index
	}
}

func (l *RaftLog) ApplySnapshot(snapshot *pb.Snapshot) {
	// clear entries
	l.entries = []pb.Entry{}
	l.applied, l.committed = snapshot.Metadata.Index, snapshot.Metadata.Index
	l.curLogIdx = snapshot.Metadata.Index
	l.pendingSnapshot = snapshot
}

func (l *RaftLog) GetSnapshot(begIdx uint64) *pb.Snapshot {
	if (len(l.entries) == 0 && begIdx <= l.LastIndex()) ||
		(len(l.entries) > 0 && begIdx < l.entries[0].Index) {
		// get entries from storage
		if l.pendingSnapshot != nil {
			return l.pendingSnapshot
		} else {
			snapshot, err := l.storage.Snapshot()
			if err != nil {
				return nil
			}
			return &snapshot
		}
	}
	return nil
}
