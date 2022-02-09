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
	}
	// todo: copy entries from storage
	begIdx, err1 := storage.FirstIndex()
	endIdx, err2 := storage.LastIndex()
	if err1 == nil && err2 == nil && begIdx <= endIdx {
		newRaft.entries, _ = storage.Entries(begIdx, endIdx+1)
		newRaft.stabled = endIdx
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
	ret := []pb.Entry{}
	for i := l.stabled; i < uint64(len(l.entries)); i++ {
		ret = append(ret, l.entries[i])
	}
	return ret
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ret := []pb.Entry{}
	for i := l.applied; i <= l.committed-1; i++ {
		ret = append(ret, l.entries[i])
	}
	return ret
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if int(i) > len(l.entries) || i <= 0 {
		return 0, fmt.Errorf("index out of range")
	}
	return l.entries[i-1].Term, nil
}

func (l *RaftLog) NextIndex() uint64 {
	return uint64(len(l.entries)) + 1
}

func (l *RaftLog) DeleteFromIdx(idx int) {
	if idx > 0 && idx < len(l.entries) {
		l.entries = l.entries[idx-1:]
	} else if idx == 0 {
		l.entries = []pb.Entry{}
	}
}

func (l *RaftLog) GetEntries(begIdx int, endIdx int) []pb.Entry {
	ret := []pb.Entry{}
	for i := begIdx - 1; i >= 0 && i < endIdx && i < len(l.entries); i++ {
		ret = append(ret, l.entries[i])
	}
	return ret
}

func (l *RaftLog) AppendEntry(index uint64, newEntry pb.Entry) {
	if index == l.NextIndex() {
		l.entries = append(l.entries, newEntry)
	} else if index < l.NextIndex() && index >= 1 && newEntry.Term != l.entries[index-1].Term {
		// 1. entry of the index already exists
		// 2. new entry has different term (i.e. different entry)
		// then modify the original entry and delete its followings
		l.entries[index-1] = newEntry
		if index <= l.stabled {
			l.stabled = index - 1
		}
		if index <= l.applied {
			l.applied = index - 1
		}
		// delete the following entries
		l.entries = l.entries[:index]
	}
}
