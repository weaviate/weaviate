//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package changelog

import (
	"maps"
	"sync/atomic"
)

// Set is an immutable snapshot of active change-logs for a shard, keyed by
// op-id. Phase 2 stores a Set via atomic.Pointer on Shard; the write-path tee
// does a single atomic load and returns early when the pointer is nil, which
// is the common production case.
//
// Mutation is copy-on-write: WithAdded and WithRemoved return fresh Sets
// without touching the receiver. Register/Unregister wrap the CAS loop so
// Shard code doesn't reinvent it.
type Set struct {
	logs map[string]*ChangeLog
}

// Get returns the log registered under opID, or nil if none.
func (s *Set) Get(opID string) *ChangeLog {
	if s == nil {
		return nil
	}
	return s.logs[opID]
}

// Len returns the number of registered logs.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.logs)
}

// ForEach calls fn for every registered log. Iteration order is undefined.
// Safe to call concurrently with Set construction because Set is immutable
// once returned from WithAdded/WithRemoved.
func (s *Set) ForEach(fn func(opID string, log *ChangeLog)) {
	if s == nil {
		return
	}
	for opID, cl := range s.logs {
		fn(opID, cl)
	}
}

// WithAdded returns a new Set containing every entry of s plus opID→log. If
// opID already exists in s, it is replaced. The receiver is unchanged.
func (s *Set) WithAdded(opID string, log *ChangeLog) *Set {
	var size int
	if s != nil {
		size = len(s.logs) + 1
	} else {
		size = 1
	}
	next := &Set{logs: make(map[string]*ChangeLog, size)}
	if s != nil {
		maps.Copy(next.logs, s.logs)
	}
	next.logs[opID] = log
	return next
}

// WithRemoved returns a new Set containing every entry of s except opID.
// Returns nil when the resulting set would be empty, so the caller can
// restore the atomic pointer's nil fast-path.
func (s *Set) WithRemoved(opID string) *Set {
	if s == nil || len(s.logs) == 0 {
		return nil
	}
	if _, ok := s.logs[opID]; !ok {
		return s
	}
	if len(s.logs) == 1 {
		return nil
	}
	next := &Set{logs: make(map[string]*ChangeLog, len(s.logs)-1)}
	for k, v := range s.logs {
		if k == opID {
			continue
		}
		next.logs[k] = v
	}
	return next
}

// Register CAS-swaps log into *ptr under opID. Safe for concurrent callers.
func Register(ptr *atomic.Pointer[Set], opID string, log *ChangeLog) {
	for {
		old := ptr.Load()
		next := old.WithAdded(opID, log)
		if ptr.CompareAndSwap(old, next) {
			return
		}
	}
}

// Unregister CAS-removes opID from *ptr. If the resulting set is empty, *ptr
// is left as nil so the tee's load-and-check fast-path returns early.
func Unregister(ptr *atomic.Pointer[Set], opID string) {
	for {
		old := ptr.Load()
		next := old.WithRemoved(opID)
		if ptr.CompareAndSwap(old, next) {
			return
		}
	}
}
