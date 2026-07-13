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

package common

import (
	"sync"
	"sync/atomic"
)

const (
	DefaultShardedLocksCount = 512
	DefaultPageSize          = 1
)

type ShardedLocks struct {
	// sharded locks
	shards []sync.Mutex
	// number of locks
	count    uint64
	PageSize uint64
}

func NewDefaultShardedLocks() *ShardedLocks {
	return NewShardedLocks(DefaultShardedLocksCount)
}

func NewShardedLocks(count uint64) *ShardedLocks {
	if count < 2 {
		count = 2
	}

	return &ShardedLocks{
		shards:   make([]sync.Mutex, count),
		count:    count,
		PageSize: DefaultPageSize,
	}
}

func NewShardedLocksWithPageSize(pageSize uint64) *ShardedLocks {
	return &ShardedLocks{
		shards:   make([]sync.Mutex, DefaultShardedLocksCount),
		count:    DefaultShardedLocksCount,
		PageSize: pageSize,
	}
}

func (sl *ShardedLocks) LockAll() {
	for i := uint64(0); i < sl.count; i++ {
		sl.shards[i].Lock()
	}
}

func (sl *ShardedLocks) UnlockAll() {
	for i := int(sl.count) - 1; i >= 0; i-- {
		sl.shards[i].Unlock()
	}
}

func (sl *ShardedLocks) LockedAll(callback func()) {
	sl.LockAll()
	defer sl.UnlockAll()

	callback()
}

func (sl *ShardedLocks) Hash(id uint64) uint64 {
	return id % sl.count
}

func (sl *ShardedLocks) Lock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.count].Lock()
}

func (sl *ShardedLocks) Unlock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.count].Unlock()
}

func (sl *ShardedLocks) Locked(id uint64, callback func()) {
	sl.Lock(id)
	defer sl.Unlock(id)

	callback()
}

type ShardedRWLocks struct {
	// sharded locks
	shards []sync.RWMutex
	// number of locks
	count    uint64
	PageSize uint64
}

func NewDefaultShardedRWLocks() *ShardedRWLocks {
	return NewShardedRWLocks(DefaultShardedLocksCount)
}

func NewShardedRWLocks(count uint64) *ShardedRWLocks {
	if count < 2 {
		count = 2
	}

	return NewShardedRWLocksWith(count, DefaultPageSize)
}

func NewShardedRWLocksWith(pages, pageSize uint64) *ShardedRWLocks {
	return &ShardedRWLocks{
		shards:   make([]sync.RWMutex, pages),
		count:    pages,
		PageSize: pageSize,
	}
}

func NewShardedRWLocksWithPageSize(pageSize uint64) *ShardedRWLocks {
	return NewShardedRWLocksWith(DefaultShardedLocksCount, pageSize)
}

// Count returns the number of lock stripes.
func (sl *ShardedRWLocks) Count() uint64 {
	return sl.count
}

func (sl *ShardedRWLocks) LockAll() {
	for i := uint64(0); i < sl.count; i++ {
		sl.shards[i].Lock()
	}
}

func (sl *ShardedRWLocks) UnlockAll() {
	for i := int(sl.count) - 1; i >= 0; i-- {
		sl.shards[i].Unlock()
	}
}

func (sl *ShardedRWLocks) LockedAll(callback func()) {
	sl.LockAll()
	defer sl.UnlockAll()

	callback()
}

func (sl *ShardedRWLocks) Hash(id uint64) uint64 {
	return id % sl.count
}

func (sl *ShardedRWLocks) Lock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.count].Lock()
}

func (sl *ShardedRWLocks) TryLock(id uint64) bool {
	return sl.shards[(id/sl.PageSize)%sl.count].TryLock()
}

func (sl *ShardedRWLocks) Unlock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.count].Unlock()
}

func (sl *ShardedRWLocks) Locked(id uint64, callback func()) {
	sl.Lock(id)
	defer sl.Unlock(id)

	callback()
}

func (sl *ShardedRWLocks) RLockAll() {
	for i := uint64(0); i < sl.count; i++ {
		sl.shards[i].RLock()
	}
}

func (sl *ShardedRWLocks) RUnlockAll() {
	for i := int(sl.count) - 1; i >= 0; i-- {
		sl.shards[i].RUnlock()
	}
}

func (sl *ShardedRWLocks) RLockedAll(callback func()) {
	sl.RLockAll()
	defer sl.RUnlockAll()

	callback()
}

func (sl *ShardedRWLocks) RLock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.count].RLock()
}

func (sl *ShardedRWLocks) RUnlock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.count].RUnlock()
}

func (sl *ShardedRWLocks) RLocked(id uint64, callback func()) {
	sl.RLock(id)
	defer sl.RUnlock(id)

	callback()
}

// LazyShardedRWLocks provides the same locking API as ShardedRWLocks, but
// allocates its stripes on first use and can grow the stripe count later via
// EnsureCount. This lets components that exist in large numbers but are
// mostly idle (e.g. per-tenant vector caches) avoid paying for a full stripe
// array up front.
//
// Correctness of the stripe swap relies on one invariant: the stripe set is
// only ever replaced while every stripe of the previous set is held
// exclusively. Consequently no stripe of the old set can be held during a
// swap, so unlock operations may always resolve the current set, and only
// acquire operations need to re-check the set after acquiring (a goroutine
// that was blocked on an old stripe retries on the new set).
type LazyShardedRWLocks struct {
	locks atomic.Pointer[ShardedRWLocks]
	// mu serializes allocation and re-striping
	mu           sync.Mutex
	initialCount uint64

	PageSize uint64
}

// NewLazyShardedRWLocks creates sharded RW locks whose stripes are allocated
// on first use, starting with initialCount stripes.
func NewLazyShardedRWLocks(initialCount, pageSize uint64) *LazyShardedRWLocks {
	if initialCount < 2 {
		initialCount = 2
	}

	return &LazyShardedRWLocks{
		initialCount: initialCount,
		PageSize:     pageSize,
	}
}

// Count returns the number of allocated stripes: 0 before first use.
func (l *LazyShardedRWLocks) Count() uint64 {
	if locks := l.locks.Load(); locks != nil {
		return locks.Count()
	}
	return 0
}

// EnsureCount grows the stripe count to at least count, allocating the
// stripes if this is the first use. It never shrinks, and never allocates
// fewer stripes than the constructor's initial count: the initial layout
// must not depend on which operation touches the instance first.
func (l *LazyShardedRWLocks) EnsureCount(count uint64) {
	if count < l.initialCount {
		count = l.initialCount
	}

	// fast path: the count of an allocated set is immutable, so a single
	// atomic load suffices to detect the common no-op case without the mutex
	if locks := l.locks.Load(); locks != nil && count <= locks.Count() {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	locks := l.locks.Load()
	if locks == nil {
		l.locks.Store(NewShardedRWLocksWith(count, l.PageSize))
		return
	}
	if count <= locks.Count() {
		return
	}

	upgraded := NewShardedRWLocksWith(count, l.PageSize)
	// drain every holder of the current set before publishing the new one,
	// upholding the swap invariant documented on the type
	locks.LockAll()
	l.locks.Store(upgraded)
	// wake goroutines still blocked on the old set; they re-check the set
	// after acquiring and retry on the new one
	locks.UnlockAll()
}

// current returns the stripe set, allocating it on first use.
func (l *LazyShardedRWLocks) current() *ShardedRWLocks {
	if locks := l.locks.Load(); locks != nil {
		return locks
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if locks := l.locks.Load(); locks != nil {
		return locks
	}
	locks := NewShardedRWLocksWith(l.initialCount, l.PageSize)
	l.locks.Store(locks)
	return locks
}

func (l *LazyShardedRWLocks) Lock(id uint64) {
	for {
		locks := l.current()
		locks.Lock(id)
		if l.locks.Load() == locks {
			return
		}
		locks.Unlock(id)
	}
}

func (l *LazyShardedRWLocks) TryLock(id uint64) bool {
	for {
		locks := l.current()
		if !locks.TryLock(id) {
			return false
		}
		if l.locks.Load() == locks {
			return true
		}
		locks.Unlock(id)
	}
}

func (l *LazyShardedRWLocks) Unlock(id uint64) {
	l.locks.Load().Unlock(id)
}

func (l *LazyShardedRWLocks) RLock(id uint64) {
	for {
		locks := l.current()
		locks.RLock(id)
		if l.locks.Load() == locks {
			return
		}
		locks.RUnlock(id)
	}
}

func (l *LazyShardedRWLocks) RUnlock(id uint64) {
	l.locks.Load().RUnlock(id)
}

func (l *LazyShardedRWLocks) LockAll() {
	for {
		locks := l.current()
		locks.LockAll()
		if l.locks.Load() == locks {
			return
		}
		locks.UnlockAll()
	}
}

func (l *LazyShardedRWLocks) UnlockAll() {
	l.locks.Load().UnlockAll()
}

func (l *LazyShardedRWLocks) RLockAll() {
	for {
		locks := l.current()
		locks.RLockAll()
		if l.locks.Load() == locks {
			return
		}
		locks.RUnlockAll()
	}
}

func (l *LazyShardedRWLocks) RUnlockAll() {
	l.locks.Load().RUnlockAll()
}

func (l *LazyShardedRWLocks) Locked(id uint64, callback func()) {
	l.Lock(id)
	defer l.Unlock(id)

	callback()
}

func (l *LazyShardedRWLocks) RLocked(id uint64, callback func()) {
	l.RLock(id)
	defer l.RUnlock(id)

	callback()
}

func (l *LazyShardedRWLocks) LockedAll(callback func()) {
	l.LockAll()
	defer l.UnlockAll()

	callback()
}

func (l *LazyShardedRWLocks) RLockedAll(callback func()) {
	l.RLockAll()
	defer l.RUnlockAll()

	callback()
}
