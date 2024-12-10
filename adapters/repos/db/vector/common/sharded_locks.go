//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import "sync"

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

	return &ShardedRWLocks{
		shards:   make([]sync.RWMutex, count),
		count:    count,
		PageSize: DefaultPageSize,
	}
}

func NewShardedRWLocksWithPageSize(pageSize uint64) *ShardedRWLocks {
	return &ShardedRWLocks{
		shards:   make([]sync.RWMutex, DefaultShardedLocksCount),
		count:    DefaultShardedLocksCount,
		PageSize: pageSize,
	}
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

func (sl *ShardedRWLocks) Lock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.count].Lock()
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
