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

const DefaultShardedLocksCount = 512

type ShardedLocks struct {
	// number of locks
	count int
	// ensures single LockAll and multiple RLockAll, Lock and RLock
	// LockAll is exclusive to RLockAll, Lock, RLock
	writeAll *sync.RWMutex
	// indicates whether write of any single shard is ongoing, exclusive with readAll
	writeAny *sync.RWMutex
	// indicates whether read of all shards is ongoing, exclusive with writeAny
	readAll *sync.RWMutex
	// allows safe transition between writeAny and readAll
	change *sync.RWMutex
	// sharded locks
	shards []*sync.RWMutex
}

func NewDefaultShardedLocks() *ShardedLocks {
	return NewShardedLocks(DefaultShardedLocksCount)
}

func NewShardedLocks(count int) *ShardedLocks {
	if count < 2 {
		count = 2
	}

	writeAll := new(sync.RWMutex)
	writeAny := new(sync.RWMutex)
	readAll := new(sync.RWMutex)
	change := new(sync.RWMutex)
	shards := make([]*sync.RWMutex, count)
	for i := 0; i < count; i++ {
		shards[i] = new(sync.RWMutex)
	}

	return &ShardedLocks{
		count:    count,
		writeAll: writeAll,
		readAll:  readAll,
		writeAny: writeAny,
		change:   change,
		shards:   shards,
	}
}

func (sl *ShardedLocks) LockAll() {
	sl.writeAll.Lock()
}

func (sl *ShardedLocks) UnlockAll() {
	sl.writeAll.Unlock()
}

func (sl *ShardedLocks) LockedAll(callback func()) {
	sl.LockAll()
	defer sl.UnlockAll()

	callback()
}

func (sl *ShardedLocks) Lock(id uint64) {
	sl.writeAll.RLock()
	sl.markOngoingWriteAny()
	sl.shards[sl.mid(id)].Lock()
}

func (sl *ShardedLocks) Unlock(id uint64) {
	sl.shards[sl.mid(id)].Unlock()
	sl.writeAny.RUnlock()
	sl.writeAll.RUnlock()
}

func (sl *ShardedLocks) Locked(id uint64, callback func()) {
	sl.Lock(id)
	defer sl.Unlock(id)

	callback()
}

func (sl *ShardedLocks) RLockAll() {
	sl.writeAll.RLock()
	sl.markOngoingReadAll()
}

func (sl *ShardedLocks) RUnlockAll() {
	sl.readAll.RUnlock()
	sl.writeAll.RUnlock()
}

func (sl *ShardedLocks) RLockedAll(callback func()) {
	sl.RLockAll()
	defer sl.RUnlockAll()

	callback()
}

func (sl *ShardedLocks) RLock(id uint64) {
	sl.writeAll.RLock()
	sl.shards[sl.mid(id)].RLock()
}

func (sl *ShardedLocks) RUnlock(id uint64) {
	sl.shards[sl.mid(id)].RUnlock()
	sl.writeAll.RUnlock()
}

func (sl *ShardedLocks) RLocked(id uint64, callback func()) {
	sl.RLock(id)
	defer sl.RUnlock(id)

	callback()
}

func (sl *ShardedLocks) mid(id uint64) uint64 {
	return id % uint64(sl.count)
}

func (sl *ShardedLocks) markOngoingWriteAny() {
	sl.change.RLock()
	defer sl.change.RUnlock()

	// wait until no ongoing readAll
	sl.readAll.Lock()
	// mark ongoing writeAny
	sl.writeAny.RLock()
	sl.readAll.Unlock()
}

func (sl *ShardedLocks) markOngoingReadAll() {
	sl.change.Lock()
	defer sl.change.Unlock()

	// wait until no ongoing writeAny
	sl.writeAny.Lock()
	// mark ongoing readAll
	sl.readAll.RLock()
	sl.writeAny.Unlock()
}
