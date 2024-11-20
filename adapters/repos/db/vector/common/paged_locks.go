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

import (
	"sync"
)

const DefaultPagedLocksCount = 512
const DefaultPagedLocksPageSize = 32

type PagedLocks struct {
	// paged locks
	shards []sync.Mutex
	// number of locks
	Count    uint64
	PageSize uint64
}

func NewDefaultPagedLocks() *PagedLocks {
	return NewPagedLocks(DefaultPagedLocksCount)
}

func NewPagedLocks(count uint64) *PagedLocks {
	if count < 2 {
		count = 2
	}

	return &PagedLocks{
		shards:   make([]sync.Mutex, count),
		Count:    count,
		PageSize: DefaultPagedLocksPageSize,
	}
}

func NewPagedLocksWithPageSize(count uint64, pageSize uint64) *PagedLocks {
	if count < 2 {
		count = 2
	}

	return &PagedLocks{
		shards:   make([]sync.Mutex, count),
		Count:    count,
		PageSize: pageSize,
	}
}

func (sl *PagedLocks) LockAll() {
	for i := uint64(0); i < sl.Count; i++ {
		sl.shards[i].Lock()
	}
}

func (sl *PagedLocks) UnlockAll() {
	for i := int(sl.Count) - 1; i >= 0; i-- {
		sl.shards[i].Unlock()
	}
}

func (sl *PagedLocks) LockedAll(callback func()) {
	sl.LockAll()
	defer sl.UnlockAll()

	callback()
}

func (sl *PagedLocks) Lock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.Count].Lock()
}

func (sl *PagedLocks) Unlock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.Count].Unlock()
}

func (sl *PagedLocks) Locked(id uint64, callback func()) {
	sl.Lock(id)
	defer sl.Unlock(id)

	callback()
}

type PagedRWLocks struct {
	// paged locks
	shards []sync.RWMutex
	// number of locks
	Count    uint64
	PageSize uint64
}

func NewDefaultPagedRWLocks() *PagedRWLocks {
	return NewPagedRWLocks(DefaultPagedLocksCount)
}

func NewPagedRWLocks(count uint64) *PagedRWLocks {
	if count < 2 {
		count = 2
	}

	return &PagedRWLocks{
		shards:   make([]sync.RWMutex, count),
		Count:    count,
		PageSize: DefaultPagedLocksPageSize,
	}
}

func NewPagedRWLocksWithPageSize(count uint64, pageSize uint64) *PagedRWLocks {
	if count < 2 {
		count = 2
	}

	return &PagedRWLocks{
		shards:   make([]sync.RWMutex, count),
		Count:    count,
		PageSize: pageSize,
	}
}

func (sl *PagedRWLocks) LockAll() {
	for i := uint64(0); i < sl.Count; i++ {
		sl.shards[i].Lock()
	}
}

func (sl *PagedRWLocks) UnlockAll() {
	for i := int(sl.Count) - 1; i >= 0; i-- {
		sl.shards[i].Unlock()
	}
}

func (sl *PagedRWLocks) LockedAll(callback func()) {
	sl.LockAll()
	defer sl.UnlockAll()

	callback()
}

func (sl *PagedRWLocks) Lock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.Count].Lock()
}

func (sl *PagedRWLocks) Unlock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.Count].Unlock()
}

func (sl *PagedRWLocks) Locked(id uint64, callback func()) {
	sl.Lock(id)
	defer sl.Unlock(id)

	callback()
}

func (sl *PagedRWLocks) RLockAll() {
	for i := uint64(0); i < sl.Count; i++ {
		sl.shards[i].RLock()
	}
}

func (sl *PagedRWLocks) RUnlockAll() {
	for i := int(sl.Count) - 1; i >= 0; i-- {
		sl.shards[i].RUnlock()
	}
}

func (sl *PagedRWLocks) RLockedAll(callback func()) {
	sl.RLockAll()
	defer sl.RUnlockAll()

	callback()
}

func (sl *PagedRWLocks) RLock(id uint64) {

	sl.shards[(id/sl.PageSize)%sl.Count].RLock()
}

func (sl *PagedRWLocks) RUnlock(id uint64) {
	sl.shards[(id/sl.PageSize)%sl.Count].RUnlock()
}

func (sl *PagedRWLocks) RLocked(id uint64, callback func()) {
	sl.RLock(id)
	defer sl.RUnlock(id)

	callback()
}
