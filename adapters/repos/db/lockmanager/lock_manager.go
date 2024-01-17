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

package lockmanager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
)

var pool = sync.Pool{
	New: func() any {
		return new(atomic.Int32)
	},
}

type dummyLock struct{}

func (d *dummyLock) Lock()   {}
func (d *dummyLock) Unlock() {}

// A LockManager is used to acquire locks on individual database objects.
// It provides granular locking per object and reduces contention
// by sharding the locks.
type LockManager struct {
	shards []Shard
	size   uint64

	vacuum      bool
	shutdownCtx context.Context
	shutdown    context.CancelFunc
	closed      chan struct{}
}

type Shard struct {
	m         *xsync.MapOf[uint64, *atomic.Int32]
	cond      sync.Cond
	readLock  sync.RWMutex
	writeLock sync.RWMutex
}

func New() *LockManager {
	return NewWith(512, false)
}

func NewWith(size uint64, vacuum bool) *LockManager {
	l := LockManager{
		shards: make([]Shard, size),
		size:   uint64(size),
		vacuum: vacuum,
	}

	var dl dummyLock
	for i := uint64(0); i < size; i++ {
		l.shards[i].cond.L = &dl
		l.shards[i].m = xsync.NewMapOf[uint64, *atomic.Int32]()
	}

	if vacuum {
		l.closed = make(chan struct{})

		l.shutdownCtx, l.shutdown = context.WithCancel(context.Background())

		go func() {
			defer close(l.closed)

			t := time.NewTicker(30 * time.Second)
			defer t.Stop()

			for {
				select {
				case <-l.shutdownCtx.Done():
					return
				case <-t.C:
					l.Vacuum()
				}
			}
		}()
	}

	return &l
}

func (l *LockManager) Shutdown(ctx context.Context) error {
	if !l.vacuum {
		return nil
	}

	l.shutdown()

	select {
	case <-l.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *LockManager) groupForID(id uint64) *Shard {
	slot := id % l.size
	return &l.shards[slot]
}

func (l *LockManager) lockForID(group *Shard, id uint64, create bool) *atomic.Int32 {
	lock, ok := group.m.Load(id)
	if ok {
		return lock
	}
	if !create {
		return nil
	}

	lock = pool.Get().(*atomic.Int32)
	ll, ok := group.m.LoadOrStore(id, lock)
	if ok {
		pool.Put(lock)
		return ll
	}

	return lock
}

func (l *LockManager) Lock(id uint64) {
	group := l.groupForID(id)

	group.writeLock.RLock()

	lock := l.lockForID(group, id, true)

	for {
		if lock.CompareAndSwap(0, -1) {
			return
		}

		group.cond.Wait()
	}
}

func (l *LockManager) Unlock(id uint64) {
	group := l.groupForID(id)
	lock := l.lockForID(group, id, false)
	if lock == nil {
		panic("unlocking unlocked lock")
	}

	if !lock.CompareAndSwap(-1, 0) {
		panic("unlocking unlocked lock")
	}

	group.writeLock.RUnlock()

	group.cond.Broadcast()
}

func (l *LockManager) RLock(id uint64) {
	group := l.groupForID(id)

	group.readLock.RLock()

	lock := l.lockForID(group, id, true)

	for {
		v := lock.Load()
		for v >= 0 {
			if lock.CompareAndSwap(v, v+1) {
				return
			}

			v = lock.Load()
		}

		group.cond.Wait()
	}
}

func (l *LockManager) RUnlock(id uint64) {
	group := l.groupForID(id)

	lock := l.lockForID(group, id, false)
	if lock == nil {
		panic("unlocking unlocked rlock")
	}

	if res := lock.Add(-1); res < 0 {
		panic("unlocking unlocked rlock")
	}

	group.readLock.RUnlock()

	group.cond.Broadcast()
}

func (l *LockManager) LockAll() {
	for i := 0; i < int(l.size); i++ {
		l.shards[i].writeLock.Lock()
		l.shards[i].readLock.Lock()
	}
}

func (l *LockManager) UnlockAll() {
	for i := int(l.size) - 1; i >= 0; i-- {
		l.shards[i].readLock.Unlock()
		l.shards[i].writeLock.Unlock()
	}
}

func (l *LockManager) RLockAll() {
	for i := uint64(0); i < l.size; i++ {
		l.shards[i].writeLock.Lock()
	}
}

func (l *LockManager) RUnlockAll() {
	for i := int(l.size) - 1; i >= 0; i-- {
		l.shards[i].writeLock.Unlock()
	}
}

func (l *LockManager) Vacuum() {
	var keys []uint64
	var locks []*atomic.Int32
	for i := 0; i < int(l.size); i++ {
		shard := &l.shards[i]
		shard.writeLock.Lock()
		shard.readLock.Lock()

		keys = keys[:0]
		locks = locks[:0]

		shard.m.Range(func(key uint64, lock *atomic.Int32) bool {
			if lock.Load() == 0 {
				keys = append(keys, key)
				locks = append(locks, lock)
			}
			return true
		})

		if len(keys) > 0 {
			for _, key := range keys {
				shard.m.Delete(key)
			}
			for _, lock := range locks {
				lock.Store(0)
				pool.Put(lock)
			}
		}

		shard.readLock.Unlock()
		shard.writeLock.Unlock()
	}
}
