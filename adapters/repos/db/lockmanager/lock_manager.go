package lockmanager

import (
	"sync"
	"sync/atomic"
)

// A LockManager is used to acquire locks on individual database objects.
// It provides granular locking per object and reduces contention
// by sharding the locks.
type LockManager struct {
	locks []LockGroup
	size  uint64
}

type LockGroup struct {
	m         sync.Map
	cond      sync.Cond
	readLock  sync.RWMutex
	writeLock sync.RWMutex
}

func New() *LockManager {
	return NewWith(1024)
}

func NewWith(size uint64) *LockManager {
	l := LockManager{
		locks: make([]LockGroup, size),
		size:  uint64(size),
	}

	for i := uint64(0); i < size; i++ {
		l.locks[i].cond.L = new(sync.Mutex)
	}

	return &l
}

func (l *LockManager) lockForID(id uint64) (*atomic.Int64, *LockGroup) {
	slot := id % l.size
	group := &l.locks[slot]

	var lock *atomic.Int64
	v, ok := group.m.Load(id)
	if ok {
		return v.(*atomic.Int64), group
	}

	lock = new(atomic.Int64)
	v, ok = group.m.LoadOrStore(id, lock)
	if ok {
		return v.(*atomic.Int64), group
	}

	return lock, group
}

func (l *LockManager) Lock(id uint64) {
	lock, group := l.lockForID(id)

	group.writeLock.RLock()

	for {
		if lock.CompareAndSwap(0, -1) {
			return
		}

		group.cond.L.Lock()
		for lock.Load() != 0 {
			group.cond.Wait()
		}
		group.cond.L.Unlock()
	}
}

func (l *LockManager) Unlock(id uint64) {
	lock, group := l.lockForID(id)

	if !lock.CompareAndSwap(-1, 0) {
		panic("unlocking unlocked lock")
	}

	group.writeLock.RUnlock()

	group.cond.L.Lock()
	group.cond.Broadcast()
	group.cond.L.Unlock()
}

func (l *LockManager) RLock(id uint64) {
	lock, group := l.lockForID(id)

	group.readLock.RLock()

	for {
		for v := lock.Load(); v >= 0; v = lock.Load() {
			if lock.CompareAndSwap(v, v+1) {
				return
			}
		}

		group.cond.L.Lock()
		for lock.Load() != 0 {
			group.cond.Wait()
		}
		group.cond.L.Unlock()
	}
}

func (l *LockManager) RUnlock(id uint64) {
	lock, group := l.lockForID(id)

	if res := lock.Add(-1); res < 0 {
		panic("unlocking unlocked rlock")
	}

	group.readLock.RUnlock()

	group.cond.L.Lock()
	group.cond.Broadcast()
	group.cond.L.Unlock()
}

func (l *LockManager) LockAll() {
	for i := 0; i < int(l.size); i++ {
		l.locks[i].writeLock.Lock()
		l.locks[i].readLock.Lock()
	}
}

func (l *LockManager) UnlockAll() {
	for i := int(l.size) - 1; i >= 0; i-- {
		l.locks[i].readLock.Unlock()
		l.locks[i].writeLock.Unlock()
	}
}

func (l *LockManager) RLockAll() {
	for i := uint64(0); i < l.size; i++ {
		l.locks[i].writeLock.Lock()
	}
}

func (l *LockManager) RUnlockAll() {
	for i := int(l.size) - 1; i >= 0; i-- {
		l.locks[i].writeLock.Unlock()
	}
}
