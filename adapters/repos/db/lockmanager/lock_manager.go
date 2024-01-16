package lockmanager

import (
	"sync"
	"sync/atomic"
	"time"
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

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()

		for range t.C {
			l.Vaccum()
		}
	}()

	return &l
}

func (l *LockManager) groupForID(id uint64) *LockGroup {
	slot := id % l.size
	return &l.locks[slot]
}

func (l *LockManager) lockForID(group *LockGroup, id uint64) *atomic.Int64 {
	var lock *atomic.Int64
	v, ok := group.m.Load(id)
	if ok {
		return v.(*atomic.Int64)
	}

	lock = new(atomic.Int64)
	v, ok = group.m.LoadOrStore(id, lock)
	if ok {
		return v.(*atomic.Int64)
	}

	return lock
}

func (l *LockManager) Lock(id uint64) {
	group := l.groupForID(id)

	group.writeLock.RLock()

	lock := l.lockForID(group, id)

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
	group := l.groupForID(id)
	lock := l.lockForID(group, id)

	if !lock.CompareAndSwap(-1, 0) {
		panic("unlocking unlocked lock")
	}

	group.writeLock.RUnlock()

	group.cond.L.Lock()
	group.cond.Broadcast()
	group.cond.L.Unlock()
}

func (l *LockManager) RLock(id uint64) {
	group := l.groupForID(id)

	group.readLock.RLock()

	lock := l.lockForID(group, id)

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
	group := l.groupForID(id)

	lock := l.lockForID(group, id)

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

func (l *LockManager) Vaccum() {
	for i := 0; i < int(l.size); i++ {
		group := &l.locks[i]
		group.writeLock.Lock()
		group.readLock.Lock()

		group.m.Range(func(key, value interface{}) bool {
			if value.(*atomic.Int64).Load() == 0 {
				l.locks[i].m.Delete(key)
			}
			return true
		})

		group.readLock.Unlock()
		group.writeLock.Unlock()
	}
}
