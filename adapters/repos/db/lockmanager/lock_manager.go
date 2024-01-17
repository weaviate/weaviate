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

// A LockManager is used to acquire locks on individual database objects.
// It provides granular locking per object and reduces contention
// by sharding the locks.
type LockManager struct {
	locks []LockGroup
	size  uint64

	vaccum      bool
	shutdownCtx context.Context
	shutdown    context.CancelFunc
	closed      chan struct{}
}

type LockGroup struct {
	m         *xsync.MapOf[uint64, *atomic.Int32]
	cond      sync.Cond
	readLock  sync.RWMutex
	writeLock sync.RWMutex
}

func New() *LockManager {
	return NewWith(1024, false)
}

func NewWith(size uint64, vaccum bool) *LockManager {
	l := LockManager{
		locks:  make([]LockGroup, size),
		size:   uint64(size),
		vaccum: vaccum,
	}

	for i := uint64(0); i < size; i++ {
		l.locks[i].cond.L = new(sync.Mutex)
		l.locks[i].m = xsync.NewMapOf[uint64, *atomic.Int32]()
	}

	if vaccum {
		l.closed = make(chan struct{})

		l.shutdownCtx, l.shutdown = context.WithCancel(context.Background())

		go func() {
			defer close(l.closed)

			t := time.NewTicker(10 * time.Second)
			defer t.Stop()

			for {
				select {
				case <-l.shutdownCtx.Done():
					return
				case <-t.C:
					l.Vaccum()
				}
			}
		}()
	}

	return &l
}

func (l *LockManager) Shutdown(ctx context.Context) error {
	if !l.vaccum {
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

func (l *LockManager) groupForID(id uint64) *LockGroup {
	slot := id % l.size
	return &l.locks[slot]
}

func (l *LockManager) lockForID(group *LockGroup, id uint64, create bool) *atomic.Int32 {
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

		group.cond.L.Lock()
		for lock.Load() != 0 {
			group.cond.Wait()
		}
		group.cond.L.Unlock()

		lock = l.lockForID(group, id, true)
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

	group.cond.L.Lock()
	group.cond.Broadcast()
	group.cond.L.Unlock()
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

		group.cond.L.Lock()
		for lock.Load() < 0 {
			group.cond.Wait()
		}
		group.cond.L.Unlock()

		lock = l.lockForID(group, id, true)
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
	var keys []uint64
	var locks []*atomic.Int32
	for i := 0; i < int(l.size); i++ {
		group := &l.locks[i]
		group.writeLock.Lock()
		group.readLock.Lock()

		keys = keys[:0]
		locks = locks[:0]

		group.m.Range(func(key uint64, lock *atomic.Int32) bool {
			if lock.Load() == 0 {
				keys = append(keys, key)
				locks = append(locks, lock)
			}
			return true
		})

		if len(keys) > 0 {
			for _, key := range keys {
				group.m.Delete(key)
			}
			for _, lock := range locks {
				lock.Store(0)
				pool.Put(lock)
			}
		}

		group.readLock.Unlock()
		group.writeLock.Unlock()
	}
}
