package vector

import (
	"github.com/weaviate/weaviate/adapters/repos/db/lockmanager"
)

type ShardLocks struct {
	locks *lockmanager.LockManager
}

func NewShardLocks() *ShardLocks {
	return &ShardLocks{
		locks: lockmanager.New(1000),
	}
}

func (sl *ShardLocks) LockAll() {
	sl.locks.LockAll()
	// lock := ShardLock{
	// 	id:    sl.newID(),
	// 	locks: sl.locks,
	// }

	// // acquire an exclusive lock on all shards
	// err := sl.locks.Lock(ctx, lock.id, &allShardsObject, lockmanager.X)
	// if err != nil {
	// 	return nil, err
	// }

	// return &lock, nil
}

func (sl *ShardLocks) UnlockAll() {
	sl.locks.UnlockAll()
}

func (sl *ShardLocks) Lock(id uint64) {
	sl.locks.Lock(id)
}

func (sl *ShardLocks) Unlock(id uint64) {
	sl.locks.Unlock(id)
}

func (sl *ShardLocks) RLockAll() {
	sl.locks.RLockAll()
	// lock := ShardLock{
	// 	id:    sl.newID(),
	// 	locks: sl.locks,
	// }

	// // acquire a shared lock on all shards
	// err := sl.locks.Lock(ctx, lock.id, &allShardsObject, lockmanager.S)
	// if err != nil {
	// 	return nil, err
	// }

	// return &lock, nil
}

func (sl *ShardLocks) RUnlockAll() {
	sl.locks.RUnlockAll()
}

func (sl *ShardLocks) RLock(id uint64) {
	sl.locks.RLock(id)
}

func (sl *ShardLocks) RUnlock(id uint64) {
	sl.locks.RUnlock(id)
}
