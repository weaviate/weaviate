package vector

import (
	"context"
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/lockmanager"
)

const (
	lockAllShards = iota
	lockSingleShard
)

var (
	allShardsObject = lockmanager.Object{Level: lockAllShards}
)

type ShardLock struct {
	id     uint64
	locks  *lockmanager.LockManager
	single *lockmanager.Object
}

type ShardLocks struct {
	locks *lockmanager.LockManager
	ids   atomic.Uint64
}

func (sl *ShardLock) Unlock() {
	if sl.single != nil {
		sl.locks.Unlock(sl.id, sl.single)
	}

	sl.locks.Unlock(sl.id, &allShardsObject)
}

func NewShardLocks() *ShardLocks {
	return &ShardLocks{
		locks: lockmanager.New(),
	}
}

func (sl *ShardLocks) newID() uint64 {
	return sl.ids.Add(1)
}

func (sl *ShardLocks) LockAll(ctx context.Context) (*ShardLock, error) {
	lock := ShardLock{
		id:    sl.newID(),
		locks: sl.locks,
	}

	// acquire an exclusive lock on all shards
	err := sl.locks.Lock(ctx, lock.id, &allShardsObject, lockmanager.X)
	if err != nil {
		return nil, err
	}

	return &lock, nil
}

func (sl *ShardLocks) Lock(ctx context.Context, id uint64) (*ShardLock, error) {
	lock := ShardLock{
		id:     sl.newID(),
		locks:  sl.locks,
		single: &lockmanager.Object{Level: lockSingleShard, ID: id},
	}

	// acquire an intent exclusive lock on all shards
	err := sl.locks.Lock(ctx, lock.id, &allShardsObject, lockmanager.IX)
	if err != nil {
		return nil, err
	}

	// acquire an exclusive lock on the shard
	err = sl.locks.Lock(ctx, lock.id, lock.single, lockmanager.X)
	if err != nil {
		sl.locks.Unlock(lock.id, &allShardsObject)
		return nil, err
	}

	return &lock, nil
}

func (sl *ShardLocks) RLockAll(ctx context.Context) (*ShardLock, error) {
	lock := ShardLock{
		id:    sl.newID(),
		locks: sl.locks,
	}

	// acquire a shared lock on all shards
	err := sl.locks.Lock(ctx, lock.id, &allShardsObject, lockmanager.S)
	if err != nil {
		return nil, err
	}

	return &lock, nil
}

func (sl *ShardLocks) RLock(ctx context.Context, id uint64) (*ShardLock, error) {
	lock := ShardLock{
		id:     sl.newID(),
		locks:  sl.locks,
		single: &lockmanager.Object{Level: lockSingleShard, ID: id},
	}

	// acquire an intent shared lock on all shards
	err := sl.locks.Lock(ctx, lock.id, &allShardsObject, lockmanager.IS)
	if err != nil {
		return nil, err
	}

	// acquire a shared lock on the shard
	err = sl.locks.Lock(ctx, lock.id, lock.single, lockmanager.S)
	if err != nil {
		sl.locks.Unlock(lock.id, &allShardsObject)
		return nil, err
	}

	return &lock, nil
}
