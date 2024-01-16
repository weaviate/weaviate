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

package vector

import (
	"sync"
	"testing"
)

func TestShardLocks_ParallelLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll does not fall into deadlock
	count := 10
	sl := NewShardLocks()

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()

			sl.LockAll()
			sl.UnlockAll()
		}()
	}
	wg.Wait()
}

func TestShardLocks_ParallelRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel RLockAll does not fall into deadlock
	count := 10
	sl := NewShardLocks()

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			sl.RLockAll()
			sl.RUnlockAll()
		}()
	}
	wg.Wait()
}

func TestShardLocks_ParallelLocksAllAndRLocksAll(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll does not fall into deadlock
	count := 50
	sl := NewShardLocks()

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				sl.LockAll()
				sl.UnlockAll()
			} else {
				sl.RLockAll()
				sl.RUnlockAll()
			}
		}(i)
	}
	wg.Wait()
}

func TestShardLocks_MixedLocks(t *testing.T) {
	// no asserts
	// ensures parallel LockAll + RLockAll + Lock + RLock does not fall into deadlock
	count := 1000
	sl := NewShardLocks()

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			id := uint64(i)
			if i%5 == 0 {
				if i%2 == 0 {
					sl.LockAll()
					sl.UnlockAll()
				} else {
					sl.RLockAll()
					sl.RUnlockAll()
				}
			} else {
				if i%2 == 0 {
					sl.Lock(id)
					sl.Unlock(id)
				} else {
					sl.RLock(id)
					sl.RUnlock(id)
				}
			}
		}(i)
	}
	wg.Wait()
}
