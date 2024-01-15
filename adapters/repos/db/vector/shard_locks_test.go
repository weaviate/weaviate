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
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
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

			lock, err := sl.LockAll(context.TODO())
			require.NoError(t, err)

			lock.Unlock()
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
			lock, err := sl.RLockAll(context.TODO())
			require.NoError(t, err)

			lock.Unlock()
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
				lock, err := sl.LockAll(context.TODO())
				require.NoError(t, err)

				lock.Unlock()
			} else {
				lock, err := sl.RLockAll(context.TODO())
				require.NoError(t, err)

				lock.Unlock()
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
					lock, err := sl.LockAll(context.TODO())
					require.NoError(t, err)

					lock.Unlock()
				} else {
					lock, err := sl.RLockAll(context.TODO())
					require.NoError(t, err)
					lock.Unlock()
				}
			} else {
				if i%2 == 0 {
					lock, err := sl.Lock(context.TODO(), id)
					require.NoError(t, err)
					lock.Unlock()
				} else {
					lock, err := sl.RLock(context.TODO(), id)
					require.NoError(t, err)
					lock.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
}
