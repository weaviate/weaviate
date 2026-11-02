//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// bucketMoves moves bucket "moved" to the name "target".
var bucketMoves = []struct {
	name         string
	targetExists bool
	move         func(ctx context.Context, s *Store) error
}{
	{
		name: "RenameBucket",
		move: func(ctx context.Context, s *Store) error {
			s.Bucket("moved").UpdateStatus(storagestate.StatusReadOnly)
			return s.RenameBucket(ctx, "moved", "target")
		},
	},
	{
		name:         "ReplaceBuckets",
		targetExists: true,
		move: func(ctx context.Context, s *Store) error {
			return s.ReplaceBuckets(ctx, "target", "moved")
		},
	},
}

func newStoreForMove(t *testing.T, targetExists bool) *Store {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", WithStrategy(StrategyReplace)))
	if targetExists {
		require.NoError(t, store.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))
	}
	return store
}

// leaveFailedFlush leaves key in b.flushing, as a flush that failed to write
// its segment does.
func leaveFailedFlush(t *testing.T, b *Bucket, key string) (restore func()) {
	require.NoError(t, b.Put([]byte(key), []byte(key)))
	require.NoError(t, os.Chmod(b.GetDir(), 0o555))
	restore = func() { require.NoError(t, os.Chmod(b.GetDir(), 0o755)) }
	t.Cleanup(restore)
	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)
	return restore
}

func reopenStore(t *testing.T, store *Store) *Store {
	ctx := context.Background()
	require.NoError(t, store.Shutdown(ctx))
	logger, _ := test.NewNullLogger()
	reopened, err := New(store.dir, store.dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Shutdown(ctx) })
	require.NoError(t, reopened.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))
	return reopened
}

func requireKeys(t *testing.T, b *Bucket, keys ...string) {
	for _, key := range keys {
		val, err := b.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, []byte(key), val, "key %q", key)
	}
}

func TestStore_MoveKeepsUnflushedWrites(t *testing.T) {
	for _, mv := range bucketMoves {
		for _, failedFlush := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failedFlush=%v", mv.name, failedFlush), func(t *testing.T) {
				ctx := context.Background()
				store := newStoreForMove(t, mv.targetExists)
				keys := []string{"active"}
				if failedFlush {
					leaveFailedFlush(t, store.Bucket("moved"), "flushing")()
					keys = append(keys, "flushing")
				}
				require.NoError(t, store.Bucket("moved").Put([]byte("active"), []byte("active")))

				require.NoError(t, mv.move(ctx, store))
				requireKeys(t, store.Bucket("target"), keys...)
				requireKeys(t, reopenStore(t, store).Bucket("target"), keys...)
			})
		}
	}
}

func TestStore_MoveFailingToFlushLeavesBucketUnchanged(t *testing.T) {
	for _, mv := range bucketMoves {
		t.Run(mv.name, func(t *testing.T) {
			ctx := context.Background()
			store := newStoreForMove(t, mv.targetExists)
			t.Cleanup(func() { _ = store.Shutdown(ctx) })
			movedDir := store.Bucket("moved").GetDir()
			leaveFailedFlush(t, store.Bucket("moved"), "flushing")

			require.ErrorContains(t, mv.move(ctx, store), "flush")

			moved := store.Bucket("moved")
			require.NotNil(t, moved)
			require.Equal(t, movedDir, moved.GetDir())
			requireKeys(t, moved, "flushing")
			require.Equal(t, mv.targetExists, store.Bucket("target") != nil)
		})
	}
}

func TestStore_ReplaceBucketsKeepsConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	store := newStoreForMove(t, true)
	moved := store.Bucket("moved")

	var (
		stop  atomic.Bool
		acked atomic.Int64
		wg    sync.WaitGroup
		mu    sync.Mutex
		keys  []string
	)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				key := fmt.Sprintf("w%d-%d", w, i)
				if !assert.NoError(t, moved.Put([]byte(key), []byte(key))) {
					return
				}
				mu.Lock()
				keys = append(keys, key)
				mu.Unlock()
				acked.Add(1)
			}
		}()
	}
	waitForAcks := func(n int64) {
		for acked.Load() < n {
			runtime.Gosched()
		}
	}
	waitForAcks(1000)
	require.NoError(t, store.ReplaceBuckets(ctx, "target", "moved"))
	waitForAcks(acked.Load() + 1000)
	stop.Store(true)
	wg.Wait()

	requireKeys(t, store.Bucket("target"), keys...)
	requireKeys(t, reopenStore(t, store).Bucket("target"), keys...)
}

// A writer that took the active memtable before the move but inserts only
// after the move looked at it must not write into a memtable the move drops.
func TestStore_MoveWaitsForPinnedWriter(t *testing.T) {
	for _, mv := range bucketMoves {
		t.Run(mv.name, func(t *testing.T) {
			ctx := context.Background()
			store := newStoreForMove(t, mv.targetExists)
			active, err := store.Bucket("moved").getActiveMemtableForWrite()
			require.NoError(t, err)

			moveErr := make(chan error, 1)
			go func() { moveErr <- mv.move(ctx, store) }()
			select {
			case err := <-moveErr:
				moveErr <- err
			case <-time.After(500 * time.Millisecond):
			}
			require.NoError(t, active.put([]byte("pinned"), []byte("pinned")))
			active.decWriterCount()
			require.NoError(t, <-moveErr)

			requireKeys(t, reopenStore(t, store).Bucket("target"), "pinned")
		})
	}
}

// A flush can call back into the store (a skip-key function looking up
// another bucket), so a move must not flush while holding the store's lock.
func TestStore_MoveFlushesBucketWhoseSkipKeyLooksUpBuckets(t *testing.T) {
	for _, mv := range bucketMoves {
		t.Run(mv.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStoreForDrain(t)
			require.NoError(t, store.CreateOrLoadBucket(ctx, "other", WithStrategy(StrategyReplace)))
			opts := []BucketOption{
				WithStrategy(StrategySetCollection),
				WithShouldSkipKeyFunction(func(key []byte, ctx context.Context) (bool, error) {
					_, release := store.AcquireBucketForRead("other")
					release()
					return false, nil
				}),
			}
			require.NoError(t, store.CreateOrLoadBucket(ctx, "moved", opts...))
			if mv.targetExists {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "target", opts...))
			}
			require.NoError(t, store.Bucket("moved").SetAdd([]byte("key"), [][]byte{[]byte("value")}))

			moveErr := make(chan error, 1)
			go func() { moveErr <- mv.move(ctx, store) }()
			select {
			case err := <-moveErr:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				// the store stays locked, so it is not shut down
				t.Fatal("move deadlocked")
			}
			t.Cleanup(func() { _ = store.Shutdown(ctx) })

			values, err := store.Bucket("target").SetList([]byte("key"))
			require.NoError(t, err)
			require.Equal(t, [][]byte{[]byte("value")}, values)
		})
	}
}
