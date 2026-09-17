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

package db

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
)

// A request's lazy load must hold the shard's create lock: a teardown slipping between lookup and pin rebuilds the shard on a wrapper already out of the map.
func TestGetOrInitShardPinsUnderCreateLock(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "PinUnderCreateLock", singleShardState())
	var name string
	var lazy *LazyLoadShard
	for n, l := range f.coldShards(t) {
		name, lazy = n, l
	}

	permit := loadlimiter.NewLoadLimiter(prometheus.NewRegistry(), "pin_under_create_lock", 1)
	require.NoError(t, permit.Acquire(ctx))
	permitHeld := true
	releasePermit := func() {
		if permitHeld {
			permitHeld = false
			permit.Release()
		}
	}
	defer releasePermit()
	lazy.shardLoadLimiter = permit

	loaded := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, release, err := f.index.getOrInitShard(ctx, name)
		release()
		loaded <- err
	}, f.index.logger)

	parked := 0
	require.Eventually(t, func() bool {
		if lazy.mutex.TryLock() {
			lazy.mutex.Unlock()
			parked = 0
			return false
		}
		parked++
		return parked >= 5
	}, 10*time.Second, 5*time.Millisecond)

	locked := make(chan struct{})
	enterrors.GoWrapper(func() {
		f.index.shardCreateLocks.Lock(name)
		f.index.shardCreateLocks.Unlock(name)
		close(locked)
	}, f.index.logger)
	lockAcquired := func() bool {
		select {
		case <-locked:
			return true
		default:
			return false
		}
	}
	require.Never(t, lockAcquired, 300*time.Millisecond, 20*time.Millisecond, "the create lock was free while a request's load was in flight")

	releasePermit()
	select {
	case err := <-loaded:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("load never finished")
	}
	require.Eventually(t, lockAcquired, 10*time.Second, 10*time.Millisecond)
}

func TestSchemaWalksPinUnderCreateLock(t *testing.T) {
	ctx := testCtx()
	walks := map[string]func(f *addPropertyLazyFixture) error{
		"add property":    func(f *addPropertyLazyFixture) error { return f.index.addProperty(ctx, textProp("late", true)) },
		"update property": func(f *addPropertyLazyFixture) error { return f.index.updateProperty(ctx, f.schemaClass.Properties[0]) },
	}
	for name, walk := range walks {
		t.Run(name, func(t *testing.T) {
			f := newAddPropertyLazyFixture(t, "WalkPinUnderCreateLock", singleShardState())
			var shardName string
			for n := range f.coldShards(t) {
				shardName = n
			}
			_, release, err := f.index.getOrInitShard(ctx, shardName)
			require.NoError(t, err)
			release()

			f.index.shardCreateLocks.Lock(shardName)
			lockHeld := true
			unlock := func() {
				if lockHeld {
					lockHeld = false
					f.index.shardCreateLocks.Unlock(shardName)
				}
			}
			defer unlock()

			var walkErr error
			finished := make(chan struct{})
			enterrors.GoWrapper(func() {
				walkErr = walk(f)
				close(finished)
			}, f.index.logger)
			walkFinished := func() bool {
				select {
				case <-finished:
					return true
				default:
					return false
				}
			}
			require.Never(t, walkFinished, 300*time.Millisecond, 20*time.Millisecond, "the walk pinned the shard while its create lock was held")

			unlock()
			select {
			case <-finished:
				require.NoError(t, walkErr)
			case <-time.After(30 * time.Second):
				t.Fatal("walk never finished")
			}
		})
	}
}
