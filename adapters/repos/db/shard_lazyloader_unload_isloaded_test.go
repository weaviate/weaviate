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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// unloadParkedInTeardown starts a TryUnload, parks it once the shard is marked shut and returns what releases it.
func unloadParkedInTeardown(t *testing.T, l *LazyLoadShard) (release func(), unloaded <-chan bool) {
	t.Helper()
	entered, unblock := make(chan struct{}), make(chan struct{})
	release = sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(release)
	l.unloadMarkedHook = func() {
		close(entered)
		<-unblock
	}
	done := make(chan bool, 1)
	enterrors.GoWrapper(func() { done <- l.TryUnload(testCtx(), UnloadRequest{}) }, l.shardOpts.index.logger)
	<-entered
	return release, done
}

func TestLazyLoadShard_IsLoadedWaitsForALoadInFlight(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	require.True(t, f.lazy.TryUnload(testCtx(), UnloadRequest{}))
	loading := make(chan struct{})
	loaded := make(chan struct{})
	hook := func(*LazyLoadShard) {
		close(loading)
		<-loaded
	}
	f.unloader.onBeforeLoad.Store(&hook)
	go func() {
		_, _, err := f.lazy.loadIfCold(testCtx())
		assert.NoError(t, err)
	}()
	<-loading
	result := make(chan bool, 1)
	go func() { result <- f.lazy.isLoaded() }()
	waiting := assert.Eventually(t, func() bool { return f.lazy.loadWaiters.Load() == 1 }, 5*time.Second, time.Millisecond)
	f.unloader.onBeforeLoad.Store(nil)
	close(loaded)
	require.True(t, waiting)
	require.True(t, <-result, "a load in flight is waited on")
	require.Zero(t, f.lazy.loadWaiters.Load())
}

func TestLazyLoadShard_ColdReadsDoNotWaitOnAnUnloadTeardown(t *testing.T) {
	tests := []struct {
		name  string
		check func(t *testing.T, f *unloadFixture)
		after func(t *testing.T, f *unloadFixture)
	}{
		{name: "addProperty", check: func(t *testing.T, f *unloadFixture) {
			prop := textProp("late", true)
			f.schemaClass.Properties = append(f.schemaClass.Properties, prop)
			require.NoError(t, f.index.addProperty(testCtx(), prop))
			require.False(t, f.lazy.isLoaded(), "a shard mid-teardown reads as unloaded")
		}, after: func(t *testing.T, f *unloadFixture) {
			shard, release, err := f.index.GetShard(testCtx(), f.name)
			require.NoError(t, err)
			defer release()
			require.NotNil(t, shard.Store().Bucket(helpers.BucketFromPropNameLSM("late")), "the reload builds the added property")
		}},
		{name: "status reads", check: func(t *testing.T, f *unloadFixture) {
			lazyLoading := storagestate.StatusLazyLoading.String()
			require.Equal(t, storagestate.StatusLazyLoading, f.lazy.GetStatus())
			require.Equal(t, lazyLoading, f.lazy.GetStatusReason())
			status, err := f.index.IncomingGetShardStatus(testCtx(), f.name)
			require.NoError(t, err)
			require.Equal(t, lazyLoading, status)
			size, err := f.index.IncomingGetShardQueueSize(testCtx(), f.name)
			require.NoError(t, err)
			require.Zero(t, size)
			statuses, legacy, err := f.index.getShardsStorageStatus(testCtx(), "")
			require.NoError(t, err)
			require.Equal(t, lazyLoading, statuses[f.name][f.index.Config.NodeName])
			require.Equal(t, lazyLoading, legacy[f.name])
			sizes, err := f.index.getShardsQueueSize(testCtx(), "")
			require.NoError(t, err)
			require.Contains(t, sizes, f.name)
			nodeStatus, count := f.index.shardNodeStatus(testCtx(), f.name, f.lazy, 1, 1)
			require.False(t, nodeStatus.Loaded)
			require.Equal(t, lazyLoading, nodeStatus.VectorIndexingStatus)
			require.Zero(t, count)
		}},
		{name: "create lock is free", check: func(t *testing.T, f *unloadFixture) {
			require.True(t, f.index.shardCreateLocks.TryLock(f.name))
			f.index.shardCreateLocks.Unlock(f.name)
		}},
		{name: "async replication walk", check: func(t *testing.T, f *unloadFixture) {
			require.NoError(t, f.index.applyAsyncReplicationToLoadedShards(testCtx()))
			require.NoError(t, f.lazy.enableAsyncReplication(testCtx(), f.index.AsyncReplicationConfig()))
			require.NoError(t, f.lazy.disableAsyncReplication(testCtx()))
			require.False(t, f.lazy.hasActiveAsyncReplicationTargetOverrides())
		}},
		{name: "hasGeoIndexForProp answers from the class", check: func(t *testing.T, f *unloadFixture) {
			f.schemaClass.Properties = append(f.schemaClass.Properties,
				&models.Property{Name: "where", DataType: schema.DataTypeGeoCoordinates.PropString()})
			require.True(t, f.lazy.hasGeoIndexForProp("where"))
			require.False(t, f.lazy.hasGeoIndexForProp("name"))
		}},
		{name: "warmup skips it", check: func(t *testing.T, f *unloadFixture) {
			warm, outcome := f.index.warmupCandidate(f.name)
			require.False(t, warm)
			require.Equal(t, monitoring.WarmupSkippedAlreadyLoaded, outcome)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			release, done := unloadParkedInTeardown(t, f.lazy)

			checked := make(chan struct{})
			go func() {
				defer close(checked)
				tt.check(t, f)
			}()
			select {
			case <-checked:
			case <-time.After(5 * time.Second):
				require.FailNow(t, "waited on an unload teardown")
			}
			release()
			require.True(t, <-done)
			if tt.after != nil {
				tt.after(t, f)
			}
		})
	}
}

func TestLazyLoadShard_ProbeShardDataWaitsOutAnUnloadTeardown(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	release, done := unloadParkedInTeardown(t, f.lazy)

	var hasData, hosted bool
	probed := make(chan error, 1)
	enterrors.GoWrapper(func() {
		var err error
		hasData, hosted, err = f.index.IncomingProbeShardData(testCtx(), f.name)
		probed <- err
	}, f.index.logger)
	select {
	case <-probed:
		require.FailNow(t, "the probe read the shard's files mid-teardown")
	case <-time.After(200 * time.Millisecond):
	}
	release()
	require.True(t, <-done)
	require.NoError(t, <-probed)
	require.True(t, hasData)
	require.True(t, hosted)
}

func TestLazyLoadShard_IndexCloseDoesNotWaitOnAnUnloadTeardownsRead(t *testing.T) {
	tests := []struct {
		name  string
		close func(f *unloadFixture) error
		// waitsOnShard: the close then shuts the shard itself, which waits on l.mutex for the teardown.
		waitsOnShard bool
	}{
		{name: "beginClose", close: func(f *unloadFixture) error { return f.index.beginClose() }},
		{name: "Index.Shutdown", close: func(f *unloadFixture) error { return f.index.Shutdown(testCtx()) }, waitsOnShard: true},
		{name: "drop", close: func(f *unloadFixture) error { return f.index.drop() }, waitsOnShard: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			f.put(t)
			release, done := unloadParkedInTeardown(t, f.lazy)

			closed := make(chan error, 1)
			enterrors.GoWrapper(func() { closed <- tt.close(f) }, f.index.logger)
			require.Eventually(t, func() bool {
				f.index.closeLock.RLock()
				defer f.index.closeLock.RUnlock()
				return f.index.closed
			}, 5*time.Second, time.Millisecond)
			timeout := 5 * time.Second
			if tt.waitsOnShard {
				timeout = 200 * time.Millisecond
			}
			select {
			case err := <-closed:
				require.False(t, tt.waitsOnShard, "the close returned before the shard's teardown finished")
				require.NoError(t, err)
			case <-time.After(timeout):
				require.True(t, tt.waitsOnShard, "the index close waited on the teardown's read")
			}
			release()
			require.True(t, <-done)
			if tt.waitsOnShard {
				require.NoError(t, <-closed)
			}
			require.Nil(t, f.lazy.currentShard(), "the close never reloads the unloaded shard")
		})
	}
}
