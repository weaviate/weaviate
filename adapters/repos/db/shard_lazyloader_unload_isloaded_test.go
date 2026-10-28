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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// blockTeardown parks the next TryUnload once the shard is marked shut and returns what releases it.
func blockTeardown(t *testing.T, l *LazyLoadShard) (entered <-chan struct{}, release func()) {
	t.Helper()
	in, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	l.unloadMarkedHook = func() {
		close(in)
		<-unblock
	}
	return in, release
}

func TestLazyLoadShard_AddPropertyDoesNotWaitOnAnUnloadTeardown(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	entered, release := blockTeardown(t, f.lazy)
	done := make(chan UnloadOutcome, 1)
	go func() {
		done <- f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex})
	}()
	<-entered

	prop := textProp("late", true)
	f.schemaClass.Properties = append(f.schemaClass.Properties, prop)
	added := make(chan error, 1)
	go func() { added <- f.index.addProperty(testCtx(), prop) }()
	select {
	case err := <-added:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		release()
		require.FailNow(t, "addProperty waited on an unload teardown")
	}
	require.False(t, f.lazy.isLoaded(), "a shard mid-teardown reads as unloaded")

	release()
	require.Equal(t, UnloadOutcomeOK, <-done)
	shard, rel, err := f.index.GetShard(testCtx(), f.name)
	defer rel()
	require.NoError(t, err)
	require.NotNil(t, shard.Store().Bucket(helpers.BucketFromPropNameLSM("late")), "the reload builds the added property")
}

func TestLazyLoadShard_IsLoadedWaitsForALoadButRefusesUnloadsMeanwhile(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex}))
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

	f.lazy.loadWaiters.Add(1)
	require.Equal(t, UnloadOutcomeLockBusy, f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex}),
		"an unload never starts a teardown a waiting isLoaded would then sit through")
	f.lazy.loadWaiters.Add(-1)
	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex}))
}

func TestLazyLoadShard_IsLoadedRacesUnloadsAndReloads(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					f.lazy.isLoaded()
				}
			}
		}()
	}
	unloads := 0
	outcomes := map[UnloadOutcome]int{}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		o := f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex})
		outcomes[o]++
		if o == UnloadOutcomeOK {
			unloads++
		}
		shard, release, err := f.index.GetShard(testCtx(), f.name)
		require.NoError(t, err)
		require.NotNil(t, shard)
		release()
	}
	close(stop)
	wg.Wait()
	require.Positive(t, unloads, "%v", outcomes)
	require.True(t, f.lazy.isLoaded())
	require.EqualValues(t, 1, f.unloader.registry.Loaded())
}

// TryUnload holds shardCreateLocks only across try-locks and atomics; the
// teardown runs after it, so no request path waits on the create lock behind it.
func TestLazyLoadShard_TryUnloadNeverHoldsTheCreateLockAcrossItsTeardown(t *testing.T) {
	u := newFakeShardUnloader()
	f := newAddPropertyLazyFixture(t, "UnloadLocks", multiShardState(), withShardUnloader(u))
	var victim, other string
	var lazy *LazyLoadShard
	for name, l := range f.coldShards(t) {
		if lazy == nil {
			victim, lazy = name, l
		} else {
			other = name
		}
	}
	for _, name := range []string{victim, other} {
		shard, release, err := f.index.GetShard(testCtx(), name)
		require.NoError(t, err)
		require.NotNil(t, shard)
		release()
	}
	entered, release := blockTeardown(t, lazy)
	done := make(chan UnloadOutcome, 1)
	go func() { done <- lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex}) }()
	<-entered

	require.True(t, f.index.shardCreateLocks.TryLock(victim), "the victim's create lock is free during its teardown")
	f.index.shardCreateLocks.Unlock(victim)
	pinned := make(chan error, 1)
	go func() {
		_, rel, err := f.index.pinResidentShard(other, true)
		rel()
		pinned <- err
	}()
	select {
	case err := <-pinned:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		release()
		require.FailNow(t, "a pin on another shard waited on an unload teardown")
	}
	require.False(t, lazy.isLoaded())

	release()
	require.Equal(t, UnloadOutcomeOK, <-done)
}

func TestLazyLoadShard_ColdReadsDoNotWaitOnAnUnloadTeardown(t *testing.T) {
	tests := []struct {
		name  string
		check func(t *testing.T, f *unloadFixture)
	}{
		{name: "GetStatus", check: func(t *testing.T, f *unloadFixture) {
			require.Equal(t, storagestate.StatusLazyLoading, f.lazy.GetStatus())
		}},
		{name: "GetStatusReason", check: func(t *testing.T, f *unloadFixture) {
			require.Equal(t, storagestate.StatusLazyLoading.String(), f.lazy.GetStatusReason())
		}},
		{name: "IncomingGetShardStatus", check: func(t *testing.T, f *unloadFixture) {
			status, err := f.index.IncomingGetShardStatus(testCtx(), f.name)
			require.NoError(t, err)
			require.Equal(t, storagestate.StatusLazyLoading.String(), status)
		}},
		{name: "nodes status", check: func(t *testing.T, f *unloadFixture) {
			status, count := f.index.shardNodeStatus(testCtx(), f.name, f.lazy, 1, 1)
			require.False(t, status.Loaded)
			require.Equal(t, storagestate.StatusLazyLoading.String(), status.VectorIndexingStatus)
			require.Zero(t, count)
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
			entered, release := blockTeardown(t, f.lazy)
			done := make(chan UnloadOutcome, 1)
			go func() {
				done <- f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex})
			}()
			<-entered

			checked := make(chan struct{})
			go func() {
				defer close(checked)
				tt.check(t, f)
			}()
			select {
			case <-checked:
			case <-time.After(5 * time.Second):
				release()
				require.FailNow(t, "waited on an unload teardown")
			}
			release()
			require.Equal(t, UnloadOutcomeOK, <-done)
		})
	}
}

func TestLazyLoadShard_ProbeShardDataWaitsOutAnUnloadTeardown(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)
	entered, release := blockTeardown(t, f.lazy)
	done := make(chan UnloadOutcome, 1)
	go func() {
		done <- f.lazy.TryUnload(testCtx(), UnloadRequest{Mode: UnloadModeRequest, Reindex: noReindex})
	}()
	<-entered

	type probe struct {
		hasData, hosted bool
		err             error
	}
	probed := make(chan probe, 1)
	go func() {
		hasData, hosted, err := f.index.IncomingProbeShardData(testCtx(), f.name)
		probed <- probe{hasData, hosted, err}
	}()
	select {
	case <-probed:
		release()
		require.FailNow(t, "the probe read the shard's files mid-teardown")
	case <-time.After(200 * time.Millisecond):
	}
	release()
	require.Equal(t, UnloadOutcomeOK, <-done)
	got := <-probed
	require.NoError(t, got.err)
	require.True(t, got.hasData)
	require.True(t, got.hosted)
}
