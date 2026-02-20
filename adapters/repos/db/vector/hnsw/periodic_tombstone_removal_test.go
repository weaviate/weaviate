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

package hnsw

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	testhelper "github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type periodicNoopBucketView struct{}

func (n *periodicNoopBucketView) ReleaseView() {}

// toggleAllocChecker is a thread-safe AllocChecker that can be toggled to
// simulate memory pressure during cleanup.
type toggleAllocChecker struct {
	shouldErr atomic.Bool
}

func (t *toggleAllocChecker) CheckAlloc(sizeInBytes int64) error {
	if t.shouldErr.Load() {
		return fmt.Errorf("insufficient memory: need %d bytes", sizeInBytes)
	}
	return nil
}

func (t *toggleAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}

func (t *toggleAllocChecker) Refresh(updateMappings bool) {}

func TestPeriodicTombstoneRemoval(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	cleanupIntervalSeconds := 1
	tombstoneCallbacks := cyclemanager.NewCallbackGroup("tombstone", logger, 1)
	tombstoneCleanupCycle := cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(time.Duration(cleanupIntervalSeconds)*time.Second),
		tombstoneCallbacks.CycleCallback, logger)
	tombstoneCleanupCycle.Start()

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "automatic-tombstone-removal",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &periodicNoopBucketView{} },
	}, ent.UserConfig{
		CleanupIntervalSeconds: cleanupIntervalSeconds,
		MaxConnections:         30,
		EFConstruction:         128,
	}, tombstoneCallbacks, testinghelpers.NewDummyStore(t))
	index.PostStartup(context.Background())

	require.Nil(t, err)

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}

	t.Run("delete an entry and verify there is a tombstone", func(t *testing.T) {
		for i := range testVectors {
			if i%2 != 0 {
				continue
			}

			err := index.Delete(uint64(i))
			require.Nil(t, err)
		}
	})

	t.Run("verify there are now tombstones", func(t *testing.T) {
		index.tombstoneLock.RLock()
		ts := len(index.tombstones)
		index.tombstoneLock.RUnlock()
		assert.True(t, ts > 0)
	})

	t.Run("wait for tombstones to disappear", func(t *testing.T) {
		testhelper.AssertEventuallyEqual(t, true, func() interface{} {
			index.tombstoneLock.RLock()
			ts := len(index.tombstones)
			index.tombstoneLock.RUnlock()
			return ts == 0
		}, "wait until tombstones have been cleaned up")
	})

	if err := index.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tombstoneCleanupCycle.StopAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTombstoneCleanupAbortsOnMemoryPressure_PreCheck(t *testing.T) {
	ctx := context.Background()
	logger, logHook := test.NewNullLogger()
	cleanupIntervalSeconds := 1
	tombstoneCallbacks := cyclemanager.NewCallbackGroup("tombstone", logger, 1)
	tombstoneCleanupCycle := cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(time.Duration(cleanupIntervalSeconds)*time.Second),
		tombstoneCallbacks.CycleCallback, logger)

	allocChecker := &toggleAllocChecker{}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "tombstone-oom-precheck",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		AllocChecker:          allocChecker,
		Logger:                logger,
		GetViewThunk:          func() common.BucketView { return &periodicNoopBucketView{} },
	}, ent.UserConfig{
		CleanupIntervalSeconds: cleanupIntervalSeconds,
		MaxConnections:         30,
		EFConstruction:         128,
	}, tombstoneCallbacks, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, index.Shutdown(context.Background()))
	})
	index.PostStartup(context.Background())

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.NoError(t, err)
	}

	// Delete some entries to create tombstones
	for i := range testVectors {
		if i%2 != 0 {
			continue
		}
		err := index.Delete(uint64(i))
		require.NoError(t, err)
	}

	// Verify tombstones exist
	index.tombstoneLock.RLock()
	ts := len(index.tombstones)
	index.tombstoneLock.RUnlock()
	require.True(t, ts > 0, "expected tombstones to exist")

	// Simulate memory pressure before starting cleanup cycle
	allocChecker.shouldErr.Store(true)

	// Start the cleanup cycle — it should be skipped due to memory pressure
	// at the pre-check in tombstoneCleanup (startup.go).
	tombstoneCleanupCycle.Start()
	t.Cleanup(func() {
		require.NoError(t, tombstoneCleanupCycle.StopAndWait(context.Background()))
	})

	// Wait for the OOM skip log entry to confirm a cleanup cycle was attempted
	// and rejected due to memory pressure (the pre-check in tombstoneCleanup).
	hasOOMLogEntry := func() bool {
		for _, entry := range logHook.AllEntries() {
			if event, ok := entry.Data["event"]; ok && event == "cleanup_skipped_oom" {
				return true
			}
		}
		return false
	}
	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		return hasOOMLogEntry()
	}, "expected cleanup_skipped_oom log entry")

	// Tombstones should still be present because cleanup was skipped
	index.tombstoneLock.RLock()
	tsAfter := len(index.tombstones)
	index.tombstoneLock.RUnlock()
	assert.Equal(t, ts, tsAfter, "tombstones should remain when cleanup is skipped due to memory pressure")

	// Now release memory pressure and verify cleanup proceeds
	allocChecker.shouldErr.Store(false)

	testhelper.AssertEventuallyEqual(t, true, func() interface{} {
		index.tombstoneLock.RLock()
		remaining := len(index.tombstones)
		index.tombstoneLock.RUnlock()
		return remaining == 0
	}, "tombstones should be cleaned up after memory pressure subsides")
}

func TestTombstoneCleanupAbortsOnMemoryPressure_MidCleanup(t *testing.T) {
	ctx := context.Background()
	logger, logHook := test.NewNullLogger()

	tombstoneCallbacks := cyclemanager.NewCallbackGroup("tombstone", logger, 1)

	// Use DummyMonitor during setup so Add() calls pass without issues.
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "tombstone-oom-midcleanup",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		GetViewThunk:          func() common.BucketView { return &periodicNoopBucketView{} },
	}, ent.UserConfig{
		CleanupIntervalSeconds: 1,
		MaxConnections:         30,
		EFConstruction:         128,
	}, tombstoneCallbacks, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, index.Shutdown(context.Background()))
	})
	index.PostStartup(context.Background())

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.NoError(t, err)
	}

	// Delete some entries to create tombstones
	for i := range testVectors {
		if i%2 != 0 {
			continue
		}
		err := index.Delete(uint64(i))
		require.NoError(t, err)
	}

	index.tombstoneLock.RLock()
	ts := len(index.tombstones)
	index.tombstoneLock.RUnlock()
	require.True(t, ts > 0, "expected tombstones to exist")

	// Swap in a failing allocChecker and set a very short monitor interval
	// so the background goroutine fires immediately. We call
	// cleanUpTombstonedNodes directly to bypass the pre-check and exercise
	// the background memory monitor path.
	allocChecker := &toggleAllocChecker{}
	allocChecker.shouldErr.Store(true)
	index.allocChecker = allocChecker
	index.tombstoneMemCheckInterval = 1 * time.Millisecond

	executed, err := index.cleanUpTombstonedNodes(func() bool { return false })
	require.NoError(t, err)
	assert.True(t, executed, "cleanup should have started before being aborted")

	// Verify the background monitor logged the abort
	hasAbortLogEntry := func() bool {
		for _, entry := range logHook.AllEntries() {
			if action, ok := entry.Data["action"]; ok && action == "hnsw_tombstone_cleanup" {
				return true
			}
		}
		return false
	}
	assert.True(t, hasAbortLogEntry(), "expected mid-cleanup OOM abort log entry")

	// Tombstones should still be present because cleanup was aborted mid-cycle
	index.tombstoneLock.RLock()
	tsAfter := len(index.tombstones)
	index.tombstoneLock.RUnlock()
	assert.True(t, tsAfter > 0, "tombstones should remain when cleanup is aborted mid-cycle due to memory pressure")
}
