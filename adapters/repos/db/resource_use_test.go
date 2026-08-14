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
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// newTestMemMonitor creates a memwatch.Monitor with controllable memory usage.
// The ratio returned by Ratio() will be usedMemory/limit.
// For example, usedMemory=95, limit=100 gives Ratio()=0.95 (i.e. 95%).
func newTestMemMonitor(usedMemory, limit int64) *memwatch.Monitor {
	return memwatch.NewMonitor(
		func() int64 { return usedMemory },
		func(size int64) int64 { return limit },
		1.0,
	)
}

// testResourceDB creates a minimal DB with one index containing the given mock shards.
// The DB is configured with the given disk and memory readonly thresholds.
func testResourceDB(t *testing.T, diskROPercent, memROPercent uint64, shards map[string]*MockShardLike) *DB {
	t.Helper()
	asShardLike := make(map[string]ShardLike, len(shards))
	for name, shard := range shards {
		asShardLike[name] = shard
	}
	return testResourceDBWithShards(t, diskROPercent, memROPercent, asShardLike)
}

// testResourceDBWithShards is testResourceDB for shards that are not mocks, e.g.
// a LazyLoadShard.
func testResourceDBWithShards(t *testing.T, diskROPercent, memROPercent uint64, shards map[string]ShardLike) *DB {
	t.Helper()
	logger, _ := test.NewNullLogger()

	idx := &Index{
		closingCtx: context.Background(),
		logger:     logger,
		shards:     shardMap{},
	}
	for name, shard := range shards {
		idx.shards.Store(name, shard)
	}

	return &DB{
		logger: logger,
		config: Config{
			ResourceUsage: config.ResourceUsage{
				DiskUse: config.DiskUse{
					ReadOnlyPercentage: diskROPercent,
				},
				MemUse: config.MemUse{
					ReadOnlyPercentage: memROPercent,
				},
			},
		},
		resourceScanState: newResourceScanState(),
		indices: map[string]*Index{
			"TestIndex": idx,
		},
	}
}

// newStatefulShardMock returns a MockShardLike whose GetStatus/GetStatusReason
// reflect the mutations made by SetStatusReadonly/UpdateStatus. The mutex guards
// reads from the test goroutine.
func newStatefulShardMock(t *testing.T, initial ShardStatus) (*MockShardLike, *ShardStatus, *sync.Mutex) {
	t.Helper()
	mu := &sync.Mutex{}
	status := &initial
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().RunAndReturn(func() storagestate.Status {
		mu.Lock()
		defer mu.Unlock()
		return status.Status
	}).Maybe()
	shard.EXPECT().GetStatusReason().RunAndReturn(func() string {
		mu.Lock()
		defer mu.Unlock()
		return status.Reason
	}).Maybe()
	shard.EXPECT().SetStatusReadonly(mock.AnythingOfType("string")).RunAndReturn(func(reason string) error {
		mu.Lock()
		defer mu.Unlock()
		status.Status = storagestate.StatusReadOnly
		status.Reason = reason
		return nil
	}).Maybe()
	shard.EXPECT().UpdateStatus(mock.AnythingOfType("string"), mock.AnythingOfType("string")).RunAndReturn(func(in, reason string) error {
		mu.Lock()
		defer mu.Unlock()
		st, err := storagestate.ValidateStatus(strings.ToUpper(in))
		if err != nil {
			return err
		}
		status.Status = st
		status.Reason = reason
		return nil
	}).Maybe()
	return shard, status, mu
}

func TestDiskUseReadonly_OverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	shard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).Return(nil)

	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})

	// 95% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should be true after exceeding disk threshold")
	shard.AssertCalled(t, "SetStatusReadonly", statusReasonResourcePressure)
}

func TestMemUseReadonly_OverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	shard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).Return(nil)

	db := testResourceDB(t, 0, 90, map[string]*MockShardLike{"shard1": shard})

	// 95% memory usage, threshold is 90%
	mon := newTestMemMonitor(95, 100)
	db.memUseReadonly(mon)

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should be true after exceeding memory threshold")
	shard.AssertCalled(t, "SetStatusReadonly", statusReasonResourcePressure)
}

func TestResourceUseReadonly_BothOverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	// May be called twice (once for disk, once for memory). The second call is
	// technically redundant since isReadOnly is already true, but setShardsReadOnly
	// is called for both.
	shard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	shard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).Return(nil)

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly.Load())
}

func TestDiskUseReadonly_UnderThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	// SetStatusReadonly should NOT be called
	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})

	// 85% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 15, avail: 15}
	db.diskUseReadonly(du)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should remain false when under threshold")
	shard.AssertNotCalled(t, "SetStatusReadonly", mock.Anything)
}

func TestDiskUseReadonly_ThresholdDisabled(t *testing.T) {
	shard := NewMockShardLike(t)
	db := testResourceDB(t, 0, 0, map[string]*MockShardLike{"shard1": shard})

	// 95% disk usage, but threshold is 0 (disabled)
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should remain false when threshold is disabled")
	shard.AssertNotCalled(t, "SetStatusReadonly", mock.Anything)
}

// A shard that fails to go READONLY must not take the process down, and must not
// stop the scan from marking the remaining shards.
func TestSetShardsReadOnly_ShardError(t *testing.T) {
	tests := []struct {
		name           string
		shardErr       error
		wantErrorLevel bool
	}{
		{
			name:     "store closed concurrently",
			shardErr: fmt.Errorf("%w: updating buckets state in store %q", lsmkv.ErrAlreadyClosed, "/data/shard"),
		},
		{
			name:           "unexpected error",
			shardErr:       fmt.Errorf("disk I/O error"),
			wantErrorLevel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failingShard := NewMockShardLike(t)
			failingShard.EXPECT().GetStatus().Return(storagestate.StatusReady)
			failingShard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).Return(tt.shardErr)

			healthyShard := NewMockShardLike(t)
			healthyShard.EXPECT().GetStatus().Return(storagestate.StatusReady)
			healthyShard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).Return(nil)

			db := testResourceDB(t, 90, 0, map[string]*MockShardLike{
				"failing_shard": failingShard,
				"healthy_shard": healthyShard,
			})
			logger := db.logger.(*logrus.Logger)
			hook := test.NewLocal(logger)
			exited := false
			logger.ExitFunc = func(int) { exited = true }

			// 95% disk usage, threshold is 90%
			db.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})

			assert.False(t, exited, "a failed shard status update must not exit the process")
			healthyShard.AssertCalled(t, "SetStatusReadonly", statusReasonResourcePressure)
			assert.True(t, db.resourceScanState.isReadOnly.Load())

			errLine := firstErrorEntry(hook)
			if tt.wantErrorLevel {
				require.NotNil(t, errLine, "an unexpected error must be logged")
				assert.Contains(t, errLine.Message, "failing_shard")
			} else {
				assert.Nil(t, errLine, "a shard closing concurrently is routine, not an error")
			}
		})
	}
}

// A panic mid-scan must not leave the DB-wide index lock held: the scan runs in
// a GoWrapper goroutine that recovers and exits, so a held lock would block
// every later index operation for the life of the process.
func TestSetShardsReadOnly_PanicReleasesIndexLock(t *testing.T) {
	panickingShard := NewMockShardLike(t)
	panickingShard.EXPECT().GetStatus().RunAndReturn(func() storagestate.Status {
		panic("shard status read blew up")
	})

	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"panicking_shard": panickingShard})

	assert.Panics(t, func() {
		db.setShardsReadOnly(statusReasonResourcePressure)
	})

	require.True(t, db.indexLock.TryLock(), "index lock must be released when the scan panics")
	db.indexLock.Unlock()
}

// The resource scanner must not force-load a cold shard: loading one costs the
// memory the scan may be reacting to, and a failed load panics out of the scan
// goroutine, leaving the node without resource protection.
func TestSetShardsReadOnly_SkipsUnloadedShard(t *testing.T) {
	coldShard := &LazyLoadShard{shardOpts: &deferredShardOpts{name: "cold_shard"}}

	loadedShard := NewMockShardLike(t)
	loadedShard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	loadedShard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).Return(nil)

	db := testResourceDBWithShards(t, 90, 0, map[string]ShardLike{
		"cold_shard":   coldShard,
		"loaded_shard": loadedShard,
	})

	// 95% disk usage, threshold is 90%
	assert.NotPanics(t, func() {
		db.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})
	})

	assert.False(t, coldShard.isLoaded(), "a cold shard must not be loaded by the resource scan")
	loadedShard.AssertCalled(t, "SetStatusReadonly", statusReasonResourcePressure)
	assert.True(t, db.resourceScanState.isReadOnly.Load())
}

// The flag must be raised before the sweep: a shard loading concurrently reads
// it to decide whether it comes up READONLY, and the sweep can only see shards
// that finished loading. Raising it after the sweep leaves a window in which a
// shard is caught by neither.
func TestSetShardsReadOnly_FlagRaisedBeforeSweep(t *testing.T) {
	var db *DB
	var flagDuringSweep atomic.Bool

	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	shard.EXPECT().SetStatusReadonly(statusReasonResourcePressure).RunAndReturn(func(string) error {
		flagDuringSweep.Store(db.resourceScanState.isReadOnly.Load())
		return nil
	})

	db = testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})

	// 95% disk usage, threshold is 90%
	db.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})

	assert.True(t, flagDuringSweep.Load(),
		"a shard loading while the sweep runs must already see the read-only flag")
}

func TestResourceUseRecovery_BothBelowThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	// 50% disk and 50% memory, both below 90% threshold
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should be false after recovery")
	shard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
}

func TestResourceUseRecovery_DiskRecoveredMemoryStillOver(t *testing.T) {
	shard := NewMockShardLike(t)
	// No status changes expected since memory is still above threshold

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	// 50% disk (below 90%), 95% memory (above 90%)
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should remain true when memory is still over threshold")
	shard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestResourceUseRecovery_MemoryRecoveredDiskStillOver(t *testing.T) {
	shard := NewMockShardLike(t)
	// No status changes expected since disk is still above threshold

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	// 95% disk (above 90%), 50% memory (below 90%)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should remain true when disk is still over threshold")
	shard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestResourceUseRecovery_BothStillOverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	// 95% disk and 95% memory, both above 90% threshold
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should remain true when both are over threshold")
	shard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestResourceUseRecovery_ThresholdsDisabled(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	// Both thresholds disabled (0), but isReadOnly was set somehow
	db := testResourceDB(t, 0, 0, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should be false when thresholds are disabled")
}

func TestResourceUseRecovery_OnlyDiskThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	// Only disk threshold enabled, memory disabled
	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	// 50% disk (below threshold), memory high but threshold disabled
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "should recover when only enabled threshold is below limit")
}

func TestResourceUseRecovery_OnlyMemThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	// Only memory threshold enabled, disk disabled
	db := testResourceDB(t, 0, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	// Disk high but threshold disabled, 50% memory (below threshold)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "should recover when only enabled threshold is below limit")
}

func TestSetShardsReady_OnlyRecoverReadOnlyShards(t *testing.T) {
	readonlyShard := NewMockShardLike(t)
	readonlyShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	readonlyShard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	readonlyShard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	readyShard := NewMockShardLike(t)
	readyShard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	// UpdateStatus should NOT be called on a shard that is already READY

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{
		"readonly_shard": readonlyShard,
		"ready_shard":    readyShard,
	})
	db.resourceScanState.isReadOnly.Store(true)

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly.Load())
	readonlyShard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
	readyShard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

// The flag must be dropped before the sweep: a shard loading concurrently
// would otherwise inherit READONLY after the sweep already passed it, and
// nothing would flip it back - the recovery pass only runs while the flag is
// set.
func TestSetShardsReady_FlagDroppedBeforeSweep(t *testing.T) {
	var db *DB
	var flagDuringSweep atomic.Bool

	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).
		RunAndReturn(func(status, reason string) error {
			flagDuringSweep.Store(db.resourceScanState.isReadOnly.Load())
			return nil
		})

	db = testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	db.setShardsReady()

	assert.False(t, flagDuringSweep.Load(),
		"a shard loading while the recovery sweep runs must no longer see the read-only flag")
}

// A shard whose store closed concurrently (tenant deletion, deactivation,
// shutdown) must not hold the whole DB in read-only mode: it takes no writes
// either way, and comes back READY the next time it is loaded.
func TestSetShardsReady_ClosedStoreDoesNotBlockRecovery(t *testing.T) {
	closingShard := NewMockShardLike(t)
	closingShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	closingShard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	closingShard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).
		Return(fmt.Errorf("%w: updating buckets state in store %q", lsmkv.ErrAlreadyClosed, "/data/shard"))

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"closing_shard": closingShard})
	db.resourceScanState.isReadOnly.Store(true)
	hook := test.NewLocal(db.logger.(*logrus.Logger))

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly.Load(),
		"a shard closing concurrently must not keep the DB read-only")
	assert.Nil(t, firstErrorEntry(hook), "a shard closing concurrently is routine, not an error")
}

func TestReadonlyRecoveryCycle(t *testing.T) {
	// This test simulates the full cycle:
	// 1. Usage goes over threshold → shards become READONLY
	// 2. Usage drops below threshold → shards recover to READY
	// 3. Usage goes over threshold again → shards become READONLY again

	// Stateful mock so GetStatus reflects the actual transitions across the
	// cycle (setShardsReadOnly now skips shards that are already read-only).
	shard, status, mu := newStatefulShardMock(t, ShardStatus{Status: storagestate.StatusReady})

	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})

	// Step 1: Disk usage exceeds threshold → READONLY (resource pressure)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(0, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "should be readonly after exceeding threshold")
	mu.Lock()
	assert.Equal(t, storagestate.StatusReadOnly, status.Status)
	assert.Equal(t, statusReasonResourcePressure, status.Reason)
	mu.Unlock()

	// Step 2: Disk usage drops below threshold → recovery to READY
	du = diskUse{total: 100, free: 50, avail: 50}
	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "should recover after usage drops below threshold")
	mu.Lock()
	assert.Equal(t, storagestate.StatusReady, status.Status)
	mu.Unlock()

	// Step 3: Disk usage exceeds threshold again → READONLY again
	du = diskUse{total: 100, free: 5, avail: 5}
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "should be readonly again after re-exceeding threshold")
	mu.Lock()
	assert.Equal(t, storagestate.StatusReadOnly, status.Status)
	mu.Unlock()
}

func TestSetShardsReady_MultipleIndices(t *testing.T) {
	logger, _ := test.NewNullLogger()

	shard1 := NewMockShardLike(t)
	shard1.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard1.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard1.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	shard2 := NewMockShardLike(t)
	shard2.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard2.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard2.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	idx1 := &Index{
		closingCtx: context.Background(),
		logger:     logger,
		shards:     shardMap{},
	}
	idx1.shards.Store("shard1", shard1)

	idx2 := &Index{
		closingCtx: context.Background(),
		logger:     logger,
		shards:     shardMap{},
	}
	idx2.shards.Store("shard2", shard2)

	db := &DB{
		logger: logger,
		config: Config{
			ResourceUsage: config.ResourceUsage{
				DiskUse: config.DiskUse{ReadOnlyPercentage: 90},
				MemUse:  config.MemUse{ReadOnlyPercentage: 90},
			},
		},
		resourceScanState: newResourceScanState(),
		indices: map[string]*Index{
			"Index1": idx1,
			"Index2": idx2,
		},
	}
	db.resourceScanState.isReadOnly.Store(true)

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly.Load())
	shard1.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
	shard2.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
}

func TestSetShardsReady_SkipsNonResourcePressureReadonly(t *testing.T) {
	// A shard that is READONLY due to a vector index config update should NOT
	// be recovered by the resource scanner. Only shards set READONLY due to
	// resource pressure should be transitioned back to READY.
	configUpdateShard := NewMockShardLike(t)
	configUpdateShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	configUpdateShard.EXPECT().GetStatusReason().Return(statusReasonVectorIndexUpdate)
	// UpdateStatus should NOT be called

	resourcePressureShard := NewMockShardLike(t)
	resourcePressureShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	resourcePressureShard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	resourcePressureShard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{
		"config_update_shard":     configUpdateShard,
		"resource_pressure_shard": resourcePressureShard,
	})
	db.resourceScanState.isReadOnly.Store(true)

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly.Load())
	configUpdateShard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
	resourcePressureShard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
}

// A shard that is READONLY for a vector-index config update must stay READONLY
// across a resource readonly→recovery cycle: the resource scanner must not
// relabel it as resource-pressure and then recover it mid-update.
func TestResourceCycle_DoesNotRecoverConfigUpdateShard(t *testing.T) {
	// Simulate UpdateVectorIndexConfig having marked the shard READONLY.
	shard, status, mu := newStatefulShardMock(t,
		ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate})

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"config_update_shard": shard})

	// Resource pressure trips while the config update is in flight.
	mon := newTestMemMonitor(0, 100)
	db.resourceUseReadonly(mon, diskUse{total: 100, free: 5, avail: 5})

	// Resource pressure clears → recovery pass runs.
	db.resourceUseRecovery(mon, diskUse{total: 100, free: 50, avail: 50})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, storagestate.StatusReadOnly, status.Status,
		"config-update shard must stay READONLY across a resource readonly→recovery cycle; "+
			"resource recovery must not re-admit writes while the vector-index config update is still in flight")
}

func TestSetShardsReady_SkipsUserInitiatedReadonly(t *testing.T) {
	// A shard manually set to READONLY by a user should not be auto-recovered.
	userShard := NewMockShardLike(t)
	userShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	userShard.EXPECT().GetStatusReason().Return("manually set by user")
	// UpdateStatus should NOT be called

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"user_shard": userShard})
	db.resourceScanState.isReadOnly.Store(true)

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly.Load())
	userShard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestSetShardsReady_PartialFailure(t *testing.T) {
	successShard := NewMockShardLike(t)
	successShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	successShard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	successShard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	failingShard := NewMockShardLike(t)
	failingShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	failingShard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	failingShard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(fmt.Errorf("disk I/O error"))

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{
		"success_shard": successShard,
		"failing_shard": failingShard,
	})
	db.resourceScanState.isReadOnly.Store(true)

	db.setShardsReady()

	assert.True(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should remain true when some shards fail to transition")
	successShard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
	failingShard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
}

// Shards held read-only by memory pressure must go ready again once memory
// drops — the scan pass is the only thing refreshing the monitor.
func TestScanResourceUsageOnce_SeesMemoryDropWhileReadOnly(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().GetStatusReason().Return(statusReasonResourcePressure)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	var used atomic.Int64
	used.Store(95)
	mon := memwatch.NewMonitor(used.Load, func(int64) int64 { return 100 }, 1.0)

	db := testResourceDB(t, 0, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly.Store(true)

	used.Store(10)
	db.scanResourceUsageOnce(mon, diskUse{total: 100, free: 100, avail: 100}, false)

	assert.False(t, db.resourceScanState.isReadOnly.Load(), "isReadOnly should lift once memory drops")
	shard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
}
