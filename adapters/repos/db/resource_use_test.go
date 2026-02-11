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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

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

func TestDiskUseReadonly_OverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().SetStatusReadonly(mock.AnythingOfType("string")).Return(nil)

	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})

	// 95% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should be true after exceeding disk threshold")
	shard.AssertCalled(t, "SetStatusReadonly", mock.AnythingOfType("string"))
}

func TestMemUseReadonly_OverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().SetStatusReadonly(mock.AnythingOfType("string")).Return(nil)

	db := testResourceDB(t, 0, 90, map[string]*MockShardLike{"shard1": shard})

	// 95% memory usage, threshold is 90%
	mon := newTestMemMonitor(95, 100)
	db.memUseReadonly(mon)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should be true after exceeding memory threshold")
	shard.AssertCalled(t, "SetStatusReadonly", mock.AnythingOfType("string"))
}

func TestResourceUseReadonly_BothOverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	// May be called twice (once for disk, once for memory). The second call is
	// technically redundant since isReadOnly is already true, but setShardsReadOnly
	// is called for both.
	shard.EXPECT().SetStatusReadonly(mock.AnythingOfType("string")).Return(nil)

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly)
}

func TestDiskUseReadonly_UnderThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	// SetStatusReadonly should NOT be called
	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})

	// 85% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 15, avail: 15}
	db.diskUseReadonly(du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should remain false when under threshold")
	shard.AssertNotCalled(t, "SetStatusReadonly", mock.Anything)
}

func TestDiskUseReadonly_ThresholdDisabled(t *testing.T) {
	shard := NewMockShardLike(t)
	db := testResourceDB(t, 0, 0, map[string]*MockShardLike{"shard1": shard})

	// 95% disk usage, but threshold is 0 (disabled)
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should remain false when threshold is disabled")
	shard.AssertNotCalled(t, "SetStatusReadonly", mock.Anything)
}

func TestResourceUseRecovery_BothBelowThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 50% disk and 50% memory, both below 90% threshold
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should be false after recovery")
	shard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
}

func TestResourceUseRecovery_DiskRecoveredMemoryStillOver(t *testing.T) {
	shard := NewMockShardLike(t)
	// No status changes expected since memory is still above threshold

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 50% disk (below 90%), 95% memory (above 90%)
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should remain true when memory is still over threshold")
	shard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestResourceUseRecovery_MemoryRecoveredDiskStillOver(t *testing.T) {
	shard := NewMockShardLike(t)
	// No status changes expected since disk is still above threshold

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 95% disk (above 90%), 50% memory (below 90%)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should remain true when disk is still over threshold")
	shard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestResourceUseRecovery_BothStillOverThreshold(t *testing.T) {
	shard := NewMockShardLike(t)

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 95% disk and 95% memory, both above 90% threshold
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should remain true when both are over threshold")
	shard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestResourceUseRecovery_ThresholdsDisabled(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	// Both thresholds disabled (0), but isReadOnly was set somehow
	db := testResourceDB(t, 0, 0, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should be false when thresholds are disabled")
}

func TestResourceUseRecovery_OnlyDiskThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	// Only disk threshold enabled, memory disabled
	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 50% disk (below threshold), memory high but threshold disabled
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "should recover when only enabled threshold is below limit")
}

func TestResourceUseRecovery_OnlyMemThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := NewMockShardLike(t)
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	// Only memory threshold enabled, disk disabled
	db := testResourceDB(t, 0, 90, map[string]*MockShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// Disk high but threshold disabled, 50% memory (below threshold)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "should recover when only enabled threshold is below limit")
}

func TestSetShardsReady_OnlyRecoverReadOnlyShards(t *testing.T) {
	readonlyShard := NewMockShardLike(t)
	readonlyShard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	readonlyShard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	readyShard := NewMockShardLike(t)
	readyShard.EXPECT().GetStatus().Return(storagestate.StatusReady)
	// UpdateStatus should NOT be called on a shard that is already READY

	db := testResourceDB(t, 90, 90, map[string]*MockShardLike{
		"readonly_shard": readonlyShard,
		"ready_shard":    readyShard,
	})
	db.resourceScanState.isReadOnly = true

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly)
	readonlyShard.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
	readyShard.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything)
}

func TestReadonlyRecoveryCycle(t *testing.T) {
	// This test simulates the full cycle:
	// 1. Usage goes over threshold → shards become READONLY
	// 2. Usage drops below threshold → shards recover to READY
	// 3. Usage goes over threshold again → shards become READONLY again

	shard := NewMockShardLike(t)

	// Step 1: Expect SetStatusReadonly to be called
	shard.EXPECT().SetStatusReadonly(mock.AnythingOfType("string")).Return(nil).Once()

	db := testResourceDB(t, 90, 0, map[string]*MockShardLike{"shard1": shard})

	// Step 1: Disk usage exceeds threshold
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(0, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "should be readonly after exceeding threshold")

	// Step 2: Disk usage drops below threshold → recovery
	shard.EXPECT().GetStatus().Return(storagestate.StatusReadOnly).Once()
	shard.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil).Once()

	du = diskUse{total: 100, free: 50, avail: 50}
	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "should recover after usage drops below threshold")

	// Step 3: Disk usage exceeds threshold again → readonly again
	shard.EXPECT().SetStatusReadonly(mock.AnythingOfType("string")).Return(nil).Once()

	du = diskUse{total: 100, free: 5, avail: 5}
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "should be readonly again after re-exceeding threshold")
}

func TestSetShardsReady_MultipleIndices(t *testing.T) {
	logger, _ := test.NewNullLogger()

	shard1 := NewMockShardLike(t)
	shard1.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
	shard1.EXPECT().UpdateStatus(storagestate.StatusReady.String(), mock.AnythingOfType("string")).Return(nil)

	shard2 := NewMockShardLike(t)
	shard2.EXPECT().GetStatus().Return(storagestate.StatusReadOnly)
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
	db.resourceScanState.isReadOnly = true

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly)
	shard1.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
	shard2.AssertCalled(t, "UpdateStatus", storagestate.StatusReady.String(), mock.AnythingOfType("string"))
}
