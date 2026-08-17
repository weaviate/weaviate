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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
func testResourceDB(t *testing.T, diskROPercent, memROPercent uint64, shards map[string]ShardLike) *DB {
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

// statusShard is a MockShardLike backed by an in-memory ShardStatus, so tests
// assert the status the scanner leaves behind instead of the calls it made.
type statusShard struct {
	*MockShardLike

	mu     sync.Mutex
	status ShardStatus

	// racingWrite stands in for a producer that changes the status inside the
	// scanner's decision window. It is applied once, right before the scanner's
	// own write would land.
	racingWrite *ShardStatus
}

func newStatusShard(t *testing.T, initial ShardStatus) *statusShard {
	t.Helper()
	s := &statusShard{MockShardLike: NewMockShardLike(t), status: initial}

	s.EXPECT().UpdateStatusIf(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("string")).
		RunAndReturn(func(cond func(ShardStatus) bool, in, reason string) error {
			s.mu.Lock()
			defer s.mu.Unlock()

			if s.racingWrite != nil {
				s.status = *s.racingWrite
				s.racingWrite = nil
			}
			if !cond(s.status) {
				return nil
			}

			status, err := storagestate.ValidateStatus(strings.ToUpper(in))
			if err != nil {
				return err
			}
			s.status = ShardStatus{Status: status, Reason: reason}
			return nil
		}).Maybe()

	return s
}

func (s *statusShard) get() ShardStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func TestDiskUseReadonly_OverThreshold(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady})

	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})

	// 95% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should be true after exceeding disk threshold")
	assert.Equal(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure}, shard.get())
}

func TestMemUseReadonly_OverThreshold(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady})

	db := testResourceDB(t, 0, 90, map[string]ShardLike{"shard1": shard})

	// 95% memory usage, threshold is 90%
	mon := newTestMemMonitor(95, 100)
	db.memUseReadonly(mon)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should be true after exceeding memory threshold")
	assert.Equal(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure}, shard.get())
}

func TestResourceUseReadonly_BothOverThreshold(t *testing.T) {
	// setShardsReadOnly runs for both disk and memory. The second pass finds the
	// shard already read-only and leaves it alone.
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly)
	assert.Equal(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure}, shard.get())
}

func TestDiskUseReadonly_UnderThreshold(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady})
	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})

	// 85% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 15, avail: 15}
	db.diskUseReadonly(du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should remain false when under threshold")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)
}

func TestDiskUseReadonly_ThresholdDisabled(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady})
	db := testResourceDB(t, 0, 0, map[string]ShardLike{"shard1": shard})

	// 95% disk usage, but threshold is 0 (disabled)
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should remain false when threshold is disabled")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)
}

func TestResourceUseRecovery_BothBelowThreshold(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 50% disk and 50% memory, both below 90% threshold
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should be false after recovery")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)
}

func TestResourceUseRecovery_DiskRecoveredMemoryStillOver(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 50% disk (below 90%), 95% memory (above 90%)
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should remain true when memory is still over threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get().Status)
}

func TestResourceUseRecovery_MemoryRecoveredDiskStillOver(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 95% disk (above 90%), 50% memory (below 90%)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should remain true when disk is still over threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get().Status)
}

func TestResourceUseRecovery_BothStillOverThreshold(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 95% disk and 95% memory, both above 90% threshold
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should remain true when both are over threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get().Status)
}

func TestResourceUseRecovery_ThresholdsDisabled(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	// Both thresholds disabled (0), but isReadOnly was set somehow
	db := testResourceDB(t, 0, 0, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should be false when thresholds are disabled")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)
}

func TestResourceUseRecovery_OnlyDiskThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	// Only disk threshold enabled, memory disabled
	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// 50% disk (below threshold), memory high but threshold disabled
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "should recover when only enabled threshold is below limit")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)
}

func TestResourceUseRecovery_OnlyMemThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	// Only memory threshold enabled, disk disabled
	db := testResourceDB(t, 0, 90, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	// Disk high but threshold disabled, 50% memory (below threshold)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "should recover when only enabled threshold is below limit")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)
}

func TestSetShardsReady_OnlyRecoverReadOnlyShards(t *testing.T) {
	readonlyShard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})
	readyShard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady, Reason: statusReasonNotifyReady})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{
		"readonly_shard": readonlyShard,
		"ready_shard":    readyShard,
	})
	db.resourceScanState.isReadOnly = true

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly)
	assert.Equal(t, storagestate.StatusReady, readonlyShard.get().Status)
	assert.Equal(t, ShardStatus{Status: storagestate.StatusReady, Reason: statusReasonNotifyReady}, readyShard.get(),
		"an already-READY shard must not be relabelled by the recovery sweep")
}

func TestReadonlyRecoveryCycle(t *testing.T) {
	// This test simulates the full cycle:
	// 1. Usage goes over threshold → shards become READONLY
	// 2. Usage drops below threshold → shards recover to READY
	// 3. Usage goes over threshold again → shards become READONLY again
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReady})

	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})

	// Step 1: Disk usage exceeds threshold → READONLY (resource pressure)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(0, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "should be readonly after exceeding threshold")
	assert.Equal(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure}, shard.get())

	// Step 2: Disk usage drops below threshold → recovery to READY
	du = diskUse{total: 100, free: 50, avail: 50}
	db.resourceUseRecovery(mon, du)

	assert.False(t, db.resourceScanState.isReadOnly, "should recover after usage drops below threshold")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)

	// Step 3: Disk usage exceeds threshold again → READONLY again
	du = diskUse{total: 100, free: 5, avail: 5}
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.resourceScanState.isReadOnly, "should be readonly again after re-exceeding threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get().Status)
}

func TestSetShardsReady_MultipleIndices(t *testing.T) {
	logger, _ := test.NewNullLogger()

	shard1 := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})
	shard2 := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

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
	assert.Equal(t, storagestate.StatusReady, shard1.get().Status)
	assert.Equal(t, storagestate.StatusReady, shard2.get().Status)
}

func TestSetShardsReady_SkipsNonResourcePressureReadonly(t *testing.T) {
	// A shard that is READONLY due to a vector index config update should NOT
	// be recovered by the resource scanner. Only shards set READONLY due to
	// resource pressure should be transitioned back to READY.
	configUpdateShard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate})
	resourcePressureShard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{
		"config_update_shard":     configUpdateShard,
		"resource_pressure_shard": resourcePressureShard,
	})
	db.resourceScanState.isReadOnly = true

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly)
	assert.Equal(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate}, configUpdateShard.get())
	assert.Equal(t, storagestate.StatusReady, resourcePressureShard.get().Status)
}

// A shard that is READONLY for a vector-index config update must stay READONLY
// across a resource readonly→recovery cycle: the resource scanner must not
// relabel it as resource-pressure and then recover it mid-update.
func TestResourceCycle_DoesNotRecoverConfigUpdateShard(t *testing.T) {
	// Simulate UpdateVectorIndexConfig having marked the shard READONLY.
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"config_update_shard": shard})

	// Resource pressure trips while the config update is in flight.
	mon := newTestMemMonitor(0, 100)
	db.resourceUseReadonly(mon, diskUse{total: 100, free: 5, avail: 5})

	// Resource pressure clears → recovery pass runs.
	db.resourceUseRecovery(mon, diskUse{total: 100, free: 50, avail: 50})

	assert.Equal(t, storagestate.StatusReadOnly, shard.get().Status,
		"config-update shard must stay READONLY across a resource readonly→recovery cycle; "+
			"resource recovery must not re-admit writes while the vector-index config update is still in flight")
}

func TestSetShardsReady_SkipsUserInitiatedReadonly(t *testing.T) {
	// A shard manually set to READONLY by a user should not be auto-recovered.
	userShard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonManualUpdate})

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"user_shard": userShard})
	db.resourceScanState.isReadOnly = true

	db.setShardsReady()

	assert.False(t, db.resourceScanState.isReadOnly)
	assert.Equal(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonManualUpdate}, userShard.get())
}

func TestSetShardsReady_PartialFailure(t *testing.T) {
	successShard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	failingShard := NewMockShardLike(t)
	failingShard.EXPECT().UpdateStatusIf(mock.Anything, storagestate.StatusReady.String(), mock.AnythingOfType("string")).
		Return(fmt.Errorf("disk I/O error"))

	db := testResourceDB(t, 90, 90, map[string]ShardLike{
		"success_shard": successShard,
		"failing_shard": failingShard,
	})
	db.resourceScanState.isReadOnly = true

	db.setShardsReady()

	assert.True(t, db.resourceScanState.isReadOnly, "isReadOnly should remain true when some shards fail to transition")
	assert.Equal(t, storagestate.StatusReady, successShard.get().Status)
}

// Shards held read-only by memory pressure must go ready again once memory
// drops — the scan pass is the only thing refreshing the monitor.
func TestScanResourceUsageOnce_SeesMemoryDropWhileReadOnly(t *testing.T) {
	shard := newStatusShard(t, ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure})

	var used atomic.Int64
	used.Store(95)
	mon := memwatch.NewMonitor(used.Load, func(int64) int64 { return 100 }, 1.0)

	db := testResourceDB(t, 0, 90, map[string]ShardLike{"shard1": shard})
	db.resourceScanState.isReadOnly = true

	used.Store(10)
	db.scanResourceUsageOnce(mon, diskUse{total: 100, free: 100, avail: 100}, false)

	assert.False(t, db.resourceScanState.isReadOnly, "isReadOnly should lift once memory drops")
	assert.Equal(t, storagestate.StatusReady, shard.get().Status)
}

// Both sweeps decide and write in one shard call, so a status set between the
// two never gets overwritten. Reading the status first and writing it after
// leaves a window in which a freeze another writer just set is relabelled as
// resource pressure — and then lifted by the recovery sweep.
func TestResourceScanner_StatusChangedInsideDecisionWindow(t *testing.T) {
	tests := []struct {
		name        string
		initial     ShardStatus
		racingWrite ShardStatus
		recovery    bool
	}{
		{
			name:        "readonly sweep, manual freeze lands first",
			initial:     ShardStatus{Status: storagestate.StatusReady},
			racingWrite: ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonManualUpdate},
		},
		{
			name:        "readonly sweep, vector-index freeze lands first",
			initial:     ShardStatus{Status: storagestate.StatusReady},
			racingWrite: ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate},
		},
		{
			name:        "recovery sweep, manual freeze lands first",
			initial:     ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure},
			racingWrite: ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonManualUpdate},
			recovery:    true,
		},
		{
			name:        "recovery sweep, vector-index freeze lands first",
			initial:     ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonResourcePressure},
			racingWrite: ShardStatus{Status: storagestate.StatusReadOnly, Reason: statusReasonVectorIndexUpdate},
			recovery:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shard := newStatusShard(t, tc.initial)
			shard.racingWrite = &tc.racingWrite

			db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})
			mon := newTestMemMonitor(0, 100)

			if tc.recovery {
				db.resourceScanState.isReadOnly = true
				db.resourceUseRecovery(mon, diskUse{total: 100, free: 50, avail: 50})
			} else {
				db.resourceUseReadonly(mon, diskUse{total: 100, free: 5, avail: 5})
			}

			require.Equal(t, tc.racingWrite, shard.get(),
				"the scanner must not overwrite a status set inside its decision window")
		})
	}
}

// The scanner sweeps every shard on the node, including the cold ones. Loading
// them to record a status resurrects tenants that were deliberately unloaded,
// and the load itself fails under the memory pressure that triggered the sweep
// — panicking through mustLoad and killing the scan goroutine for good.
func TestResourceScanner_DoesNotForceLoadColdShards(t *testing.T) {
	tests := []struct {
		name     string
		recovery bool
	}{
		{name: "readonly sweep"},
		{name: "recovery sweep", recovery: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			warm, idx := testShard(t, t.Context(), "ResourceScanColdShard")
			cold := newColdShard(idx, "cold_tenant")
			idx.shards.Store(cold.Name(), cold)

			logger, _ := test.NewNullLogger()
			db := &DB{
				logger: logger,
				config: Config{
					ResourceUsage: config.ResourceUsage{
						DiskUse: config.DiskUse{ReadOnlyPercentage: 90},
					},
				},
				resourceScanState: newResourceScanState(),
				indices:           map[string]*Index{idx.ID(): idx},
			}
			mon := newTestMemMonitor(0, 100)

			require.NotPanics(t, func() {
				if tc.recovery {
					require.NoError(t, warm.UpdateStatus(storagestate.StatusReadOnly.String(), statusReasonResourcePressure))
					db.resourceScanState.isReadOnly = true
					db.resourceUseRecovery(mon, diskUse{total: 100, free: 50, avail: 50})
				} else {
					db.resourceUseReadonly(mon, diskUse{total: 100, free: 5, avail: 5})
				}
			})

			require.False(t, cold.isLoaded(), "a cold tenant must not be loaded to record a status")

			wantWarm := storagestate.StatusReadOnly
			if tc.recovery {
				wantWarm = storagestate.StatusReady
			}
			require.Equal(t, wantWarm, warm.GetStatus(), "the loaded shard must still be swept")
		})
	}
}
