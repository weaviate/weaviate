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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
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

// testResourceDB creates a minimal DB with one index containing the given shards.
// The DB is configured with the given disk and memory readonly thresholds.
func testResourceDB(t *testing.T, diskROPercent, memROPercent uint64, shards map[string]ShardLike) *DB {
	t.Helper()
	logger, _ := test.NewNullLogger()

	idx := &Index{
		closingCtx: context.Background(),
		logger:     logger,
		shards:     shardMap{},
	}
	overlay := &clusterschema.NodeOverlay{}
	for name, shard := range shards {
		idx.shards.Store(name, shard)
		if fake, ok := shard.(*statusShard); ok {
			fake.overlay = overlay
		}
	}

	return &DB{
		localNodeName: "node1",
		logger:        logger,
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
		nodeOverlay:       overlay,
		indices: map[string]*Index{
			"TestIndex": idx,
		},
	}
}

// statusShard is a MockShardLike that settles against the overlay like a real
// shard, so tests assert the status the scanner leaves behind instead of the
// calls it made.
type statusShard struct {
	*MockShardLike

	overlay *clusterschema.NodeOverlay

	mu     sync.Mutex
	status storagestate.Status

	// updateErr is returned instead of settling, standing in for a shard whose
	// store rejects the change.
	updateErr error

	// onUpdate runs at the start of every settle, before the shard's own lock is
	// taken, so a probe can observe what the sweep holds around it.
	onUpdate func()
}

func newStatusShard(t *testing.T, initial storagestate.Status) *statusShard {
	t.Helper()
	s := &statusShard{MockShardLike: NewMockShardLike(t), status: initial}

	s.EXPECT().syncStorePause().RunAndReturn(func() error {
		if s.onUpdate != nil {
			s.onUpdate()
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		if s.updateErr != nil {
			return s.updateErr
		}
		s.status = storagestate.StatusReady
		if s.overlay.ResourcePressure() {
			s.status = storagestate.StatusReadOnly
		}
		return nil
	}).Maybe()

	return s
}

func (s *statusShard) get() storagestate.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func TestDiskUseReadonly_OverThreshold(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReady)

	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})

	// 95% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.True(t, db.nodeOverlay.ResourcePressure(), "pressure should be true after exceeding disk threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())
}

func TestMemUseReadonly_OverThreshold(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReady)

	db := testResourceDB(t, 0, 90, map[string]ShardLike{"shard1": shard})

	// 95% memory usage, threshold is 90%
	mon := newTestMemMonitor(95, 100)
	db.memUseReadonly(mon)

	assert.True(t, db.nodeOverlay.ResourcePressure(), "pressure should be true after exceeding memory threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())
}

func TestResourceUseReadonly_BothOverThreshold(t *testing.T) {
	// setShardsReadOnly runs for both disk and memory. The second pass finds the
	// shard already read-only and leaves it alone.
	shard := newStatusShard(t, storagestate.StatusReady)

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.nodeOverlay.ResourcePressure())
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())
}

func TestDiskUseReadonly_UnderThreshold(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReady)
	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})

	// 85% disk usage, threshold is 90%
	du := diskUse{total: 100, free: 15, avail: 15}
	db.diskUseReadonly(du)

	assert.False(t, db.nodeOverlay.ResourcePressure(), "pressure should remain false when under threshold")
	assert.Equal(t, storagestate.StatusReady, shard.get())
}

func TestDiskUseReadonly_ThresholdDisabled(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReady)
	db := testResourceDB(t, 0, 0, map[string]ShardLike{"shard1": shard})

	// 95% disk usage, but threshold is 0 (disabled)
	du := diskUse{total: 100, free: 5, avail: 5}
	db.diskUseReadonly(du)

	assert.False(t, db.nodeOverlay.ResourcePressure(), "pressure should remain false when threshold is disabled")
	assert.Equal(t, storagestate.StatusReady, shard.get())
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
			failingShard := newStatusShard(t, storagestate.StatusReady)
			failingShard.updateErr = tt.shardErr

			healthyShard := newStatusShard(t, storagestate.StatusReady)

			db := testResourceDB(t, 90, 0, map[string]ShardLike{
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
			assert.Equal(t, storagestate.StatusReadOnly, healthyShard.get())
			assert.True(t, db.nodeOverlay.ResourcePressure())

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

// A sweep must not hold the DB-wide index lock while it touches shards.
// Deciding whether a shard is loaded blocks on that shard's load lock - which
// is what makes a sweep and a shard building itself mutually exclusive - and a
// shard load can run for minutes. Every object read and write takes indexLock
// read side, so holding it across a load would stall the whole node.
func TestResourceSweep_DoesNotHoldIndexLock(t *testing.T) {
	tests := []struct {
		name    string
		initial storagestate.Status
		sweep   func(db *DB)
	}{
		{
			name:    "read-only sweep",
			initial: storagestate.StatusReady,
			sweep:   func(db *DB) { db.setShardsReadOnly() },
		},
		{
			name:    "recovery sweep",
			initial: storagestate.StatusReadOnly,
			sweep:   func(db *DB) { db.setShardsReady() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var db *DB
			var heldDuringSweep atomic.Bool

			shard := newStatusShard(t, tt.initial)
			shard.onUpdate = func() {
				// TryRLock fails while any goroutine holds the write side, and a
				// RWMutex is not reentrant, so this reports the lock either way.
				if db.indexLock.TryRLock() {
					db.indexLock.RUnlock()
					return
				}
				heldDuringSweep.Store(true)
			}

			db = testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
			db.nodeOverlay.SetResourcePressure(true)

			tt.sweep(db)

			assert.False(t, heldDuringSweep.Load(),
				"the sweep must release indexLock before it touches shards")
		})
	}
}

// A panic mid-scan must not leave the DB-wide index lock held: the scan runs in
// a GoWrapper goroutine that recovers and exits, so a held lock would block
// every later index operation for the life of the process.
func TestSetShardsReadOnly_PanicReleasesIndexLock(t *testing.T) {
	panickingShard := newStatusShard(t, storagestate.StatusReady)
	panickingShard.onUpdate = func() { panic("shard status write blew up") }

	db := testResourceDB(t, 90, 0, map[string]ShardLike{"panicking_shard": panickingShard})

	assert.Panics(t, func() {
		db.setShardsReadOnly()
	})

	require.True(t, db.indexLock.TryLock(), "index lock must be released when the scan panics")
	db.indexLock.Unlock()
}

// The resource scanner must not force-load a cold shard: loading one costs the
// memory the scan may be reacting to, and a failed load panics out of the scan
// goroutine, leaving the node without resource protection.
func TestSetShardsReadOnly_SkipsUnloadedShard(t *testing.T) {
	coldShard := &LazyLoadShard{shardOpts: &deferredShardOpts{name: "cold_shard"}}
	loadedShard := newStatusShard(t, storagestate.StatusReady)

	db := testResourceDB(t, 90, 0, map[string]ShardLike{
		"cold_shard":   coldShard,
		"loaded_shard": loadedShard,
	})

	// 95% disk usage, threshold is 90%
	assert.NotPanics(t, func() {
		db.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})
	})

	assert.False(t, coldShard.isLoaded(), "a cold shard must not be loaded by the resource scan")
	assert.Equal(t, storagestate.StatusReadOnly, loadedShard.get())
	assert.True(t, db.nodeOverlay.ResourcePressure())
}

// The flag must be raised before the sweep: a shard loading concurrently reads
// it to decide whether it comes up READONLY, and the sweep can only see shards
// that finished loading. Raising it after the sweep leaves a window in which a
// shard is caught by neither.
func TestSetShardsReadOnly_FlagRaisedBeforeSweep(t *testing.T) {
	var db *DB
	var flagDuringSweep atomic.Bool

	shard := newStatusShard(t, storagestate.StatusReady)
	shard.onUpdate = func() { flagDuringSweep.Store(db.nodeOverlay.ResourcePressure()) }

	db = testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})

	// 95% disk usage, threshold is 90%
	db.diskUseReadonly(diskUse{total: 100, free: 5, avail: 5})

	assert.True(t, flagDuringSweep.Load(),
		"a shard loading while the sweep runs must already see the read-only flag")
}

func TestResourceUseRecovery_BothBelowThreshold(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	// 50% disk and 50% memory, both below 90% threshold
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.nodeOverlay.ResourcePressure(), "pressure should be false after recovery")
	assert.Equal(t, storagestate.StatusReady, shard.get())
}

func TestResourceUseRecovery_DiskRecoveredMemoryStillOver(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	// 50% disk (below 90%), 95% memory (above 90%)
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.nodeOverlay.ResourcePressure(), "pressure should remain true when memory is still over threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())
}

func TestResourceUseRecovery_MemoryRecoveredDiskStillOver(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	// 95% disk (above 90%), 50% memory (below 90%)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.nodeOverlay.ResourcePressure(), "pressure should remain true when disk is still over threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())
}

func TestResourceUseRecovery_BothStillOverThreshold(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	// 95% disk and 95% memory, both above 90% threshold
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.True(t, db.nodeOverlay.ResourcePressure(), "pressure should remain true when both are over threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())
}

func TestResourceUseRecovery_ThresholdsDisabled(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	// Both thresholds are disabled (0), but pressure was set somehow
	db := testResourceDB(t, 0, 0, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.nodeOverlay.ResourcePressure(), "pressure should be false when thresholds are disabled")
	assert.Equal(t, storagestate.StatusReady, shard.get())
}

func TestResourceUseRecovery_OnlyDiskThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	// Only disk threshold enabled, memory disabled
	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	// 50% disk (below threshold), memory high but threshold disabled
	du := diskUse{total: 100, free: 50, avail: 50}
	mon := newTestMemMonitor(95, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.nodeOverlay.ResourcePressure(), "should recover when only enabled threshold is below limit")
	assert.Equal(t, storagestate.StatusReady, shard.get())
}

func TestResourceUseRecovery_OnlyMemThresholdEnabled_BelowThreshold(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	// Only memory threshold enabled, disk disabled
	db := testResourceDB(t, 0, 90, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	// Disk high but threshold disabled, 50% memory (below threshold)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(50, 100)

	db.resourceUseRecovery(mon, du)

	assert.False(t, db.nodeOverlay.ResourcePressure(), "should recover when only enabled threshold is below limit")
	assert.Equal(t, storagestate.StatusReady, shard.get())
}

func TestSetShardsReady_SyncsEveryShard(t *testing.T) {
	shards := map[string]ShardLike{
		"shard1": newStatusShard(t, storagestate.StatusReadOnly),
		"shard2": newStatusShard(t, storagestate.StatusReadOnly),
	}
	db := testResourceDB(t, 90, 90, shards)
	db.nodeOverlay.SetResourcePressure(true)

	db.setShardsReady()

	assert.False(t, db.nodeOverlay.ResourcePressure())
	for name, shard := range shards {
		assert.Equal(t, storagestate.StatusReady, shard.(*statusShard).get(), name)
	}
}

// The flag must be dropped before the sweep: a shard loading concurrently
// would otherwise inherit READONLY after the sweep already passed it, and
// nothing would flip it back - the recovery pass only runs while the flag is
// set.
func TestSetShardsReady_FlagDroppedBeforeSweep(t *testing.T) {
	var db *DB
	var flagDuringSweep atomic.Bool

	shard := newStatusShard(t, storagestate.StatusReadOnly)
	shard.onUpdate = func() { flagDuringSweep.Store(db.nodeOverlay.ResourcePressure()) }

	db = testResourceDB(t, 90, 90, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	db.setShardsReady()

	assert.False(t, flagDuringSweep.Load(),
		"a shard loading while the recovery sweep runs must no longer see the read-only flag")
}

// A shard whose store closed concurrently (tenant deletion, deactivation,
// shutdown) must not hold the whole DB in read-only mode: it takes no writes
// either way, and comes back READY the next time it is loaded.
func TestSetShardsReady_ClosedStoreDoesNotBlockRecovery(t *testing.T) {
	closingShard := newStatusShard(t, storagestate.StatusReadOnly)
	closingShard.updateErr = fmt.Errorf("%w: updating buckets state in store %q", lsmkv.ErrAlreadyClosed, "/data/shard")

	db := testResourceDB(t, 90, 90, map[string]ShardLike{"closing_shard": closingShard})
	db.nodeOverlay.SetResourcePressure(true)
	hook := test.NewLocal(db.logger.(*logrus.Logger))

	db.setShardsReady()

	assert.False(t, db.nodeOverlay.ResourcePressure(),
		"a shard closing concurrently must not keep the DB read-only")
	assert.Nil(t, firstErrorEntry(hook), "a shard closing concurrently is routine, not an error")
}

// The recovery sweep must skip cold shards for the same reason the read-only
// sweep does: a cold shard has no resource-pressure status to release, and
// force-loading one spends the memory this pass is recovering from. It must
// also not count as a failed transition, which would put the DB straight back
// into read-only.
func TestSetShardsReady_SkipsUnloadedShard(t *testing.T) {
	coldShard := &LazyLoadShard{shardOpts: &deferredShardOpts{name: "cold_shard"}}
	loadedShard := newStatusShard(t, storagestate.StatusReadOnly)

	db := testResourceDB(t, 90, 90, map[string]ShardLike{
		"cold_shard":   coldShard,
		"loaded_shard": loadedShard,
	})
	db.nodeOverlay.SetResourcePressure(true)

	assert.NotPanics(t, func() { db.setShardsReady() })

	assert.False(t, coldShard.isLoaded(), "a cold shard must not be loaded by the recovery sweep")
	assert.Equal(t, storagestate.StatusReady, loadedShard.get())
	assert.False(t, db.nodeOverlay.ResourcePressure(),
		"a skipped cold shard must not count as a failed transition")
}

func TestReadonlyRecoveryCycle(t *testing.T) {
	// This test simulates the full cycle:
	// 1. Usage goes over threshold → shards become READONLY
	// 2. Usage drops below threshold → shards recover to READY
	// 3. Usage goes over threshold again → shards become READONLY again
	shard := newStatusShard(t, storagestate.StatusReady)

	db := testResourceDB(t, 90, 0, map[string]ShardLike{"shard1": shard})

	// Step 1: Disk usage exceeds threshold → READONLY (resource pressure)
	du := diskUse{total: 100, free: 5, avail: 5}
	mon := newTestMemMonitor(0, 100)
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.nodeOverlay.ResourcePressure(), "should be readonly after exceeding threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())

	// Step 2: Disk usage drops below threshold → recovery to READY
	du = diskUse{total: 100, free: 50, avail: 50}
	db.resourceUseRecovery(mon, du)

	assert.False(t, db.nodeOverlay.ResourcePressure(), "should recover after usage drops below threshold")
	assert.Equal(t, storagestate.StatusReady, shard.get())

	// Step 3: Disk usage exceeds threshold again → READONLY again
	du = diskUse{total: 100, free: 5, avail: 5}
	db.resourceUseReadonly(mon, du)

	assert.True(t, db.nodeOverlay.ResourcePressure(), "should be readonly again after re-exceeding threshold")
	assert.Equal(t, storagestate.StatusReadOnly, shard.get())
}

func TestSetShardsReady_MultipleIndices(t *testing.T) {
	logger, _ := test.NewNullLogger()

	shard1 := newStatusShard(t, storagestate.StatusReadOnly)
	shard2 := newStatusShard(t, storagestate.StatusReadOnly)

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

	overlay := &clusterschema.NodeOverlay{}
	shard1.overlay, shard2.overlay = overlay, overlay

	db := &DB{
		localNodeName: "node1",
		logger:        logger,
		config: Config{
			ResourceUsage: config.ResourceUsage{
				DiskUse: config.DiskUse{ReadOnlyPercentage: 90},
				MemUse:  config.MemUse{ReadOnlyPercentage: 90},
			},
		},
		resourceScanState: newResourceScanState(),
		nodeOverlay:       overlay,
		indices: map[string]*Index{
			"Index1": idx1,
			"Index2": idx2,
		},
	}
	db.nodeOverlay.SetResourcePressure(true)

	db.setShardsReady()

	assert.False(t, db.nodeOverlay.ResourcePressure())
	assert.Equal(t, storagestate.StatusReady, shard1.get())
	assert.Equal(t, storagestate.StatusReady, shard2.get())
}

func TestSetShardsReady_PartialFailure(t *testing.T) {
	successShard := newStatusShard(t, storagestate.StatusReadOnly)

	failingShard := newStatusShard(t, storagestate.StatusReadOnly)
	failingShard.updateErr = fmt.Errorf("disk I/O error")

	db := testResourceDB(t, 90, 90, map[string]ShardLike{
		"success_shard": successShard,
		"failing_shard": failingShard,
	})
	db.nodeOverlay.SetResourcePressure(true)

	db.setShardsReady()

	assert.True(t, db.nodeOverlay.ResourcePressure(), "pressure should remain true when some shards fail to transition")
	assert.Equal(t, storagestate.StatusReady, successShard.get())
	assert.Equal(t, storagestate.StatusReadOnly, failingShard.get())
}

// Shards held read-only by memory pressure must go ready again once memory
// drops — the scan pass is the only thing refreshing the monitor.
func TestScanResourceUsageOnce_SeesMemoryDropWhileReadOnly(t *testing.T) {
	shard := newStatusShard(t, storagestate.StatusReadOnly)

	var used atomic.Int64
	used.Store(95)
	mon := memwatch.NewMonitor(used.Load, func(int64) int64 { return 100 }, 1.0)

	db := testResourceDB(t, 0, 90, map[string]ShardLike{"shard1": shard})
	db.nodeOverlay.SetResourcePressure(true)

	used.Store(10)
	db.scanResourceUsageOnce(mon, diskUse{total: 100, free: 100, avail: 100}, time.Now())

	assert.False(t, db.nodeOverlay.ResourcePressure(), "pressure should lift once memory drops")
	assert.Equal(t, storagestate.StatusReady, shard.get())
}

// The scan reads the mappings when MappingsReadDue says so.
func TestScanResourceUsageOnce_ReadsMappingsWhenDue(t *testing.T) {
	mon := memwatch.NewMonitor(func() int64 { return 0 }, func(int64) int64 { return 100 }, 1.0)
	db := testResourceDB(t, 0, 0, nil)
	due := time.Now().Add(time.Minute)
	require.True(t, mon.MappingsReadDue(due))

	db.scanResourceUsageOnce(mon, diskUse{total: 100, free: 100, avail: 100}, due)

	assert.False(t, mon.MappingsReadDue(due), "the scan should have read the mappings")
}

func TestWarnMappingsReadFailed(t *testing.T) {
	readErr := errors.New("too many open files")
	cases := []struct {
		name             string
		errs             []error // one read result per scan
		expectedWarnings int
	}{
		{name: "a successful read warns nothing", errs: []error{nil}, expectedWarnings: 0},
		{name: "a failed read warns", errs: []error{readErr}, expectedWarnings: 1},
		{name: "a second failure within the backoff warns nothing", errs: []error{readErr, readErr}, expectedWarnings: 1},
		{name: "a failure after a successful read warns again", errs: []error{readErr, nil, readErr}, expectedWarnings: 2},
		{name: "a success ends an incident of several failures", errs: []error{readErr, readErr, nil, readErr}, expectedWarnings: 2},
		{name: "a failure after the reset is throttled again", errs: []error{readErr, nil, readErr, readErr}, expectedWarnings: 2},
		{name: "every failure after a success warns", errs: []error{readErr, nil, readErr, nil, readErr}, expectedWarnings: 3},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			db := testResourceDB(t, 0, 0, nil)
			db.logger = logger

			for _, err := range tt.errs {
				db.warnMappingsReadFailed(err)
			}

			assert.Len(t, hook.AllEntries(), tt.expectedWarnings)
		})
	}
}
