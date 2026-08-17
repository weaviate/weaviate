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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type diskUse struct {
	total uint64
	free  uint64
	avail uint64
}

func (d diskUse) percentUsed() float64 {
	used := d.total - d.free
	return (float64(used) / float64(d.total)) * 100
}

func (d diskUse) String() string {
	GB := 1024 * 1024 * 1024

	return fmt.Sprintf("total: %.2fGB, free: %.2fGB, used: %.2fGB (avail: %.2fGB)",
		float64(d.total)/float64(GB),
		float64(d.free)/float64(GB),
		float64(d.total-d.free)/float64(GB),
		float64(d.avail)/float64(GB))
}

func (d *DB) scanResourceUsage() {
	f := func() {
		t := time.NewTicker(time.Millisecond * 500)
		i := 0
		defer t.Stop()
		for {
			select {
			case <-d.shutdown:
				return
			case <-t.C:
				updateMappings := i%(memwatch.MappingDelayInS*2) == 0
				du := d.getDiskUse(d.config.RootPath)
				d.scanResourceUsageOnce(d.memMonitor, du, updateMappings)
				i += 1
			}
		}
	}
	enterrors.GoWrapper(f, d.logger)
}

// scanResourceUsageOnce runs a single scan pass. The monitor is refreshed here
// because both branches read from it: without it, shards held read-only by
// memory pressure would never see the usage drop back below the threshold.
func (db *DB) scanResourceUsageOnce(mon *memwatch.Monitor, du diskUse, updateMappings bool) {
	mon.Refresh(updateMappings)
	if db.resourceScanState.isReadOnly.Load() {
		db.resourceUseRecovery(mon, du)
	} else {
		db.resourceUseWarn(mon, du)
		db.resourceUseReadonly(mon, du)
	}
}

type resourceScanState struct {
	diskWarning *interval.BackoffTimer
	memWarning  *interval.BackoffTimer

	// transition is held for write only while isReadOnly is flipped, never
	// across a sweep - a sweep takes indexLock and the shards' own locks. A
	// shard settling under the read lock therefore cannot straddle the flip.
	transition sync.RWMutex

	// isReadOnly reports whether the scan currently holds shards read-only.
	// Written by the scan goroutine, read by every shard that is built: a shard
	// created or loaded while this is set comes up READONLY instead of READY.
	// See [Shard.inheritResourcePressureReadOnly].
	isReadOnly atomic.Bool
}

// resourcePressureReadOnly reports whether the resource scan currently holds
// shards read-only. Tolerates a nil DB and a nil scan state: a shard built
// inside [NewIndex] has no owning DB to read yet, and picks the flag up from
// [DB.reconcileIndexResourcePressure] once the index is published.
func (db *DB) resourcePressureReadOnly() bool {
	return db != nil && db.resourceScanState != nil && db.resourceScanState.isReadOnly.Load()
}

// setReadOnlyFlag flips the flag deciding whether a shard comes up READONLY
// when it is built.
func (db *DB) setReadOnlyFlag(readOnly bool) {
	db.resourceScanState.transition.Lock()
	defer db.resourceScanState.transition.Unlock()

	db.resourceScanState.isReadOnly.Store(readOnly)
}

func newResourceScanState() *resourceScanState {
	return &resourceScanState{
		diskWarning: interval.NewBackoffTimer(),
		memWarning:  interval.NewBackoffTimer(),
	}
}

// logs a warning if user-set threshold is surpassed
func (db *DB) resourceUseWarn(mon *memwatch.Monitor, du diskUse) {
	db.diskUseWarn(du)
	db.memUseWarn(mon)
}

func (db *DB) diskUseWarn(du diskUse) {
	diskWarnPercent := db.config.ResourceUsage.DiskUse.WarningPercentage
	if diskWarnPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskWarnPercent) {
			if db.resourceScanState.diskWarning.IntervalElapsed() {
				db.logger.WithField("action", "read_disk_use").
					WithField("path", db.config.RootPath).
					Warnf("disk usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(diskWarnPercent))

				db.logger.WithField("action", "disk_use_stats").
					WithField("path", db.config.RootPath).
					Debugf("%s", du.String())
				db.resourceScanState.diskWarning.IncreaseInterval()
			}
		}
	}
}

func (db *DB) memUseWarn(mon *memwatch.Monitor) {
	memWarnPercent := db.config.ResourceUsage.MemUse.WarningPercentage
	if memWarnPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memWarnPercent) {
			if db.resourceScanState.memWarning.IntervalElapsed() {
				db.logger.WithField("action", "read_memory_use").
					WithField("path", db.config.RootPath).
					Warnf("memory usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(memWarnPercent))
				db.resourceScanState.memWarning.IncreaseInterval()
			}
		}
	}
}

// sets the shard to readonly if user-set threshold is surpassed
func (db *DB) resourceUseReadonly(mon *memwatch.Monitor, du diskUse) {
	db.diskUseReadonly(du)
	db.memUseReadonly(mon)
}

func (db *DB) diskUseReadonly(du diskUse) {
	diskROPercent := db.config.ResourceUsage.DiskUse.ReadOnlyPercentage
	if diskROPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskROPercent) {
			db.setShardsReadOnly(fmt.Sprintf("disk usage too high. Set to read-only at %.2f%%, threshold set to %.2f%%", pu, float64(diskROPercent)))
			db.logger.WithField("action", "set_shard_read_only").
				WithField("path", db.config.RootPath).
				Warnf("Set READONLY, disk usage currently at %.2f%%, threshold set to %.2f%%",
					pu, float64(diskROPercent))
		}
	}
}

func (db *DB) memUseReadonly(mon *memwatch.Monitor) {
	memROPercent := db.config.ResourceUsage.MemUse.ReadOnlyPercentage
	if memROPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memROPercent) {
			db.setShardsReadOnly(fmt.Sprintf("memory usage too high. Set to read-only at %.2f%%, threshold set to %.2f%%", pu, float64(memROPercent)))
			db.logger.WithField("action", "set_shard_read_only").
				WithField("path", db.config.RootPath).
				Warnf("Set READONLY, memory usage currently at %.2f%%, threshold set to %.2f%%",
					pu, float64(memROPercent))
		}
	}
}

func (db *DB) setShardsReadOnly(reason string) {
	// Raise the flag before the sweep, never after: a lazily loading shard reads
	// it while holding the load lock the sweep needs to see that shard as loaded,
	// so the two are mutually exclusive. An eagerly built shard holds no such
	// lock, and reconciles against the flag when it is published instead.
	db.setReadOnlyFlag(true)

	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	for _, index := range db.indices {
		// Loaded shards only: force-loading a cold shard here costs the memory the
		// scan may be reacting to, and a failed load panics out of the scan
		// goroutine. Cold shards inherit the flag when they load instead.
		index.ForEachLoadedShard(func(name string, shard ShardLike) error {
			db.markShardReadOnly(name, shard)
			return nil
		})
	}
}

// markShardReadOnly holds one loaded shard read-only for resource pressure.
//
// A shard that is already read-only keeps its reason: it may be read-only for a
// non-resource reason (e.g. a vector-index config update), and relabeling it
// would let the recovery pass flip it back to READY mid-operation.
func (db *DB) markShardReadOnly(name string, shard ShardLike) {
	if shard.GetStatus() == storagestate.StatusReadOnly {
		return
	}
	// A shard whose store closed concurrently (tenant deletion, deactivation,
	// shutdown) is going away and takes no more writes.
	err := shard.SetStatusReadonly(statusReasonResourcePressure)
	if err != nil && !errors.Is(err, lsmkv.ErrAlreadyClosed) {
		db.logger.WithField("action", "set_shard_read_only").
			WithField("path", db.config.RootPath).
			Errorf("failed to set to READONLY: shard %q: %v", name, err)
	}
}

// markShardReady releases one shard from resource-pressure read-only, leaving a
// shard held read-only for any other reason alone. Reports whether the release
// succeeded.
func (db *DB) markShardReady(name string, shard ShardLike) bool {
	if shard.GetStatus() != storagestate.StatusReadOnly ||
		shard.GetStatusReason() != statusReasonResourcePressure {
		return true
	}
	// A shard whose store closed concurrently (tenant deletion, deactivation,
	// shutdown) takes no writes either way, and comes back READY if it is ever
	// loaded again.
	err := shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonResourceRecovery)
	if err != nil && !errors.Is(err, lsmkv.ErrAlreadyClosed) {
		db.logger.WithField("action", "set_shard_ready").
			WithField("path", db.config.RootPath).
			Errorf("failed to set to READY: shard %q: %v", name, err)
		return false
	}
	return true
}

// reconcileShardResourcePressure applies the current read-only flag to a shard
// the caller has just published. Publishing first is required: it is what lets
// a transition that wins the lock find the shard in the sweep that follows.
//
// A cold shard is skipped - a status change would force it to load, and it
// reads the flag itself when it does.
func (db *DB) reconcileShardResourcePressure(name string, shard ShardLike) {
	if db == nil || db.resourceScanState == nil || !shardIsLoaded(shard) {
		return
	}

	db.resourceScanState.transition.RLock()
	defer db.resourceScanState.transition.RUnlock()

	if db.resourceScanState.isReadOnly.Load() {
		db.markShardReadOnly(name, shard)
		return
	}
	db.markShardReady(name, shard)
}

// reconcileIndexResourcePressure applies the current read-only flag to the
// loaded shards of an index the caller has just published. Shards built inside
// [NewIndex] are out of the scan's reach - the index is not in db.indices for
// its sweep, and they have no owning DB to read the flag from - so this is
// where they pick it up.
func (db *DB) reconcileIndexResourcePressure(index *Index) {
	index.ForEachLoadedShard(func(name string, shard ShardLike) error {
		db.reconcileShardResourcePressure(name, shard)
		return nil
	})
}

// resourceUseRecovery checks whether resource usage has dropped below the
// configured thresholds and, if so, transitions all READONLY shards back to
// READY.  Both disk and memory must be below their respective thresholds (or
// the threshold must be disabled, i.e. set to 0) for recovery to trigger.
func (db *DB) resourceUseRecovery(mon *memwatch.Monitor, du diskUse) {
	if db.diskAboveReadonlyThreshold(du) || db.memAboveReadonlyThreshold(mon) {
		return
	}
	db.setShardsReady()
}

func (db *DB) diskAboveReadonlyThreshold(du diskUse) bool {
	p := db.config.ResourceUsage.DiskUse.ReadOnlyPercentage
	return p > 0 && du.percentUsed() > float64(p)
}

func (db *DB) memAboveReadonlyThreshold(mon *memwatch.Monitor) bool {
	p := db.config.ResourceUsage.MemUse.ReadOnlyPercentage
	return p > 0 && (mon.Ratio()*100) > float64(p)
}

func (db *DB) setShardsReady() {
	// Drop the flag before the sweep, never after: a shard loading concurrently
	// would otherwise inherit READONLY after the sweep already passed it, and
	// nothing would flip it back - this recovery pass only runs while the flag
	// is set. It goes back up below if any shard failed to transition.
	db.setReadOnlyFlag(false)

	var failedCount atomic.Int64
	func() {
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		for _, index := range db.indices {
			index.ForEachShardConcurrently(func(name string, shard ShardLike) error {
				if !db.markShardReady(name, shard) {
					failedCount.Add(1)
				}
				return nil
			})
		}
	}()

	if count := failedCount.Load(); count > 0 {
		db.setReadOnlyFlag(true)
		db.logger.WithField("action", "set_shard_ready").
			WithField("failed_count", count).
			Warn("Resource usage below threshold, but some shards failed to transition to READY")
		return
	}
	db.logger.WithField("action", "set_shard_ready").
		Info("Resource usage below threshold. Set shards back to READY")
}
