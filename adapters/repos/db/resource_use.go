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

	// isReadOnly reports whether the scan currently holds shards read-only.
	// Written by the scan goroutine, read by every shard that is built: a shard
	// created or loaded while this is set comes up READONLY instead of READY.
	// See [Shard.inheritResourcePressureReadOnly].
	isReadOnly atomic.Bool
}

// resourcePressureReadOnly reports whether the resource scan currently holds
// shards read-only. Tolerates a nil DB and a nil scan state, both of which
// occur in tests that build an Index without its owning DB.
func (db *DB) resourcePressureReadOnly() bool {
	return db != nil && db.resourceScanState != nil && db.resourceScanState.isReadOnly.Load()
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
	// Raise the flag before the sweep, never after: a shard loading concurrently
	// reads it while holding the load lock the sweep needs to see that shard as
	// loaded, so flag-then-sweep means every shard is caught by exactly one of
	// the two - the sweep if it was already loaded, the flag if it loads later.
	db.resourceScanState.isReadOnly.Store(true)

	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	for _, index := range db.indices {
		// Loaded shards only: force-loading a cold shard here costs the memory the
		// scan may be reacting to, and a failed load panics out of the scan
		// goroutine. Cold shards inherit the flag when they load instead.
		index.ForEachLoadedShard(func(name string, shard ShardLike) error {
			// Don't overwrite the reason of an already read-only shard: it may be
			// read-only for a non-resource reason (e.g. a vector-index config
			// update), and relabeling it would let setShardsReady flip it back to
			// READY mid-operation.
			if shard.GetStatus() == storagestate.StatusReadOnly {
				return nil
			}
			// A shard whose store closed concurrently (tenant deletion,
			// deactivation, shutdown) is going away and takes no more writes.
			err := shard.SetStatusReadonly(statusReasonResourcePressure)
			if err != nil && !errors.Is(err, lsmkv.ErrAlreadyClosed) {
				db.logger.WithField("action", "set_shard_read_only").
					WithField("path", db.config.RootPath).
					Errorf("failed to set to READONLY: shard %q: %v", name, err)
			}
			return nil
		})
	}
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
	db.resourceScanState.isReadOnly.Store(false)

	var failedCount atomic.Int64
	func() {
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		for _, index := range db.indices {
			index.ForEachShardConcurrently(func(name string, shard ShardLike) error {
				if shard.GetStatus() == storagestate.StatusReadOnly &&
					shard.GetStatusReason() == statusReasonResourcePressure {
					err := shard.UpdateStatus(storagestate.StatusReady.String(), statusReasonResourceRecovery)
					// A shard whose store closed concurrently (tenant deletion,
					// deactivation, shutdown) takes no writes either way, and comes
					// back READY if it is ever loaded again.
					if err != nil && !errors.Is(err, lsmkv.ErrAlreadyClosed) {
						failedCount.Add(1)
						db.logger.WithField("action", "set_shard_ready").
							WithField("path", db.config.RootPath).
							Errorf("failed to set to READY: shard %q: %v", name, err)
					}
				}
				return nil
			})
		}
	}()

	if count := failedCount.Load(); count > 0 {
		db.resourceScanState.isReadOnly.Store(true)
		db.logger.WithField("action", "set_shard_ready").
			WithField("failed_count", count).
			Warn("Resource usage below threshold, but some shards failed to transition to READY")
		return
	}
	db.logger.WithField("action", "set_shard_ready").
		Info("Resource usage below threshold. Set shards back to READY")
}
