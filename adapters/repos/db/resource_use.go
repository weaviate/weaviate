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
	"fmt"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

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
// because every pass below reads from it: without it, shards held read-only by
// memory pressure would never see the usage drop back below the threshold.
func (db *DB) scanResourceUsageOnce(mon *memwatch.Monitor, du diskUse, updateMappings bool) {
	mon.Refresh(updateMappings)

	if db.resourceScanState.isReadOnly {
		db.resourceUseRecovery(mon, du)
	} else {
		db.resourceUseWarn(mon, du)
	}

	// The read-only pass runs on every tick, not only on the one that crosses
	// the threshold. It leaves shards that are not loaded alone, so a shard that
	// loads later in the episode — a tenant that gets activated, a shard created
	// after the crossing — is READY when it comes up and takes writes on a node
	// that is over its limit until a pass reaches it. Repeating the pass bounds
	// that window by the tick interval instead of the length of the episode. The
	// pass writes only to shards that are not already read-only, and no longer
	// loads cold ones, so repeating it costs a status read per loaded shard.
	db.resourceUseReadonly(mon, du)
}

type resourceScanState struct {
	diskWarning *interval.BackoffTimer
	memWarning  *interval.BackoffTimer
	isReadOnly  bool
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
			if db.setShardsReadOnly(fmt.Sprintf("disk usage too high. Set to read-only at %.2f%%, threshold set to %.2f%%", pu, float64(diskROPercent))) {
				db.logger.WithField("action", "set_shard_read_only").
					WithField("path", db.config.RootPath).
					Warnf("Set READONLY, disk usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(diskROPercent))
			}
		}
	}
}

func (db *DB) memUseReadonly(mon *memwatch.Monitor) {
	memROPercent := db.config.ResourceUsage.MemUse.ReadOnlyPercentage
	if memROPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memROPercent) {
			if db.setShardsReadOnly(fmt.Sprintf("memory usage too high. Set to read-only at %.2f%%, threshold set to %.2f%%", pu, float64(memROPercent))) {
				db.logger.WithField("action", "set_shard_read_only").
					WithField("path", db.config.RootPath).
					Warnf("Set READONLY, memory usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(memROPercent))
			}
		}
	}
}

// setShardsReadOnly sets every loaded shard that is not already read-only to
// READONLY. It reports whether this is the pass that put the node into
// read-only, so the caller logs the crossing once instead of on every tick of
// the episode.
func (db *DB) setShardsReadOnly(reason string) bool {
	// Don't overwrite the reason of an already read-only shard: it may be
	// read-only for a non-resource reason (e.g. a vector-index config update),
	// and relabeling it would let setShardsReady flip it back to READY
	// mid-operation.
	notReadOnly := func(current ShardStatus) bool {
		return current.Status != storagestate.StatusReadOnly
	}

	db.indexLock.Lock()
	for _, index := range db.indices {
		index.ForEachShard(func(name string, shard ShardLike) error {
			err := shard.UpdateStatusIf(notReadOnly,
				storagestate.StatusReadOnly.String(), statusReasonResourcePressure)
			if err != nil {
				db.logger.WithField("action", "set_shard_read_only").
					WithField("path", db.config.RootPath).
					Fatalf("failed to set to READONLY: shard %q: %v", name, err)
			}
			return nil
		})
	}
	db.indexLock.Unlock()

	entered := !db.resourceScanState.isReadOnly
	db.resourceScanState.isReadOnly = true
	return entered
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
	// Only recover what this scanner made read-only; any other reason (a manual
	// freeze, a vector-index config update) must survive the recovery pass.
	heldByResourcePressure := func(current ShardStatus) bool {
		return current.Status == storagestate.StatusReadOnly &&
			current.Reason == statusReasonResourcePressure
	}

	var failedCount atomic.Int64
	func() {
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		for _, index := range db.indices {
			index.ForEachShardConcurrently(func(name string, shard ShardLike) error {
				err := shard.UpdateStatusIf(heldByResourcePressure,
					storagestate.StatusReady.String(), statusReasonResourceRecovery)
				if err != nil {
					failedCount.Add(1)
					db.logger.WithField("action", "set_shard_ready").
						WithField("path", db.config.RootPath).
						Errorf("failed to set to READY: shard %q: %v", name, err)
				}
				return nil
			})
		}
	}()

	if count := failedCount.Load(); count > 0 {
		db.logger.WithField("action", "set_shard_ready").
			WithField("failed_count", count).
			Warn("Resource usage below threshold, but some shards failed to transition to READY")
		return
	}
	db.resourceScanState.isReadOnly = false
	db.logger.WithField("action", "set_shard_ready").
		Info("Resource usage below threshold. Set shards back to READY")
}
