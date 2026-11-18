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
	"time"

	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/entities/interval"
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

// DiskUse reports the total and free bytes of the disk holding the data root.
func (db *DB) DiskUse() (total, free uint64) {
	du := db.getDiskUse(db.config.RootPath)
	return du.total, du.free
}

// resourceScanInterval is how often scanResourceUsage checks disk and memory use.
const resourceScanInterval = 500 * time.Millisecond

func (d *DB) scanResourceUsage() {
	f := func() {
		t := time.NewTicker(resourceScanInterval)
		defer t.Stop()
		for {
			select {
			case <-d.shutdown:
				return
			case now := <-t.C:
				du := d.getDiskUse(d.config.RootPath)
				d.scanResourceUsageOnce(d.memMonitor, du, now)
			}
		}
	}
	enterrors.GoWrapper(f, d.logger)
}

// scanResourceUsageOnce runs a single scan pass. The monitor is refreshed here
// because both branches read from it: without it, shards held read-only by
// memory pressure would never see the usage drop back below the threshold.
func (db *DB) scanResourceUsageOnce(mon *memwatch.Monitor, du diskUse, now time.Time) {
	mon.Refresh(false)
	if mon.MappingsReadDue(now) {
		db.warnMappingsReadFailed(mon.ReadMappings(now))
	}
	if db.resourcePressureReadOnly() {
		db.resourceUseRecovery(mon, du)
	} else {
		db.resourceUseWarn(mon, du)
		db.resourceUseReadonly(mon, du)
	}
}

type resourceScanState struct {
	diskWarning     *interval.BackoffTimer
	memWarning      *interval.BackoffTimer
	mappingsWarning *interval.BackoffTimer
}

// resourcePressureReadOnly reports whether the resource scan currently holds
// shards read-only. Every index gets its owning DB in [NewIndex], before it
// builds a single shard, so a shard always reads the live flag; the nil
// tolerance here covers indices assembled by hand in tests.
func (db *DB) resourcePressureReadOnly() bool {
	return db != nil && db.overlay().Node()&clusterSchema.ConditionResourcePressure != 0
}

// setReadOnlyFlag sets resource pressure in the node overlay, which drives every loaded shard from it. A
// cold shard is left alone and comes up under the current pressure when it loads.
func (db *DB) setReadOnlyFlag(readOnly bool) {
	db.overlay().SetNode(clusterSchema.ConditionResourcePressure, readOnly)
}

func newResourceScanState() *resourceScanState {
	return &resourceScanState{
		diskWarning:     interval.NewBackoffTimer(),
		memWarning:      interval.NewBackoffTimer(),
		mappingsWarning: interval.NewBackoffTimer(),
	}
}

// warnMappingsReadFailed logs err from Monitor.ReadMappings, with a backoff. A
// successful read resets the backoff, so the next failure warns at once.
func (db *DB) warnMappingsReadFailed(err error) {
	warning := db.resourceScanState.mappingsWarning
	if err == nil {
		warning.Reset()
		return
	}
	if !warning.IntervalElapsed() {
		return
	}
	db.logger.WithField("action", "read_memory_mappings").
		Warnf("cannot count memory mappings, shard loads are checked against the last count: %v", err)
	warning.IncreaseInterval()
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
			db.setShardsReadOnly()
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
			db.setShardsReadOnly()
			db.logger.WithField("action", "set_shard_read_only").
				WithField("path", db.config.RootPath).
				Warnf("Set READONLY, memory usage currently at %.2f%%, threshold set to %.2f%%",
					pu, float64(memROPercent))
		}
	}
}

func (db *DB) setShardsReadOnly() {
	db.setReadOnlyFlag(true)
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
	db.setReadOnlyFlag(false)

	db.logger.WithField("action", "set_shard_ready").
		Info("Resource usage below threshold. Set shards back to READY")
}
