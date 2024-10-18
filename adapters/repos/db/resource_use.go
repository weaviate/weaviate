//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"time"

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
				if !d.resourceScanState.isReadOnly {
					du := d.getDiskUse(d.config.RootPath)
					d.resourceUseWarn(d.memMonitor, du, updateMappings)
					d.resourceUseReadonly(d.memMonitor, du)
				}
				i += 1
			}
		}
	}
	enterrors.GoWrapper(f, d.logger)
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
func (db *DB) resourceUseWarn(mon *memwatch.Monitor, du diskUse, updateMappings bool) {
	mon.Refresh(updateMappings)
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
			db.setShardsReadOnly(fmt.Sprintf("disk usage too high. Set to read-only at %.2f%%, threshold set to %.2f%%\"", pu, float64(diskROPercent)))
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
			db.setShardsReadOnly(fmt.Sprintf("memory usage too high. Set to read-only at %.2f%%, threshold set to %.2f%%\"", pu, float64(memROPercent)))
			db.logger.WithField("action", "set_shard_read_only").
				WithField("path", db.config.RootPath).
				Warnf("Set READONLY, memory usage currently at %.2f%%, threshold set to %.2f%%",
					pu, float64(memROPercent))
		}
	}
}

func (db *DB) setShardsReadOnly(reason string) {
	db.indexLock.Lock()
	for _, index := range db.indices {
		index.ForEachShard(func(name string, shard ShardLike) error {
			err := shard.SetStatusReadonly(reason)
			if err != nil {
				db.logger.WithField("action", "set_shard_read_only").
					WithField("path", db.config.RootPath).
					WithError(err).
					Fatal("failed to set to READONLY")
			}
			return nil
		})
	}
	db.indexLock.Unlock()
	db.resourceScanState.isReadOnly = true
}
