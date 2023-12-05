//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"time"

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
	go func() {
		t := time.NewTicker(time.Millisecond * 500)
		defer t.Stop()
		for {
			select {
			case <-d.shutdown:
				return
			case <-t.C:
				if !d.resourceScanState.isReadOnly {
					du := d.getDiskUse(d.config.RootPath)
					d.resourceUseWarn(d.memMonitor, du)
					d.resourceUseReadonly(d.memMonitor, du)
				}
			}
		}
	}()
}

type resourceScanState struct {
	disk       *scanState
	mem        *scanState
	isReadOnly bool
}

type scanState struct {
	backoffLevel int
	backoffs     []time.Duration
	lastWarning  time.Time
}

func (s *scanState) getWarningInterval() time.Duration {
	if s.backoffLevel >= len(s.backoffs) {
		return time.Hour * 24
	}

	interval := s.backoffs[s.backoffLevel]

	return interval
}

func (s *scanState) increaseWarningInterval() {
	if s.backoffLevel < len(s.backoffs) {
		s.backoffLevel += 1
	}
}

func newResourceScanState() *resourceScanState {
	backoffs := []time.Duration{
		time.Duration(0),
		30 * time.Second,
		2 * time.Minute,
		10 * time.Minute,
		1 * time.Hour,
		12 * time.Hour,
	}

	return &resourceScanState{
		disk: &scanState{backoffs: backoffs},
		mem:  &scanState{backoffs: backoffs},
	}
}

// logs a warning if user-set threshold is surpassed
func (db *DB) resourceUseWarn(mon *memwatch.Monitor, du diskUse) {
	mon.Refresh()
	db.diskUseWarn(du)
	db.memUseWarn(mon)
}

func (db *DB) diskUseWarn(du diskUse) {
	diskWarnPercent := db.config.ResourceUsage.DiskUse.WarningPercentage
	if diskWarnPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskWarnPercent) {
			if time.Since(db.resourceScanState.disk.lastWarning) >
				db.resourceScanState.disk.getWarningInterval() {
				db.logger.WithField("action", "read_disk_use").
					WithField("path", db.config.RootPath).
					Warnf("disk usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(diskWarnPercent))

				db.logger.WithField("action", "disk_use_stats").
					WithField("path", db.config.RootPath).
					Debugf("%s", du.String())

				db.resourceScanState.disk.lastWarning = time.Now()
				db.resourceScanState.disk.increaseWarningInterval()
			}
		}
	}
}

func (db *DB) memUseWarn(mon *memwatch.Monitor) {
	memWarnPercent := db.config.ResourceUsage.MemUse.WarningPercentage
	if memWarnPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memWarnPercent) {
			if time.Since(db.resourceScanState.mem.lastWarning) >
				db.resourceScanState.mem.getWarningInterval() {
				db.logger.WithField("action", "read_memory_use").
					WithField("path", db.config.RootPath).
					Warnf("memory usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(memWarnPercent))

				db.resourceScanState.mem.lastWarning = time.Now()
				db.resourceScanState.mem.increaseWarningInterval()
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
	db.indexLock.Lock()
	for _, index := range db.indices {
		index.ForEachShard(func(name string, shard ShardLike) error {
			err := shard.UpdateStatus(storagestate.StatusReadOnly.String())
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
