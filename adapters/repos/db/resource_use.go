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
	"runtime"
	"runtime/debug"
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
	memMonitor := memwatch.NewMonitor(
		runtime.MemProfile, debug.SetMemoryLimit, runtime.MemProfileRate)

	go func() {
		t := time.NewTicker(time.Second * 30)
		defer t.Stop()
		for {
			select {
			case <-d.shutdown:
				return
			case <-t.C:
				if !d.resourceScanState.isReadOnly {
					du := d.getDiskUse(d.config.RootPath)
					d.resourceUseWarn(memMonitor, du)
					d.resourceUseReadonly(memMonitor, du)
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
func (d *DB) resourceUseWarn(mon *memwatch.Monitor, du diskUse) {
	d.diskUseWarn(du)
	d.memUseWarn(mon)
}

func (d *DB) diskUseWarn(du diskUse) {
	diskWarnPercent := d.config.ResourceUsage.DiskUse.WarningPercentage
	if diskWarnPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskWarnPercent) {
			if time.Since(d.resourceScanState.disk.lastWarning) >
				d.resourceScanState.disk.getWarningInterval() {
				d.logger.WithField("action", "read_disk_use").
					WithField("path", d.config.RootPath).
					Warnf("disk usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(diskWarnPercent))

				d.logger.WithField("action", "disk_use_stats").
					WithField("path", d.config.RootPath).
					Debugf("%s", du.String())

				d.resourceScanState.disk.lastWarning = time.Now()
				d.resourceScanState.disk.increaseWarningInterval()
			}
		}
	}
}

func (d *DB) memUseWarn(mon *memwatch.Monitor) {
	memWarnPercent := d.config.ResourceUsage.MemUse.WarningPercentage
	if memWarnPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memWarnPercent) {
			if time.Since(d.resourceScanState.mem.lastWarning) >
				d.resourceScanState.mem.getWarningInterval() {
				d.logger.WithField("action", "read_memory_use").
					WithField("path", d.config.RootPath).
					Warnf("memory usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(memWarnPercent))

				d.resourceScanState.mem.lastWarning = time.Now()
				d.resourceScanState.mem.increaseWarningInterval()
			}
		}
	}
}

// sets the shard to readonly if user-set threshold is surpassed
func (d *DB) resourceUseReadonly(mon *memwatch.Monitor, du diskUse) {
	d.diskUseReadonly(du)
	d.memUseReadonly(mon)
}

func (d *DB) diskUseReadonly(du diskUse) {
	diskROPercent := d.config.ResourceUsage.DiskUse.ReadOnlyPercentage
	if diskROPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskROPercent) {
			d.setShardsReadOnly()
			d.logger.WithField("action", "set_shard_read_only").
				WithField("path", d.config.RootPath).
				Warnf("Set READONLY, disk usage currently at %.2f%%, threshold set to %.2f%%",
					pu, float64(diskROPercent))
		}
	}
}

func (d *DB) memUseReadonly(mon *memwatch.Monitor) {
	memROPercent := d.config.ResourceUsage.MemUse.ReadOnlyPercentage
	if memROPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memROPercent) {
			d.setShardsReadOnly()
			d.logger.WithField("action", "set_shard_read_only").
				WithField("path", d.config.RootPath).
				Warnf("Set READONLY, memory usage currently at %.2f%%, threshold set to %.2f%%",
					pu, float64(memROPercent))
		}
	}
}

func (d *DB) setShardsReadOnly() {
	d.indexLock.Lock()
	for _, index := range d.indices {
		index.ForEachShard(func(name string, shard *Shard) error {
			err := shard.updateStatus(storagestate.StatusReadOnly.String())
			if err != nil {
				d.logger.WithField("action", "set_shard_read_only").
					WithField("path", d.config.RootPath).
					WithError(err).
					Fatal("failed to set to READONLY")
			}
			return nil
		})
	}
	d.indexLock.Unlock()
	d.resourceScanState.isReadOnly = true
}
