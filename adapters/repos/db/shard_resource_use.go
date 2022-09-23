//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"time"

	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/semi-technologies/weaviate/usecases/memwatch"
)

type resourceScanState struct {
	backoffLevel int
	backoffs     []time.Duration
	lastWarning  time.Time
}

func (d *resourceScanState) getWarningInterval() time.Duration {
	if d.backoffLevel >= len(d.backoffs) {
		return time.Hour * 24
	}

	interval := d.backoffs[d.backoffLevel]

	return interval
}

func (d *resourceScanState) increaseWarningInterval() {
	if d.backoffLevel < len(d.backoffs) {
		d.backoffLevel += 1
	}
}

func newResourceScanState() *resourceScanState {
	return &resourceScanState{
		backoffs: []time.Duration{
			time.Duration(0),
			30 * time.Second,
			2 * time.Minute,
			10 * time.Minute,
			1 * time.Hour,
			12 * time.Hour,
		},
	}
}

// logs a warning if user-set threshold is surpassed
func (s *Shard) resourceUseWarn(mon *memwatch.Monitor, du diskUse, diskPath string) {
	diskWarnPercent := s.index.Config.ResourceUsage.DiskUse.WarningPercentage
	memWarnPercent := s.index.Config.ResourceUsage.MemUse.WarningPercentage

	if diskWarnPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskWarnPercent) {
			if !s.isReadOnly() && time.Since(s.diskScanState.lastWarning) >
				s.diskScanState.getWarningInterval() {
				s.index.logger.WithField("action", "read_disk_use").
					WithField("shard", s.name).
					WithField("path", diskPath).
					Warnf("disk usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(diskWarnPercent))

				s.index.logger.WithField("action", "disk_use_stats").
					WithField("shard", s.name).
					WithField("path", diskPath).
					Debugf("%s", du.String())

				s.diskScanState.lastWarning = time.Now()
				s.diskScanState.increaseWarningInterval()
			}
		}
	}

	if memWarnPercent > 0 {
		if ratio := mon.Ratio(); ratio > float64(memWarnPercent) {
			if !s.isReadOnly() && time.Since(s.diskScanState.lastWarning) >
				s.diskScanState.getWarningInterval() {
				s.index.logger.WithField("action", "read_memory_use").
					WithField("shard", s.name).
					WithField("path", diskPath).
					Warnf("memory usage currently at %.2f%%, threshold set to %.2f%%",
						ratio, float64(memWarnPercent))

				s.diskScanState.lastWarning = time.Now()
				s.diskScanState.increaseWarningInterval()
			}
		}
	}
}

// sets the shard to readonly if user-set threshold is surpassed
func (s *Shard) resourceUseReadonly(mon *memwatch.Monitor, du diskUse, diskPath string) {
	diskROPercent := s.index.Config.ResourceUsage.DiskUse.ReadOnlyPercentage
	memROPercent := s.index.Config.ResourceUsage.MemUse.ReadOnlyPercentage

	if diskROPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskROPercent) {
			if !s.isReadOnly() {
				err := s.updateStatus(storagestate.StatusReadOnly.String())
				if err != nil {
					s.index.logger.WithField("action", "set_shard_read_only").
						WithField("shard", s.name).
						WithField("path", s.index.Config.RootPath).
						Fatal("failed to set to READONLY")
				}

				s.index.logger.WithField("action", "set_shard_read_only").
					WithField("shard", s.name).
					WithField("path", diskPath).
					Warnf("disk usage currently at %.2f%%, %s set READONLY, threshold set to %.2f%%",
						pu, s.name, float64(diskROPercent))
			}
		}
	}

	if memROPercent > 0 {
		if ratio := mon.Ratio(); ratio > float64(memROPercent) {
			if !s.isReadOnly() {
				err := s.updateStatus(storagestate.StatusReadOnly.String())
				if err != nil {
					s.index.logger.WithField("action", "set_shard_read_only").
						WithField("shard", s.name).
						WithField("path", s.index.Config.RootPath).
						Fatal("failed to set to READONLY")
				}

				s.index.logger.WithField("action", "set_shard_read_only").
					WithField("shard", s.name).
					WithField("path", diskPath).
					Warnf("memory usage currently at %.2f%%, %s set READONLY, threshold set to %.2f%%",
						ratio, s.name, float64(memROPercent))
			}
		}
	}
}
