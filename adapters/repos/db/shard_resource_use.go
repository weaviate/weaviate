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
	disk *scanState
	mem  *scanState
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
func (s *Shard) resourceUseWarn(mon *memwatch.Monitor, du diskUse) {
	s.diskUseWarn(du)
	s.memUseWarn(mon)
}

func (s *Shard) diskUseWarn(du diskUse) {
	diskWarnPercent := s.index.Config.ResourceUsage.DiskUse.WarningPercentage
	if diskWarnPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskWarnPercent) {
			if time.Since(s.resourceScanState.disk.lastWarning) >
				s.resourceScanState.disk.getWarningInterval() {
				s.index.logger.WithField("action", "read_disk_use").
					WithField("shard", s.name).
					WithField("path", s.index.Config.RootPath).
					Warnf("disk usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(diskWarnPercent))

				s.index.logger.WithField("action", "disk_use_stats").
					WithField("shard", s.name).
					WithField("path", s.index.Config.RootPath).
					Debugf("%s", du.String())

				s.resourceScanState.disk.lastWarning = time.Now()
				s.resourceScanState.disk.increaseWarningInterval()
			}
		}
	}
}

func (s *Shard) memUseWarn(mon *memwatch.Monitor) {
	memWarnPercent := s.index.Config.ResourceUsage.MemUse.WarningPercentage
	if memWarnPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memWarnPercent) {
			if time.Since(s.resourceScanState.mem.lastWarning) >
				s.resourceScanState.mem.getWarningInterval() {
				s.index.logger.WithField("action", "read_memory_use").
					WithField("shard", s.name).
					WithField("path", s.index.Config.RootPath).
					Warnf("memory usage currently at %.2f%%, threshold set to %.2f%%",
						pu, float64(memWarnPercent))

				s.resourceScanState.mem.lastWarning = time.Now()
				s.resourceScanState.mem.increaseWarningInterval()
			}
		}
	}
}

// sets the shard to readonly if user-set threshold is surpassed
func (s *Shard) resourceUseReadonly(mon *memwatch.Monitor, du diskUse) {
	s.diskUseReadonly(du)
	s.memUseReadonly(mon)
}

func (s *Shard) diskUseReadonly(du diskUse) {
	diskROPercent := s.index.Config.ResourceUsage.DiskUse.ReadOnlyPercentage
	if diskROPercent > 0 {
		if pu := du.percentUsed(); pu > float64(diskROPercent) {
			err := s.updateStatus(storagestate.StatusReadOnly.String())
			if err != nil {
				s.index.logger.WithField("action", "set_shard_read_only").
					WithField("shard", s.name).
					WithField("path", s.index.Config.RootPath).
					Fatal("failed to set to READONLY")
			}

			s.index.logger.WithField("action", "set_shard_read_only").
				WithField("shard", s.name).
				WithField("path", s.index.Config.RootPath).
				Warnf("%s set READONLY, disk usage currently at %.2f%%, threshold set to %.2f%%",
					s.name, pu, float64(diskROPercent))
		}
	}
}

func (s *Shard) memUseReadonly(mon *memwatch.Monitor) {
	memROPercent := s.index.Config.ResourceUsage.MemUse.ReadOnlyPercentage
	if memROPercent > 0 {
		if pu := mon.Ratio() * 100; pu > float64(memROPercent) {
			err := s.updateStatus(storagestate.StatusReadOnly.String())
			if err != nil {
				s.index.logger.WithField("action", "set_shard_read_only").
					WithField("shard", s.name).
					WithField("path", s.index.Config.RootPath).
					Fatal("failed to set to READONLY")
			}

			s.index.logger.WithField("action", "set_shard_read_only").
				WithField("shard", s.name).
				WithField("path", s.index.Config.RootPath).
				Warnf("%s set READONLY, memory usage currently at %.2f%%, threshold set to %.2f%%",
					s.name, pu, float64(memROPercent))
		}
	}
}
