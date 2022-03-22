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
)

type diskScanState struct {
	backoffLevel int
	backoffs     []time.Duration
	lastWarning  time.Time
}

func (d *diskScanState) getWarningInterval() time.Duration {
	if d.backoffLevel >= len(d.backoffs) {
		return time.Hour * 24
	}

	interval := d.backoffs[d.backoffLevel]

	return interval
}

func (d *diskScanState) increaseWarningInterval() {
	if d.backoffLevel < len(d.backoffs) {
		d.backoffLevel += 1
	}
}

func newDiskScanState() *diskScanState {
	return &diskScanState{
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
func (s *Shard) diskUseWarn(du diskUse, diskPath string) {
	if pu := du.percentUsed(); pu > float64(s.index.Config.DiskUseWarningPercentage) {
		if !s.isReadOnly() && time.Since(s.diskScanState.lastWarning) >
			s.diskScanState.getWarningInterval() {
			s.index.logger.WithField("action", "read_disk_use").
				WithField("shard", s.name).
				WithField("path", diskPath).
				Warnf("disk usage currently at %.2f%%, threshold set to %.2f%%",
					pu, float64(s.index.Config.DiskUseWarningPercentage))

			s.index.logger.WithField("action", "disk_use_stats").
				WithField("shard", s.name).
				WithField("path", diskPath).
				Debugf("%s", du.String())

			s.diskScanState.lastWarning = time.Now()
			s.diskScanState.increaseWarningInterval()
		}
	}
}

// sets the shard to readonly if user-set threshold is surpassed
func (s *Shard) diskUseReadonly(du diskUse, diskPath string) {
	if pu := du.percentUsed(); pu > float64(s.index.Config.DiskUseReadOnlyPercentage) {
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
					pu, s.name, float64(s.index.Config.DiskUseReadOnlyPercentage))
		}
	}
}
