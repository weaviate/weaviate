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

package lsmkv

import "time"

// if not enough config is provided we can fall back to this reasonable default
// value
const reasonableMemtableDefault = 10 * 1024 * 1024

type memtableSizeAdvisorCfg struct {
	initial     int
	stepSize    int
	maxSize     int
	minDuration time.Duration
	maxDuration time.Duration
}

type memtableSizeAdvisor struct {
	cfg    memtableSizeAdvisorCfg
	active bool
}

func newMemtableSizeAdvisor(cfg memtableSizeAdvisorCfg) *memtableSizeAdvisor {
	a := &memtableSizeAdvisor{
		cfg: cfg,
	}

	// only activate if initial size, step size, max size, and max duration are
	// given
	if a.cfg.maxSize > 0 && a.cfg.initial > 0 && a.cfg.stepSize > 0 && a.cfg.maxDuration > 0 {
		a.active = true
	}

	return a
}

func (m memtableSizeAdvisor) Initial() int {
	if m.active {
		return m.cfg.initial
	} else {
		return reasonableMemtableDefault
	}
}

func (m memtableSizeAdvisor) NextTarget(previousTarget int,
	timeSinceFlush time.Duration,
) (int, bool) {
	if !m.active {
		return reasonableMemtableDefault, false
	}

	if timeSinceFlush < m.cfg.minDuration {
		next := min(previousTarget+m.cfg.stepSize, m.cfg.maxSize)
		return next, next != previousTarget
	}
	if timeSinceFlush > m.cfg.maxDuration {
		next := max(previousTarget-m.cfg.stepSize, m.cfg.initial)
		return next, next != previousTarget
	}
	return previousTarget, false
}

func min(a, b int) int {
	if a <= b {
		return a
	}

	return b
}

func max(a, b int) int {
	if a >= b {
		return a
	}

	return b
}
