package lsmkv

import "time"

type memtableSizeAdvisorCfg struct {
	initial     int
	stepSize    int
	maxSize     int
	minDuration time.Duration
	maxDuration time.Duration
}

type memtableSizeAdvisor struct {
	cfg memtableSizeAdvisorCfg
}

func newMemtableSizeAdvisor(cfg memtableSizeAdvisorCfg) *memtableSizeAdvisor {
	return &memtableSizeAdvisor{
		cfg: cfg,
	}
}

func (m memtableSizeAdvisor) Initial() int {
	return m.cfg.initial
}

func (m memtableSizeAdvisor) NextTarget(previousTarget int,
	timeSinceFlush time.Duration,
) (int, bool) {
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
