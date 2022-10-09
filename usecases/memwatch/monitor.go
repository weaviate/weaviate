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

package memwatch

import (
	"math"
	"runtime"
)

// Monitor allows making statements about the memory ratio used by the application
type Monitor struct {
	memProfiler memProfiler
	limitSetter limitSetter
	rate        int64
}

type memProfiler func(p []runtime.MemProfileRecord, inUseZero bool) (int, bool)

// we have no intentions of ever modifying the limit, but SetMemoryLimit with a
// negative value is the only way to read the limit from the runtime
type limitSetter func(size int64) int64

// NewMonitor creates a [Monitor] with the given profiler, limitsetter and rate.
//
// Typically this would be called with runtime.MemProfile,
// debug.SetMemoryLimit, and runtime.MemProfileRate
func NewMonitor(profiler memProfiler, limitSetter limitSetter, rate int) *Monitor {
	return &Monitor{
		memProfiler: profiler,
		limitSetter: limitSetter,
		rate:        int64(rate),
	}
}

// inspired by runtime/pprof/pprof.go
func (m *Monitor) Ratio() float64 {
	var p []runtime.MemProfileRecord
	n, _ := m.memProfiler(nil, true)
	for {
		// Allocate room for a slightly bigger profile,
		// in case a few more entries have been added
		// since the call to MemProfile.
		p = make([]runtime.MemProfileRecord, n+50)
		n, ok := m.memProfiler(p, true)
		if ok {
			p = p[0:n]
			break
		}
		// Profile grew; try again.
	}

	var sum int64

	for _, r := range p {
		_, size := scaleHeapSample(r.InUseObjects(), r.InUseBytes(), m.rate)
		sum += size
	}

	// setting a negative limit is the only way to obtain the current limit
	limit := m.limitSetter(-1)
	return float64(sum) / float64(limit)
}

// copied from runtime/pprof/protomem.go
//
// scaleHeapSample adjusts the data from a heap Sample to
// account for its probability of appearing in the collected
// data. heap profiles are a sampling of the memory allocations
// requests in a program. We estimate the unsampled value by dividing
// each collected sample by its probability of appearing in the
// profile. heap profiles rely on a poisson process to determine
// which samples to collect, based on the desired average collection
// rate R. The probability of a sample of size S to appear in that
// profile is 1-exp(-S/R).
func scaleHeapSample(count, size, rate int64) (int64, int64) {
	if count == 0 || size == 0 {
		return 0, 0
	}

	if rate <= 1 {
		// if rate==1 all samples were collected so no adjustment is needed.
		// if rate<1 treat as unknown and skip scaling.
		return count, size
	}

	avgSize := float64(size) / float64(count)
	scale := 1 / (1 - math.Exp(-avgSize/float64(rate)))

	return int64(float64(count) * scale), int64(float64(size) * scale)
}
