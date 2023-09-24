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

package memwatch

import (
	"fmt"
	"math"
	"runtime"
	"sync"
)

const (
	B   = 1
	KiB = 1 << (10 * iota) // 2^10
	MiB = 1 << (10 * iota) // 2^20
	GiB = 1 << (10 * iota) // 2^30
	TiB = 1 << (10 * iota) // 2^40
)

var ErrNotEnoughMemory = fmt.Errorf("not enough memory")

// Monitor allows making statements about the memory ratio used by the application
type Monitor struct {
	memProfiler memProfiler
	limitSetter limitSetter
	rate        int64
	maxRatio    float64

	// state
	mu    sync.Mutex
	limit int64
	used  int64
}

// Refresh retrieves the current memory stats from the runtime and stores them
// in the local cache
func (m *Monitor) Refresh() {
	m.calculateCurrentUsage()
	m.updateLimit()
}

type memProfiler func(p []runtime.MemProfileRecord, inUseZero bool) (int, bool)

// we have no intentions of ever modifying the limit, but SetMemoryLimit with a
// negative value is the only way to read the limit from the runtime
type limitSetter func(size int64) int64

// NewMonitor creates a [Monitor] with the given profiler, limitsetter and rate.
//
// Typically this would be called with runtime.MemProfile,
// debug.SetMemoryLimit, and runtime.MemProfileRate
func NewMonitor(profiler memProfiler, limitSetter limitSetter,
	rate int, maxRatio float64,
) *Monitor {
	return &Monitor{
		memProfiler: profiler,
		limitSetter: limitSetter,
		rate:        int64(rate),
		maxRatio:    maxRatio,
	}
}

func (m *Monitor) CheckAlloc(sizeInBytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if float64(m.used+sizeInBytes)/float64(m.limit) > m.maxRatio {
		return ErrNotEnoughMemory
	}

	return nil
}

func (m *Monitor) Ratio() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return float64(m.used) / float64(m.limit)
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

// calculateCurrentUsage obtains the most recent mem profile records from the
// runtime, sums them up and scales them according to the set profiling rate.
//
// The logic to retrieve the records is inspired by runtime/pprof/pprof.go
func (m *Monitor) calculateCurrentUsage() {
	var p []runtime.MemProfileRecord
	n, _ := m.memProfiler(nil, true)
	for {
		// Allocate room for a slightly bigger profile,
		// in case a few more entries have been added
		// since the call to MemProfile.
		p = make([]runtime.MemProfileRecord, n+50)

		// use different var name and explicitly overwrite
		// otherwise n from the outside is shadowed and not overwritten
		n2, ok := m.memProfiler(p, true)
		if ok {
			p = p[0:n2]
			break
		}
		// Profile grew; try again.
		n = n2
	}

	var sum int64

	for _, r := range p {
		_, size := scaleHeapSample(r.InUseObjects(), r.InUseBytes(), m.rate)
		sum += size
	}

	m.setUsed(sum)
}

// setUsed is a thread-safe way way to set the current usage
func (m *Monitor) setUsed(used int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.used = used
}

func (m *Monitor) updateLimit() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// setting a negative limit is the only way to obtain the current limit
	m.limit = m.limitSetter(-1)
}
