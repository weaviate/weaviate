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

package memwatch

import (
	"fmt"
	"runtime/metrics"
	"sync"

	"github.com/weaviate/weaviate/entities/models"
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
	metricsReader metricsReader
	limitSetter   limitSetter
	maxRatio      float64

	// state
	mu    sync.Mutex
	limit int64
	used  int64
}

// Refresh retrieves the current memory stats from the runtime and stores them
// in the local cache
func (m *Monitor) Refresh() {
	m.obtainCurrentUsage()
	m.updateLimit()
}

// we have no intentions of ever modifying the limit, but SetMemoryLimit with a
// negative value is the only way to read the limit from the runtime
type limitSetter func(size int64) int64

// NewMonitor creates a [Monitor] with the given metrics reader and target
// ratio
//
// Typically this would be called with LiveHeapReader and
// debug.SetMemoryLimit
func NewMonitor(metricsReader metricsReader, limitSetter limitSetter,
	maxRatio float64,
) *Monitor {
	return &Monitor{
		metricsReader: metricsReader,
		limitSetter:   limitSetter,
		maxRatio:      maxRatio,
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

// obtainCurrentUsage obtains the most recent live heap from runtime/metrics
func (m *Monitor) obtainCurrentUsage() {
	m.setUsed(m.metricsReader())
}

func LiveHeapReader() int64 {
	const liveHeapBytesMetric = "/gc/heap/live:bytes"
	sample := make([]metrics.Sample, 1)
	sample[0].Name = liveHeapBytesMetric
	metrics.Read(sample)

	if sample[0].Value.Kind() == metrics.KindBad {
		panic(fmt.Sprintf("metric %q no longer supported", liveHeapBytesMetric))
	}

	return int64(sample[0].Value.Uint64())
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

type DummyAllocChecker struct{}

func (d DummyAllocChecker) CheckAlloc(sizeInBytes int64) error { return nil }
func (d DummyAllocChecker) Refresh()                           {}

type metricsReader func() int64

type AllocChecker interface {
	CheckAlloc(sizeInBytes int64) error
	Refresh()
}

func EstimateObjectMemory(object *models.Object) int64 {
	// Note: This is very much oversimplified. It assumes that we always need
	// the footprint of the full vector and it assumes a fixed overhead of 30B
	// per vector. In reality this depends on the HNSW settings - and possibly
	// in the future we might have completely different index types.
	//
	// However, in the meantime this should be a fairly reasonable estimate, as
	// it's not meant to fail exactly on the last available byte, but rather
	// prevent OOM crashes. Given the fuzziness and async style of the
	// memtracking somewhat decent estimate should be good enough.
	return int64(len(object.Vector)*4 + 30)
}

func EstimateObjectDeleteMemory() int64 {
	// When deleting an object we attach a tombstone to the object in the HNSW and a new segment in the Memtable and
	// additional other temporary allocations.
	// The total amount is hard to guess so we go with 100 bytes.
	return 100
}
