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
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	B   = 1
	KiB = 1 << (10 * iota) // 2^10
	MiB = 1 << (10 * iota) // 2^20
	GiB = 1 << (10 * iota) // 2^30
	TiB = 1 << (10 * iota) // 2^40
)

const (
	MappingDelayInS = 2
	mappingsEntries = 60 + MappingDelayInS
)

var (
	ErrNotEnoughMemory   = fmt.Errorf("not enough memory")
	ErrNotEnoughMappings = fmt.Errorf("not enough memory mappings")
)

// Monitor allows making statements about the memory ratio used by the application
type Monitor struct {
	metricsReader     metricsReader
	limitSetter       limitSetter
	maxRatio          float64
	maxMemoryMappings int64

	// state
	mu                     sync.Mutex
	limit                  int64
	usedMemory             int64
	usedMappings           int64
	reservedMappings       int64
	reservedMappingsBuffer []int64
	lastReservationsClear  time.Time
}

// Refresh retrieves the current memory stats from the runtime and stores them
// in the local cache
func (m *Monitor) Refresh(updateMappings bool) {
	m.obtainCurrentUsage()
	m.updateLimit()
	if updateMappings {
		m.obtainCurrentMappings()
	}
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
	m := &Monitor{
		metricsReader:          metricsReader,
		limitSetter:            limitSetter,
		maxRatio:               maxRatio,
		maxMemoryMappings:      getMaxMemoryMappings(),
		reservedMappingsBuffer: make([]int64, mappingsEntries), // one entry per second + buffer to handle delays
		lastReservationsClear:  time.Now(),
	}
	m.Refresh(true)
	return m
}

func (m *Monitor) CheckAlloc(sizeInBytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if float64(m.usedMemory+sizeInBytes)/float64(m.limit) > m.maxRatio {
		return ErrNotEnoughMemory
	}

	return nil
}

func (m *Monitor) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// mappings are only updated every Xs, so we need to extend the reservation time
	if reservationTimeInS > 0 {
		reservationTimeInS += MappingDelayInS
	}
	if reservationTimeInS > len(m.reservedMappingsBuffer) {
		reservationTimeInS = len(m.reservedMappingsBuffer)
	}

	// expire old mappings
	now := time.Now()
	m.reservedMappings -= clearReservedMappings(m.lastReservationsClear, now, m.reservedMappingsBuffer)

	if m.usedMappings+numberMappings+m.reservedMappings > m.maxMemoryMappings {
		return ErrNotEnoughMappings
	}
	if reservationTimeInS > 0 {
		m.reservedMappings += numberMappings
		m.reservedMappingsBuffer[(now.Second()+reservationTimeInS)%(mappingsEntries)] += numberMappings
	}

	m.lastReservationsClear = now

	return nil
}

func clearReservedMappings(lastClear time.Time, now time.Time, reservedMappingsBuffer []int64) int64 {
	clearedMappings := int64(0)
	if now.Sub(lastClear) >= mappingsEntries*time.Second {
		for i := 0; i < len(reservedMappingsBuffer); i++ {
			clearedMappings += reservedMappingsBuffer[i]
			reservedMappingsBuffer[i] = 0
		}
	} else if now.Second() == lastClear.Second() {
		// do nothing
	} else if now.Second() > lastClear.Second() {
		// the value of the last refresh was already cleared
		for i := lastClear.Second() + 1; i <= now.Second(); i++ {
			clearedMappings += reservedMappingsBuffer[i]
			reservedMappingsBuffer[i] = 0
		}
	} else {
		// wrap around, the value of the last refresh was already cleared
		for i := lastClear.Second() + 1; i < len(reservedMappingsBuffer); i++ {
			clearedMappings += reservedMappingsBuffer[i]
			reservedMappingsBuffer[i] = 0
		}
		for i := 0; i < now.Second(); i++ {
			clearedMappings += reservedMappingsBuffer[i]
			reservedMappingsBuffer[i] = 0
		}
	}
	return clearedMappings
}

func (m *Monitor) Ratio() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return float64(m.usedMemory) / float64(m.limit)
}

// obtainCurrentUsage obtains the most recent live heap from runtime/metrics
func (m *Monitor) obtainCurrentUsage() {
	m.setUsed(m.metricsReader())
}

func (m *Monitor) obtainCurrentMappings() {
	used := getCurrentMappings()
	monitoring.GetMetrics().MmapProcMaps.Set(float64(used))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usedMappings = used
}

func getCurrentMappings() int64 {
	switch runtime.GOOS {
	case "linux":
		return currentMappingsLinux()
	default:
		return 0
	}
}

// Counts the number of mappings by counting the number of lines within the maps file
func currentMappingsLinux() int64 {
	filePath := fmt.Sprintf("/proc/%d/maps", os.Getpid())
	file, err := os.Open(filePath)
	if err != nil {
		return 0
	}
	defer file.Close()

	var mappings int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		mappings++
	}

	if err := scanner.Err(); err != nil {
		return 0
	}

	return mappings
}

func getMaxMemoryMappings() int64 {
	maxMappings := int64(math.MaxInt64)

	// get user provided default
	if v := os.Getenv("MAX_MEMORY_MAPPINGS"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err == nil {
			return int64(asInt)
		}
	}

	// different operating systems have different ways of finding the max
	switch runtime.GOOS {
	case "linux":
		return readMaxMemoryMappingsLinux(maxMappings)
	default:
		return maxMappings // macos does not seem to have a readable limit
	}
}

func readMaxMemoryMappingsLinux(defaultValue int64) int64 {
	file, err := os.Open("/proc/sys/vm/max_map_count")
	if err != nil {
		return defaultValue
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Read the value from the file
	if scanner.Scan() {
		asInt, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return defaultValue
		}
		return int64(float64(asInt) * 0.7) // leave room for other processes on the system
	}
	return defaultValue
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

// setUsed is a thread-safe way to set the current usage
func (m *Monitor) setUsed(used int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.usedMemory = used
}

func (m *Monitor) updateLimit() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// setting a negative limit is the only way to obtain the current limit
	m.limit = m.limitSetter(-1)
}

func NewDummyMonitor() *Monitor {
	m := &Monitor{
		metricsReader:          func() int64 { return 0 },
		limitSetter:            func(size int64) int64 { return TiB },
		maxRatio:               1,
		maxMemoryMappings:      10000000,
		reservedMappingsBuffer: make([]int64, mappingsEntries),
		lastReservationsClear:  time.Now(),
	}
	m.Refresh(true)
	return m
}

type metricsReader func() int64

type AllocChecker interface {
	CheckAlloc(sizeInBytes int64) error
	CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error
	Refresh(updateMappings bool)
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

func EstimateStorObjectMemory(object *storobj.Object) int64 {
	// Note: The estimation is not super accurate. It assumes that the
	// memory is mostly used by the vector of float32 + the fixed
	// overhead. It assumes a fixed overhead of 46 Bytes per object
	// (30 Bytes from the data field models.Object + 16 Bytes from
	// remaining data fields of storobj.Object).
	return int64(len(object.Vector)*4 + 46)
}

func EstimateObjectDeleteMemory() int64 {
	// When deleting an object we attach a tombstone to the object in the HNSW and a new segment in the Memtable and
	// additional other temporary allocations.
	// The total amount is hard to guess, so we go with a default of 100 bytes.
	estimate := int64(100)
	if v := os.Getenv("MEMORY_ESTIMATE_DELETE_BYTES"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return estimate
		}

		return int64(asInt)
	}
	return estimate
}
