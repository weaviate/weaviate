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
	"os/exec"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
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

var (
	ErrNotEnoughMemory   = fmt.Errorf("not enough memory")
	ErrNotEnoughMappings = fmt.Errorf("not enough mappings")
)

// Monitor allows making statements about the memory ratio used by the application
type Monitor struct {
	metricsReader     metricsReader
	limitSetter       limitSetter
	maxRatio          float64
	maxMemoryMappings int64

	// state
	mu           sync.Mutex
	limit        int64
	usedMemory   int64
	usedMappings int64
}

// Refresh retrieves the current memory stats from the runtime and stores them
// in the local cache
func (m *Monitor) Refresh() {
	m.obtainCurrentUsage()
	m.obtainCurrentMappings()
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
	m := &Monitor{
		metricsReader:     metricsReader,
		limitSetter:       limitSetter,
		maxRatio:          maxRatio,
		maxMemoryMappings: getMaxMemoryMappings(),
	}
	m.Refresh()
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

func (m *Monitor) CheckMapping(numberMappings int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if float64(m.usedMappings+numberMappings) > float64(m.maxMemoryMappings) {
		return ErrNotEnoughMappings
	}

	return nil
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usedMappings = getCurrentMappings()
}

func getCurrentMappings() int64 {
	switch runtime.GOOS {
	case "linux":
		return currentMappingsCommand("cat", "/proc/%s/maps´")
	case "darwin":
		// command output contains a bunch of extra info
		return currentMappingsCommand("vmmap", "%s") - currentMappingsCommand("vmmap", "%s -summary")
	default:
		return 0
	}
}

func currentMappingsCommand(command, args string) int64 {
	cmd1 := exec.Command(command, fmt.Sprintf(args, strconv.Itoa(os.Getpid()))) // print mappings
	cmd2 := exec.Command("wc", "-l")                                            // count nunmber of mappings

	pipe, err := cmd1.StdoutPipe()
	if err != nil {
		return 0
	}
	cmd2.Stdin = pipe

	if err := cmd1.Start(); err != nil {
		return 0
	}

	output, err := cmd2.Output()
	if err != nil {
		return 0
	}

	mappings, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return 0
	}
	return int64(mappings)
}

func getMaxMemoryMappings() int64 {
	maxMappings := int64(math.MaxInt64)

	// get user provided default
	if v := os.Getenv("MAX_MEMORY_MAPPINGS"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return maxMappings
		}
		return int64(asInt)
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
	for scanner.Scan() {
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
		metricsReader: func() int64 { return 0 },
		limitSetter:   func(size int64) int64 { return TiB },
		maxRatio:      1,
	}
	m.Refresh()
	return m
}

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
