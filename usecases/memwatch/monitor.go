//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package memwatch

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
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

	mappingsRefreshFarFromLimit = 30 * time.Second
	// MappingsReadDue accepts a read this early, because ticker times jitter.
	// A value below the caller's tick keeps reads from coming a tick early.
	mappingsReadTolerance = 250 * time.Millisecond
)

// Monitor allows making statements about the memory ratio used by the application
type Monitor struct {
	metricsReader     metricsReader
	limitSetter       limitSetter
	maxRatio          float64
	maxMemoryMappings int64

	// state
	mu                     sync.RWMutex
	limit                  int64
	usedMemory             int64
	usedMappings           int64
	reservedMappings       int64
	reservedMappingsBuffer []int64
	createdAt              time.Time // reservation seconds count from here
	lastClearedSecond      int64     // the last second whose reservations were cleared
	lastMappingsRead       time.Time // start of the last successful ReadMappings
	lastMappingsReadFailed time.Time // start of the last failed ReadMappings
	lastMappingsAdmitted   time.Time // when CheckMappingAndReserve last admitted mappings
	mapsPath               string    // empty outside Linux
	mappingsBuf            []byte
}

// Refresh retrieves the current memory stats from the runtime and stores them
// in the local cache
func (m *Monitor) Refresh(updateMappings bool) {
	m.obtainCurrentUsage()
	m.updateLimit()
	if updateMappings {
		// MappingsReadDue retries a failed read
		_ = m.ReadMappings(time.Now())
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
		createdAt:              time.Now(),
		mapsPath:               procMapsPath(),
		mappingsBuf:            make([]byte, 32*1024),
	}
	m.Refresh(true)
	return m
}

func (m *Monitor) CheckAlloc(sizeInBytes int64) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if float64(m.usedMemory+sizeInBytes)/float64(m.limit) > m.maxRatio {
		return enterrors.ErrNotEnoughMemory
	}

	return nil
}

func (m *Monitor) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Near the limit a read comes every MappingDelayInS, so one counts these
	// mappings before their reservation expires.
	if reservationTimeInS > 0 {
		reservationTimeInS += MappingDelayInS
	}
	if reservationTimeInS > len(m.reservedMappingsBuffer) {
		reservationTimeInS = len(m.reservedMappingsBuffer)
	}

	now := time.Now()
	m.expireReservations(now)

	if m.usedMappings+numberMappings+m.reservedMappings > m.maxMemoryMappings {
		return enterrors.ErrNotEnoughMappings
	}
	if reservationTimeInS > 0 {
		m.reservedMappings += numberMappings
		m.reservedMappingsBuffer[reservationSlot(m.secondsSinceCreated(now)+int64(reservationTimeInS))] += numberMappings
	}
	m.lastMappingsAdmitted = now

	return nil
}

// MappingsReadDue reports whether the memory mappings should be read at now.
func (m *Monitor) MappingsReadDue(now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	lastAttempt := m.lastMappingsRead
	if m.lastMappingsReadFailed.After(lastAttempt) {
		lastAttempt = m.lastMappingsReadFailed
	}
	return now.Sub(lastAttempt) >= m.mappingsRefreshInterval(now)-mappingsReadTolerance
}

// mappingsRefreshInterval is the wait between reads. It is long only far from the
// limit, because a read walks every mapping. The caller holds m.mu.
func (m *Monitor) mappingsRefreshInterval(now time.Time) time.Duration {
	m.expireReservations(now)
	nearLimit := m.usedMappings+m.reservedMappings >= m.maxMemoryMappings/2
	readFailed := m.lastMappingsReadFailed.After(m.lastMappingsRead)
	if nearLimit || readFailed || m.lastMappingsAdmitted.After(m.lastMappingsRead) {
		return MappingDelayInS * time.Second
	}
	return mappingsRefreshFarFromLimit
}

// expireReservations drops the reservations that expired by now. It ignores a now
// before the last clear, because clearing a second twice drops the full-length
// reservations made in it.
func (m *Monitor) expireReservations(now time.Time) {
	second := m.secondsSinceCreated(now)
	if second <= m.lastClearedSecond {
		return
	}
	m.reservedMappings -= clearReservedMappings(m.lastClearedSecond, second, m.reservedMappingsBuffer)
	m.lastClearedSecond = second
}

// secondsSinceCreated relies on the monotonic reading that time.Now and ticker
// times carry, so a wall-clock step moves no reservation's expiry.
func (m *Monitor) secondsSinceCreated(now time.Time) int64 {
	return max(0, int64(now.Sub(m.createdAt)/time.Second))
}

// reservationSlot is the reservedMappingsBuffer index for reservations expiring in second.
func reservationSlot(second int64) int {
	return int(second % mappingsEntries)
}

// clearReservedMappings empties the slots for seconds lastCleared+1 through now
// and returns the mappings they held.
func clearReservedMappings(lastCleared, now int64, reservedMappingsBuffer []int64) int64 {
	// a gap longer than the buffer clears every slot once
	from := max(lastCleared+1, now-mappingsEntries+1)

	clearedMappings := int64(0)
	for s := from; s <= now; s++ {
		i := reservationSlot(s)
		clearedMappings += reservedMappingsBuffer[i]
		reservedMappingsBuffer[i] = 0
	}
	return clearedMappings
}

func (m *Monitor) Ratio() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return float64(m.usedMemory) / float64(m.limit)
}

// obtainCurrentUsage obtains the most recent live heap from runtime/metrics
func (m *Monitor) obtainCurrentUsage() {
	m.setUsed(m.metricsReader())
}

// ReadMappings counts the mappings in mapsPath and records now as the read's
// start. A failed read keeps the last count.
func (m *Monitor) ReadMappings(now time.Time) error {
	used, err := getCurrentMappings(m.mapsPath, m.mappingsBuf)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.lastMappingsReadFailed = now
		return err
	}
	monitoring.GetMetrics().MmapProcMaps.Set(float64(used))
	m.usedMappings = used
	m.lastMappingsRead = now
	return nil
}

// procMapsPath is this process's /proc/<pid>/maps, or empty outside Linux.
func procMapsPath() string {
	if runtime.GOOS != "linux" {
		return ""
	}
	return fmt.Sprintf("/proc/%d/maps", os.Getpid())
}

func getCurrentMappings(mapsPath string, buf []byte) (int64, error) {
	if mapsPath == "" {
		return 0, nil
	}
	return currentMappingsLinux(mapsPath, buf)
}

// Counts the number of mappings by counting the number of lines within the maps file
// Optimized version that counts newlines in chunks without string allocation
func currentMappingsLinux(filePath string, buf []byte) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var count int64

	for {
		n, err := file.Read(buf[:])
		count += int64(bytes.Count(buf[:n], []byte{'\n'}))

		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}

	return count, nil
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
		createdAt:              time.Now(),
		mapsPath:               procMapsPath(),
		mappingsBuf:            make([]byte, 32*1024),
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

// EstimateBatchObjectMemory sums every vector source the object carries. An object
// may hold the legacy vector, the byte vector, and named or multi vectors at once,
// and the memory check must count all of them. The 30 byte overhead is per object,
// as in the other estimators in this file.
func EstimateBatchObjectMemory(object *protocol.BatchObject) int64 {
	size := len(object.GetVector())*4 + len(object.GetVectorBytes())
	for _, vec := range object.GetVectors() {
		size += len(vec.GetVectorBytes())
	}
	if size == 0 {
		return 0
	}
	return int64(size + 30)
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
