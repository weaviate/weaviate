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
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestEstimation(t *testing.T) {
	t.Run("set correctly", func(t *testing.T) {
		t.Setenv("MEMORY_ESTIMATE_DELETE_BYTES", "120")
		assert.Equal(t, int64(120), EstimateObjectDeleteMemory())
	})

	t.Run("set wrong - use default", func(t *testing.T) {
		t.Setenv("MEMORY_ESTIMATE_DELETE_BYTES", "abc")
		assert.Equal(t, int64(100), EstimateObjectDeleteMemory())
	})

	t.Run("unset - use default", func(t *testing.T) {
		t.Setenv("MEMORY_ESTIMATE_DELETE_BYTES", "")
		assert.Equal(t, int64(100), EstimateObjectDeleteMemory())
	})
}

func TestMonitor(t *testing.T) {
	t.Run("with constant profiles (no changes)", func(t *testing.T) {
		metrics := &fakeHeapReader{val: 30000}
		limiter := &fakeLimitSetter{limit: 100000}

		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh(true)

		assert.Equal(t, 0.3, m.Ratio())
	})

	t.Run("with less memory than the threshold", func(t *testing.T) {
		metrics := &fakeHeapReader{val: 700 * MiB}
		limiter := &fakeLimitSetter{limit: 1 * GiB}

		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh(true)

		err := m.CheckAlloc(100 * MiB)
		assert.NoError(t, err, "with 700 allocated, an additional 100 would be about 80% which is not a problem")

		err = m.CheckAlloc(299 * MiB)
		assert.Error(t, err, "with 700 allocated, an additional 299 would be about 97.5% which is not allowed")

		err = m.CheckAlloc(400 * MiB)
		assert.Error(t, err, "with 700 allocated, an additional 400 would be about 110% which is not allowed")
	})

	t.Run("with memory already over the threshold", func(t *testing.T) {
		metrics := &fakeHeapReader{val: 1025 * MiB}
		limiter := &fakeLimitSetter{limit: 1 * GiB}

		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh(true)

		err := m.CheckAlloc(1 * B)
		assert.Error(t, err,
			"any check should fail, since we're already over the limit")

		err = m.CheckAlloc(10 * MiB)
		assert.Error(t, err, "any check should fail, since we're already over the limit")

		err = m.CheckAlloc(1 * TiB)
		assert.Error(t, err, "any check should fail, since we're already over the limit")
	})

	t.Run("with real dependencies", func(t *testing.T) {
		m := NewMonitor(LiveHeapReader, debug.SetMemoryLimit, 0.97)
		_ = m.Ratio()
	})
}

func TestMappings(t *testing.T) {
	// dont matter here
	metrics := &fakeHeapReader{val: 30000}
	limiter := &fakeLimitSetter{limit: 100000}

	t.Run("max memory mappings set correctly", func(t *testing.T) {
		t.Setenv("MAX_MEMORY_MAPPINGS", "120")
		assert.Equal(t, int64(120), getMaxMemoryMappings())
	})

	t.Run("max memory mappings incorrectly", func(t *testing.T) {
		t.Setenv("MAX_MEMORY_MAPPINGS", "abc")
		switch runtime.GOOS {
		case "linux":
			// we can read the max value, but it does not exist on all systems
			if _, err := os.Stat("/proc/sys/vm/max_map_count"); errors.Is(err, os.ErrNotExist) {
				assert.Equal(t, getMaxMemoryMappings(), int64(math.MaxInt64))
			} else {
				assert.Greater(t, getMaxMemoryMappings(), int64(0))
				assert.Less(t, getMaxMemoryMappings(), int64(math.MaxInt64))
			}
		default:
			// cant read on other OS so we use max int
			assert.Equal(t, getMaxMemoryMappings(), int64(math.MaxInt64))
		}
	})

	t.Run("max memory mappings not set", func(t *testing.T) {
		t.Setenv("MAX_MEMORY_MAPPINGS", "")
		switch runtime.GOOS {
		case "linux":
			// we can read the max value, but it does not exist on all systems
			if _, err := os.Stat("/proc/sys/vm/max_map_count"); errors.Is(err, os.ErrNotExist) {
				assert.Equal(t, getMaxMemoryMappings(), int64(math.MaxInt64))
			} else {
				assert.Greater(t, getMaxMemoryMappings(), int64(0))
				assert.Less(t, getMaxMemoryMappings(), int64(math.MaxInt64))
			}
		default:
			// cant read on other OS so we use max int
			assert.Equal(t, getMaxMemoryMappings(), int64(math.MaxInt64))
		}
	})

	t.Run("current memory settings", func(t *testing.T) {
		switch runtime.GOOS {
		case "linux":
			assert.Greater(t, currentMappings(t), int64(0))
			assert.Less(t, currentMappings(t), int64(math.MaxInt64))
		case "darwin":
			assert.Equal(t, currentMappings(t), int64(0))
		}
	})

	t.Run("test currentMappingsLinux with simulation file", func(t *testing.T) {
		file := createTestMappingsFile(t, 5001)
		defer os.Remove(file.Name())
		defer file.Close()

		result, err := currentMappingsLinux(file.Name(), make([]byte, 32*1024))
		require.NoError(t, err)
		assert.Equal(t, int64(5001), result, "Should count exactly 5001 mappings")
	})

	t.Run("check mappings, by open many file mappings and close them only after the test is done", func(t *testing.T) {
		if runtime.GOOS == "darwin" {
			t.Skip("macOS does not have a limit on mappings")
		}
		usedMappings := currentMappings(t)
		addMappings := 15
		t.Setenv("MAX_MEMORY_MAPPINGS", strconv.FormatInt(usedMappings+int64(addMappings), 10))
		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh(true)

		mappingsLeft := getMaxMemoryMappings() - usedMappings
		assert.InDelta(t, mappingsLeft, addMappings, 10) // other things can happen at the same time
		path := t.TempDir()

		limitReached := false

		// use up available mappings
		for i := 0; i < int(mappingsLeft)*2; i++ {
			m.Refresh(true)
			file, err := os.OpenFile(path+"example"+strconv.FormatInt(int64(i), 10)+".txt", os.O_CREATE|os.O_RDWR, 0o666)
			require.Nil(t, err)
			defer file.Close() // defer inside the loop because files should stay open until end of test to continue to use mappings
			_, err = file.Write([]byte("Hello"))
			require.Nil(t, err)

			fileInfo, err := file.Stat()
			require.Nil(t, err)

			// there might be other processes that use mappings. Don't check any specific number just that we have
			// reached the limit
			if mappingsLeft := getMaxMemoryMappings() - currentMappings(t); mappingsLeft <= 0 {
				limitReached = true
				break
			} else {
				data, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
				require.Nil(t, err)

				defer syscall.Munmap(data)
			}
		}

		// Try to reserve a large amount and have it fail (checker only runs on linux)
		switch runtime.GOOS {
		case "linux":
			// ensure that we have hit the limit of available mappings
			require.True(t, limitReached)
			// any further mapping should fail
			require.Error(t, m.CheckMappingAndReserve(int64(addMappings), 60))
		case "darwin":
			// ensure that we don't hit the limit of available mappings
			require.False(t, limitReached)
			// any further mapping should not fail
			require.Nil(t, m.CheckMappingAndReserve(int64(addMappings), 60))
		}
	})

	t.Run("check mappings for dummy, to check that it never blocks", func(t *testing.T) {
		m := NewDummyMonitor()
		m.Refresh(true)

		path := t.TempDir()
		// use many mappings, dummy monitor should never block
		for i := 0; i < 100; i++ {
			m.Refresh(true)
			file, err := os.OpenFile(path+"example"+strconv.FormatInt(int64(i), 10)+".txt", os.O_CREATE|os.O_RDWR, 0o666)
			require.Nil(t, err)
			defer file.Close() // defer inside the loop because files should stay open until end of test to continue to use mappings
			_, err = file.Write([]byte("Hello"))
			require.Nil(t, err)

			fileInfo, err := file.Stat()
			require.Nil(t, err)

			require.Nil(t, m.CheckMappingAndReserve(1, 1))
			data, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
			require.Nil(t, err)

			defer syscall.Munmap(data)
		}
	})

	t.Run("check reservations", func(t *testing.T) {
		usedMappings := currentMappings(t)
		addMappings := 15
		t.Setenv("MAX_MEMORY_MAPPINGS", strconv.FormatInt(usedMappings+int64(addMappings), 10))
		maxMappings := getMaxMemoryMappings()
		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh(true)

		// reserve up available mappings
		for i := 0; i < int(addMappings)+5; i++ {
			// there might be other processes that use mappings
			if maxMappings-m.usedMappings-int64(i) <= 0 {
				require.NotNil(t, m.CheckMappingAndReserve(1, 60))
			} else {
				require.Nil(t, m.CheckMappingAndReserve(1, 60))
			}
		}

		// any further mapping should fail
		require.NotNil(t, m.CheckMappingAndReserve(1, 60))
	})
}

func TestMappingsReservationClearing(t *testing.T) {
	const baseSecond = 30 // seconds since the monitor was built
	cases := []struct {
		name          string
		baseLineShift int
		nowShift      int
		// keys are the seconds after baseSecond at which a reservation expires
		reservations     map[int]int64
		expectedClearing int64
	}{
		{name: "no reservations", reservations: map[int]int64{}, expectedClearing: 0},
		{name: "reservations present, no expiration", nowShift: 1, reservations: map[int]int64{31: 45, 32: 14}, expectedClearing: 0},
		{name: "reservations present, one expiration", nowShift: 1, reservations: map[int]int64{31: 45, 1: 14}, expectedClearing: 14},
		{name: "reservations present, clear all", nowShift: 62, reservations: map[int]int64{30: 1, 31: 1, 32: 1, 29: 1}, expectedClearing: 4},
		{name: "reservations present, clear all after a long gap", nowShift: 200, reservations: map[int]int64{1: 1, 15: 1, 30: 1, 62: 1}, expectedClearing: 4},
		{name: "reservations present, clear nothing (same time)", reservations: map[int]int64{30: 1, 0: 1, 32: 1, 29: 1}, expectedClearing: 0},
		{name: "clear range", nowShift: 20, reservations: map[int]int64{30: 10, 59: 10, 0: 1, 1: 1, 20: 1, 21: 10}, expectedClearing: 2},
		{name: "clear over the buffer wraparound", nowShift: 45, reservations: map[int]int64{30: 1, 31: 1, 32: 1, 59: 10, 29: 1}, expectedClearing: 4},
		{name: "clear the current second over the buffer wraparound", nowShift: 45, reservations: map[int]int64{45: 1, 46: 10}, expectedClearing: 1},
		{name: "clear the whole minute 60 seconds later", nowShift: 60, reservations: map[int]int64{15: 1, 60: 1, 61: 10}, expectedClearing: 2},
		{name: "clear the whole minute 61 seconds later", nowShift: 61, reservations: map[int]int64{10: 1, 40: 1, 61: 1, 62: 10}, expectedClearing: 3},
		{name: "dont clear value of last refresh", nowShift: 2, reservations: map[int]int64{30: 1, 0: 1, 1: 1, 2: 1, 3: 1}, expectedClearing: 2},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			reservationBuffer := make([]int64, mappingsEntries)
			for expiresAfter, v := range tt.reservations {
				reservationBuffer[reservationSlot(baseSecond+int64(expiresAfter))] += v
			}
			lastCleared := int64(baseSecond + tt.baseLineShift)
			now := int64(baseSecond + tt.nowShift)
			require.Equal(t, tt.expectedClearing, clearReservedMappings(lastCleared, now, reservationBuffer))
		})
	}
}

// A reservation outlives its requested time by MappingDelayInS, and a wall-clock
// step moves no reservation's expiry.
func TestMappingsReservationHold(t *testing.T) {
	cases := []struct {
		name         string
		reservationS int
		// a clear runs with a time before the reservation, as a ticker time can
		laggingClear  bool
		wallClockStep time.Duration // moves the wall-clock reading of every clear time
		expectedHoldS int64
	}{
		{name: "no reservation time reserves nothing", reservationS: 0, expectedHoldS: 0},
		{name: "shortest reservation", reservationS: 1, expectedHoldS: 1 + MappingDelayInS},
		{name: "flush reservation", reservationS: 60, expectedHoldS: 60 + MappingDelayInS},
		{name: "reservation capped at the buffer", reservationS: 120, expectedHoldS: mappingsEntries},
		{name: "flush reservation after a lagging clear", reservationS: 60, laggingClear: true, expectedHoldS: 60 + MappingDelayInS},
		{name: "flush reservation, wall clock steps back 5 s", reservationS: 60, wallClockStep: -5 * time.Second, expectedHoldS: 60 + MappingDelayInS},
		{name: "flush reservation, wall clock steps forward 40 s", reservationS: 60, wallClockStep: 40 * time.Second, expectedHoldS: 60 + MappingDelayInS},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			m := &Monitor{
				maxMemoryMappings:      1000,
				reservedMappingsBuffer: make([]int64, mappingsEntries),
				createdAt:              time.Now().Add(-10 * time.Second),
			}
			clearAt := func(second int64) {
				m.expireReservations(withWallClockStep(t, secondAfterCreation(m, second), tt.wallClockStep))
			}
			require.NoError(t, m.CheckMappingAndReserve(5, tt.reservationS))
			reservedAt := m.lastClearedSecond
			if tt.laggingClear {
				clearAt(reservedAt - 1)
			}

			for s := int64(1); s < tt.expectedHoldS; s++ {
				clearAt(reservedAt + s)
				require.Equal(t, int64(5), m.reservedMappings, "expired %d seconds after reserving", s)
			}
			clearAt(reservedAt + tt.expectedHoldS)
			require.Equal(t, int64(0), m.reservedMappings)
			require.Equal(t, make([]int64, mappingsEntries), m.reservedMappingsBuffer)
		})
	}
}

// Reservations made in the same second share a slot and expire together.
func TestMappingsReservationsInOneSecond(t *testing.T) {
	m := &Monitor{
		maxMemoryMappings:      1000,
		reservedMappingsBuffer: make([]int64, mappingsEntries),
		createdAt:              time.Now(),
	}
	require.NoError(t, m.CheckMappingAndReserve(3, 60))
	require.NoError(t, m.CheckMappingAndReserve(3, 60))
	reservedAt := m.lastClearedSecond
	require.Equal(t, int64(6), m.reservedMappings)

	m.expireReservations(secondAfterCreation(m, reservedAt+60+MappingDelayInS))
	require.Equal(t, int64(0), m.reservedMappings)
}

// The scan calls MappingsReadDue and ReadMappings while shard loads reserve concurrently.
func TestMappingsReadDueWhileReserving(t *testing.T) {
	m := &Monitor{
		maxMemoryMappings:      math.MaxInt64,
		reservedMappingsBuffer: make([]int64, mappingsEntries),
		createdAt:              time.Now(),
		mappingsBuf:            make([]byte, 32*1024),
	}
	logger, _ := test.NewNullLogger()
	scanDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(scanDone)
		for i := 0; i < 1000; i++ {
			if now := time.Now(); m.MappingsReadDue(now) {
				assert.NoError(t, m.ReadMappings(now))
			}
		}
	}, logger)
	for i := 0; i < 1000; i++ {
		require.NoError(t, m.CheckMappingAndReserve(3, 60))
	}
	<-scanDone
}

// The scan asks MappingsReadDue on every tick and reads when it says so.
func TestMappingsReadDue(t *testing.T) {
	const tick = 500 * time.Millisecond
	cases := []struct {
		name     string
		mappings int // lines in the maps file
		max      int64
		early    time.Duration // ticks after the first come this early
		// mappings are admitted half a tick before this tick, if set
		admitBeforeTick int
		failBeforeTick  int // reads before this tick fail
		ticks           int
		expectedReads   []int
	}{
		{name: "below half the limit", mappings: 100, max: 1000, ticks: 130, expectedReads: []int{0, 60, 120}},
		{name: "at half the limit", mappings: 500, max: 1000, ticks: 20, expectedReads: []int{0, 4, 8, 12, 16, 20}},
		{name: "ticks come a nanosecond early", mappings: 500, max: 1000, early: time.Nanosecond, ticks: 20, expectedReads: []int{0, 4, 8, 12, 16, 20}},
		{name: "mappings admitted since the last read", mappings: 100, max: 1000, admitBeforeTick: 21, ticks: 90, expectedReads: []int{0, 21, 81}},
		{name: "a failed read is retried", mappings: 100, max: 1000, failBeforeTick: 6, ticks: 70, expectedReads: []int{0, 4, 8, 68}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mapsPath := writeMapsFile(t, tt.mappings)
			missingPath := filepath.Join(t.TempDir(), "missing")
			start := time.Now()
			m := &Monitor{
				maxMemoryMappings:      tt.max,
				reservedMappingsBuffer: make([]int64, mappingsEntries),
				createdAt:              start,
				mappingsBuf:            make([]byte, 32*1024),
			}

			var reads []int
			for i := 0; i <= tt.ticks; i++ {
				now := start.Add(time.Duration(i) * tick)
				if i > 0 {
					now = now.Add(-tt.early)
				}
				if tt.admitBeforeTick > 0 && i == tt.admitBeforeTick {
					m.lastMappingsAdmitted = now.Add(-tick / 2)
				}
				if !m.MappingsReadDue(now) {
					continue
				}
				reads = append(reads, i)
				failing := i < tt.failBeforeTick
				m.mapsPath = mapsPath
				if failing {
					m.mapsPath = missingPath
				}
				require.Equal(t, failing, m.ReadMappings(now) != nil)
			}
			assert.Equal(t, tt.expectedReads, reads)
		})
	}
}

// A failed read keeps the last count, so shard loads are not checked against 0.
func TestReadMappings(t *testing.T) {
	cases := []struct {
		name         string
		mapsPath     string
		expectErr    bool
		expectedUsed int64
	}{
		{name: "a read stores the count", mapsPath: writeMapsFile(t, 7), expectedUsed: 7},
		{name: "a failed read keeps the last count", mapsPath: filepath.Join(t.TempDir(), "missing"), expectErr: true, expectedUsed: 5},
		{name: "a read failing after the open keeps the last count", mapsPath: t.TempDir(), expectErr: true, expectedUsed: 5},
		{name: "an OS without a maps file counts 0", mapsPath: "", expectedUsed: 0},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			lastRead := time.Now().Add(-time.Minute)
			now := time.Now()
			m := &Monitor{
				usedMappings:     5,
				lastMappingsRead: lastRead,
				mapsPath:         tt.mapsPath,
				mappingsBuf:      make([]byte, 32*1024),
			}

			monitoring.GetMetrics().MmapProcMaps.Set(5)
			err := m.ReadMappings(now)
			assert.Equal(t, tt.expectedUsed, m.usedMappings)
			assert.Equal(t, float64(tt.expectedUsed), testutil.ToFloat64(monitoring.GetMetrics().MmapProcMaps))
			if tt.expectErr {
				require.Error(t, err)
				assert.Equal(t, lastRead, m.lastMappingsRead)
				assert.Equal(t, now, m.lastMappingsReadFailed)
			} else {
				require.NoError(t, err)
				assert.Equal(t, now, m.lastMappingsRead)
			}
		})
	}
}

func TestMappingsRefreshInterval(t *testing.T) {
	nearLimit := MappingDelayInS * time.Second
	cases := []struct {
		name     string
		used     int64
		max      int64
		reserved int64 // expires after the interval is asked for
		expired  int64 // expired when the interval is asked for
		// CheckMappingAndReserve is asked for this many mappings
		admittedBeforeRead int64
		admittedAfterRead  int64
		refusedAfterRead   int64
		readFailedAgo      time.Duration // a read failed this long ago, if set
		expected           time.Duration
	}{
		{name: "just below half the limit", used: 499, max: 1000, expected: mappingsRefreshFarFromLimit},
		{name: "at half the limit", used: 500, max: 1000, expected: nearLimit},
		{name: "past the limit", used: 1200, max: 1000, expected: nearLimit},
		{name: "reservations reach half the limit", used: 400, reserved: 100, max: 1000, expected: nearLimit},
		{name: "expired reservations do not count", used: 400, expired: 100, max: 1000, expected: mappingsRefreshFarFromLimit},
		{name: "mappings admitted since the last read", used: 100, max: 1000, admittedAfterRead: 3, expected: nearLimit},
		{name: "mappings admitted before the last read", used: 100, max: math.MaxInt64, admittedBeforeRead: 3, expected: mappingsRefreshFarFromLimit},
		{name: "refused mappings since the last read", used: 100, max: 1000, refusedAfterRead: 2000, expected: mappingsRefreshFarFromLimit},
		{name: "no limit", used: 0, max: math.MaxInt64, expected: mappingsRefreshFarFromLimit},
		{name: "the last read failed", used: 100, max: 1000, readFailedAgo: 30 * time.Second, expected: nearLimit},
		{name: "a read succeeded after the last failure", used: 100, max: 1000, readFailedAgo: 2 * time.Minute, expected: mappingsRefreshFarFromLimit},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			m := &Monitor{
				maxMemoryMappings:      tt.max,
				usedMappings:           tt.used,
				reservedMappingsBuffer: make([]int64, mappingsEntries),
				createdAt:              now.Add(-time.Minute),
				lastClearedSecond:      59,
				lastMappingsRead:       now.Add(-time.Minute),
			}
			if tt.readFailedAgo > 0 {
				m.lastMappingsReadFailed = now.Add(-tt.readFailedAgo)
			}
			m.reservedMappingsBuffer[reservationSlot(61)] = tt.reserved
			m.reservedMappingsBuffer[reservationSlot(60)] = tt.expired
			m.reservedMappings = tt.reserved + tt.expired

			if tt.admittedBeforeRead > 0 {
				require.NoError(t, m.CheckMappingAndReserve(tt.admittedBeforeRead, 60))
				require.NoError(t, m.ReadMappings(time.Now()))
			}
			if tt.admittedAfterRead > 0 {
				require.NoError(t, m.CheckMappingAndReserve(tt.admittedAfterRead, 60))
			}
			if tt.refusedAfterRead > 0 {
				require.ErrorIs(t, m.CheckMappingAndReserve(tt.refusedAfterRead, 60), enterrors.ErrNotEnoughMappings)
			}

			assert.Equal(t, tt.expected, m.mappingsRefreshInterval(now))
		})
	}
}

// currentMappings counts this process's memory mappings, or 0 outside Linux.
func currentMappings(t *testing.T) int64 {
	t.Helper()
	used, err := getCurrentMappings(procMapsPath(), make([]byte, 32*1024))
	require.NoError(t, err)
	return used
}

func writeMapsFile(t *testing.T, mappings int) string {
	t.Helper()
	file := createTestMappingsFile(t, mappings)
	require.NoError(t, file.Close())
	t.Cleanup(func() { os.Remove(file.Name()) })
	return file.Name()
}

func secondAfterCreation(m *Monitor, second int64) time.Time {
	return m.createdAt.Add(time.Duration(second) * time.Second)
}

// withWallClockStep moves t's wall-clock reading by step and keeps its monotonic
// reading, as an NTP correction does.
func withWallClockStep(tb testing.TB, t time.Time, step time.Duration) time.Time {
	tb.Helper()
	stepped := t
	// with a monotonic reading, the first word of a time.Time holds wall-clock seconds from bit 30
	wall := (*uint64)(unsafe.Pointer(&stepped))
	*wall += uint64(int64(step/time.Second)) << 30
	require.Equal(tb, t.Unix()+int64(step/time.Second), stepped.Unix(), "time.Time no longer has the expected layout")
	require.Zero(tb, stepped.Sub(t), "time.Time no longer has the expected layout")
	return stepped
}

type fakeHeapReader struct {
	val int64
}

func (f fakeHeapReader) Read() int64 {
	return f.val
}

type fakeLimitSetter struct {
	limit int64
}

func (f *fakeLimitSetter) SetMemoryLimit(newLimit int64) int64 {
	if newLimit >= 0 {
		panic("should have been read only")
	}

	return f.limit
}

func createTestMappingsFile(t testing.TB, numMappings int) *os.File {
	file, err := os.CreateTemp("", "test_maps_*")
	require.NoError(t, err)

	for i := 0; i < numMappings; i++ {
		startAddr := fmt.Sprintf("%08x", i*0x1000)
		endAddr := fmt.Sprintf("%08x", (i+1)*0x1000)
		perms := "r-xp"
		offset := fmt.Sprintf("%08x", i*0x1000)
		dev := "00:00"
		inode := fmt.Sprintf("%d", 1000+i)
		pathname := "/lib/x86_64-linux-gnu/libc.so.6"

		line := fmt.Sprintf("%s-%s %s %s %s %s %s\n",
			startAddr, endAddr, perms, offset, dev, inode, pathname)
		_, err := file.WriteString(line)
		require.NoError(t, err)
	}

	_, err = file.Seek(0, 0)
	require.NoError(t, err)

	return file
}

func BenchmarkCurrentMappingsLinuxComparison(b *testing.B) {
	// Create a large simulation file with 100k mappings
	file := createTestMappingsFile(b, 100000)
	defer os.Remove(file.Name())
	defer file.Close()

	filePath := file.Name()

	// Original implementation
	originalCurrentMappingsLinux := func(filePath string) int64 {
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

	b.Run("Original", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := originalCurrentMappingsLinux(filePath)
			if result != 100000 {
				b.Fatalf("Expected 100000 mappings, got %d", result)
			}
		}
	})

	b.Run("New", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 32*1024)
			result, err := currentMappingsLinux(filePath, buf)
			if err != nil {
				b.Fatal(err)
			}
			if result != 100000 {
				b.Fatalf("Expected 100000 mappings, got %d", result)
			}
		}
	})
}

// An object can carry several vector sources at once, and the estimate must count all of them.
func TestEstimateBatchObjectMemory(t *testing.T) {
	namedVectors := func(sizes ...int) []*protocol.Vectors {
		out := make([]*protocol.Vectors, 0, len(sizes))
		for i, size := range sizes {
			out = append(out, &protocol.Vectors{
				Name:        fmt.Sprintf("vector-%d", i),
				VectorBytes: make([]byte, size),
			})
		}
		return out
	}

	cases := []struct {
		name     string
		object   *protocol.BatchObject
		expected int64
	}{
		{
			name:     "legacy vector only",
			object:   &protocol.BatchObject{Vector: make([]float32, 10)},
			expected: 10*4 + 30,
		},
		{
			name:     "vector bytes only",
			object:   &protocol.BatchObject{VectorBytes: make([]byte, 40)},
			expected: 40 + 30,
		},
		{
			name:     "named vectors only",
			object:   &protocol.BatchObject{Vectors: namedVectors(40, 60)},
			expected: 100 + 30,
		},
		{
			name: "legacy vector and named vectors",
			object: &protocol.BatchObject{
				Vector:  make([]float32, 10),
				Vectors: namedVectors(40),
			},
			expected: 10*4 + 40 + 30,
		},
		{
			name: "every source at once",
			object: &protocol.BatchObject{
				Vector:      make([]float32, 10),
				VectorBytes: make([]byte, 40),
				Vectors:     namedVectors(40, 60, 120),
			},
			expected: 10*4 + 40 + 220 + 30,
		},
		{
			// this pins current behavior without endorsing it: an object with no
			// vector costs nothing at the memory check
			name:     "no vector at all",
			object:   &protocol.BatchObject{},
			expected: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, EstimateBatchObjectMemory(tc.object))
		})
	}
}
