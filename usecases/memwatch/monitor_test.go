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
	"errors"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			assert.Greater(t, getCurrentMappings(), int64(0))
			assert.Less(t, getCurrentMappings(), int64(math.MaxInt64))
		case "darwin":
			assert.Equal(t, getCurrentMappings(), int64(0))
		}
	})

	t.Run("check mappings, by open many file mappings and close them only after the test is done", func(t *testing.T) {
		if runtime.GOOS == "darwin" {
			t.Skip("macOS does not have a limit on mappings")
		}
		currentMappings := getCurrentMappings()
		addMappings := 15
		t.Setenv("MAX_MEMORY_MAPPINGS", strconv.FormatInt(currentMappings+int64(addMappings), 10))
		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh(true)

		mappingsLeft := getMaxMemoryMappings() - currentMappings
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
			if mappingsLeft := getMaxMemoryMappings() - getCurrentMappings(); mappingsLeft <= 0 {
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
		currentMappings := getCurrentMappings()
		addMappings := 15
		t.Setenv("MAX_MEMORY_MAPPINGS", strconv.FormatInt(currentMappings+int64(addMappings), 10))
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
	baseTime, err := time.Parse(time.RFC3339, "2021-01-01T00:00:30Z")
	require.Nil(t, err)
	cases := []struct {
		name             string
		baseLineShift    int
		nowShift         int
		reservations     map[int]int64
		expectedClearing int64
	}{
		{name: "no reservations", reservations: map[int]int64{}, expectedClearing: 0},
		{name: "reservations present, no expiration", nowShift: 1, reservations: map[int]int64{1: 45, 2: 14}, expectedClearing: 0},
		{name: "reservations present, one expiration", nowShift: 1, reservations: map[int]int64{1: 45, 31: 14}, expectedClearing: 14},
		{name: "reservations present, clear all", nowShift: 62, reservations: map[int]int64{0: 1, 1: 1, 2: 1, 59: 1}, expectedClearing: 4},
		{name: "reservations present, clear nothing (same time)", reservations: map[int]int64{0: 1, 30: 1, 2: 1, 59: 1}, expectedClearing: 0},
		{name: "clear range", nowShift: 20, reservations: map[int]int64{0: 10, 29: 10, 30: 1, 31: 1, 50: 1, 51: 10}, expectedClearing: 2},
		{name: "clear over minute wraparound", nowShift: 45, reservations: map[int]int64{0: 1, 1: 1, 2: 1, 29: 10, 59: 1}, expectedClearing: 4},
		{name: "dont clear value of last refresh", nowShift: 2, reservations: map[int]int64{0: 1, 30: 1, 31: 1, 32: 1, 33: 1}, expectedClearing: 2},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			baseTimeLocal := baseTime.Add(time.Duration(tt.baseLineShift) * time.Second)
			now := baseTime.Add(time.Duration(tt.nowShift) * time.Second)
			reservationBuffer := make([]int64, 62)
			for i, v := range tt.reservations {
				reservationBuffer[i] = v
			}
			require.Equal(t, clearReservedMappings(baseTimeLocal, now, reservationBuffer), tt.expectedClearing)
		})
	}
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
