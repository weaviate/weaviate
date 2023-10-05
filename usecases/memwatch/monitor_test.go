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
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMonitor(t *testing.T) {
	t.Run("with constant profiles (no changes)", func(t *testing.T) {
		profiler := &fakeMemProfiler{
			copyProfiles: [][]runtime.MemProfileRecord{
				{sampleProfile(10000), sampleProfile(20000)},
				{sampleProfile(10000), sampleProfile(20000)},
			},
		}

		limiter := &fakeLimitSetter{limit: 100000}

		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 1, 0.97)
		m.Refresh()

		assert.Equal(t, 0.3, m.Ratio())
	})

	t.Run("with one more profile on second call", func(t *testing.T) {
		profiler := &fakeMemProfiler{
			copyProfiles: [][]runtime.MemProfileRecord{
				{sampleProfile(10000)},
				{sampleProfile(10000), sampleProfile(20000)},
			},
		}

		limiter := &fakeLimitSetter{limit: 100000}

		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 1, 0.97)

		m.Refresh()
		assert.Equal(t, 0.3, m.Ratio())
	})

	t.Run("with less memory than the threshold", func(t *testing.T) {
		profiler := &fakeMemProfiler{
			copyProfiles: [][]runtime.MemProfileRecord{
				{sampleProfile(700 * MiB)},
				{sampleProfile(700 * MiB)},
				{sampleProfile(700 * MiB)},
			},
		}

		limiter := &fakeLimitSetter{limit: 1 * GiB}

		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 1, 0.97)
		m.Refresh()

		err := m.CheckAlloc(100 * MiB)
		assert.NoError(t, err, "with 700 allocated, an additional 100 would be about 80% which is not a problem")

		err = m.CheckAlloc(299 * MiB)
		assert.Error(t, err, "with 700 allocated, an additional 299 would be about 97.5% which is not allowed")

		err = m.CheckAlloc(400 * MiB)
		assert.Error(t, err, "with 700 allocated, an additional 400 would be about 110% which is not allowed")
	})

	t.Run("with memory already over the threshold", func(t *testing.T) {
		profiler := &fakeMemProfiler{
			copyProfiles: [][]runtime.MemProfileRecord{
				{sampleProfile(1025 * MiB)},
				{sampleProfile(1025 * MiB)},
				{sampleProfile(1025 * MiB)},
			},
		}

		limiter := &fakeLimitSetter{limit: 1 * GiB}

		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 1, 0.97)
		m.Refresh()

		err := m.CheckAlloc(1 * B)
		assert.Error(t, err,
			"any check should fail, since we're already over the limit")

		err = m.CheckAlloc(10 * MiB)
		assert.Error(t, err, "any check should fail, since we're already over the limit")

		err = m.CheckAlloc(1 * TiB)
		assert.Error(t, err, "any check should fail, since we're already over the limit")
	})

	t.Run("with sampling rate", func(t *testing.T) {
		profiler := &fakeMemProfiler{
			copyProfiles: [][]runtime.MemProfileRecord{
				{sampleProfile(10000)},
				{sampleProfile(10000), sampleProfile(20000)},
			},
		}

		limiter := &fakeLimitSetter{limit: 100000}

		// sample rate is small enough to not alter the result, yet big enough to
		// cover the calculation
		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 5, 0.97)

		m.Refresh()
		assert.Equal(t, 0.3, m.Ratio())
	})

	t.Run("with real dependencies", func(t *testing.T) {
		m := NewMonitor(runtime.MemProfile, debug.SetMemoryLimit, runtime.MemProfileRate, 0.97)
		_ = m.Ratio()
	})
}

type fakeMemProfiler struct {
	copyProfiles [][]runtime.MemProfileRecord
	call         int
}

func (f *fakeMemProfiler) MemProfile(p []runtime.MemProfileRecord, inUseZero bool) (int, bool) {
	profiles := f.copyProfiles[f.call]
	f.call++

	if len(p) >= len(profiles) {
		copy(p, profiles)
		return len(profiles), true
	}

	return len(profiles), false
}

func sampleProfile(in int64) runtime.MemProfileRecord {
	return runtime.MemProfileRecord{
		AllocBytes:   in,
		FreeBytes:    0,
		AllocObjects: 1,
		FreeObjects:  0,
	}
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
