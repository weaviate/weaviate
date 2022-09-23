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

		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 1)

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

		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 1)

		assert.Equal(t, 0.3, m.Ratio())
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
		m := NewMonitor(profiler.MemProfile, limiter.SetMemoryLimit, 5)

		assert.Equal(t, 0.3, m.Ratio())
	})

	t.Run("with real dependencies", func(t *testing.T) {
		m := NewMonitor(runtime.MemProfile, debug.SetMemoryLimit, runtime.MemProfileRate)
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
