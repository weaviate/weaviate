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
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMonitor(t *testing.T) {
	t.Run("with constant profiles (no changes)", func(t *testing.T) {
		metrics := &fakeHeapReader{val: 30000}
		limiter := &fakeLimitSetter{limit: 100000}

		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh()

		assert.Equal(t, 0.3, m.Ratio())
	})

	t.Run("with less memory than the threshold", func(t *testing.T) {
		metrics := &fakeHeapReader{val: 700 * MiB}
		limiter := &fakeLimitSetter{limit: 1 * GiB}

		m := NewMonitor(metrics.Read, limiter.SetMemoryLimit, 0.97)
		m.Refresh()

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
		m.Refresh()

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
