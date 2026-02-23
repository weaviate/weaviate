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

package lsmkv

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/concurrency"
	"golang.org/x/sync/errgroup"
)

func TestConsistentViewCache(t *testing.T) {
	logger, _ := test.NewNullLogger()

	newKV := func(key string, val uint16) map[string][]byte {
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, val)
		return map[string][]byte{key: buf}
	}
	newMemtable := func(name string, id uint16) memtable {
		return newTestMemtableReplace(newKV(name, id))
	}
	newSegment := func(name string, id uint16) Segment {
		return newFakeReplaceSegment(newKV(name, id))
	}
	newConsistentView := func(id uint16, releasedViewCallback func(uint16)) BucketConsistentView {
		return BucketConsistentView{
			Active:   newMemtable("active", id),
			Flushing: newMemtable("flushing", id),
			Disk:     []Segment{newSegment("segment", id)},
			release:  func() { releasedViewCallback(id) },
		}
	}
	newCreateConsistentView := func() (func() BucketConsistentView, func() (int, []uint16)) {
		createdCounter := new(atomic.Uint32)
		lock := new(sync.Mutex)
		releasedViewIds := make([]uint16, 0, 16)
		releasedViewCallback := func(id uint16) {
			lock.Lock()
			defer lock.Unlock()
			releasedViewIds = append(releasedViewIds, id)
		}

		return func() BucketConsistentView {
				return newConsistentView(uint16(createdCounter.Add(1)), releasedViewCallback)
			}, func() (int, []uint16) {
				lock.Lock()
				defer lock.Unlock()
				return int(createdCounter.Load()), append(make([]uint16, 0, len(releasedViewIds)), releasedViewIds...)
			}
	}

	concurrently := func(concurrency, count int, callback func()) (wait func() error) {
		eg := new(errgroup.Group)
		eg.SetLimit(concurrency)
		for range count {
			eg.Go(func() error {
				callback()
				return nil
			})
		}
		return eg.Wait
	}

	assertViewId := func(t *testing.T, expectedId uint16, view BucketConsistentView) {
		t.Helper()

		bId, err := view.Active.get([]byte("active"))
		require.NoError(t, err)
		assert.Equal(t, expectedId, binary.LittleEndian.Uint16(bId))
	}

	t.Run("cache does not create consistent view without calling Get", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()

		NewConsistentViewCache(logger, createConsistentView)

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 0, countCreatedViews)
		assert.Empty(t, releasedViewIds)
	})

	t.Run("consistent view is created after first Get call", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 0, countCreatedViews)
		assert.Empty(t, releasedViewIds)

		view := cache.Get()
		defer view.ReleaseView()
		assertViewId(t, 1, view)

		countCreatedViews, releasedViewIds = getStats()
		assert.Equal(t, 1, countCreatedViews)
		assert.Empty(t, releasedViewIds)
	})

	t.Run("multiple Get calls return same consistent view", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()

		cache := NewConsistentViewCache(logger, createConsistentView)

		concurrently(concurrency.NUMCPU, 50, func() {
			view := cache.Get()
			defer view.ReleaseView()
			assertViewId(t, 1, view)
		})()

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 1, countCreatedViews)
		assert.Empty(t, releasedViewIds)
	})

	t.Run("Invalidate call on empty cache does not create consistent view", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		cache.Invalidate()

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 0, countCreatedViews)
		assert.Empty(t, releasedViewIds)
	})

	t.Run("Invalidate call on non-empty cache does not create new consistent view", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)
		view := cache.Get()
		defer view.ReleaseView()

		cache.Invalidate()

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 1, countCreatedViews)
		assert.Empty(t, releasedViewIds)
	})

	t.Run("Get calls create new consistent views after each invalidation", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		views := make([]BucketConsistentView, 5)
		for i := range views {
			views[i] = cache.Get()
			assertViewId(t, uint16(i+1), views[i])
			cache.Invalidate()
		}
		defer func() {
			for i := range views {
				views[i].ReleaseView()
			}
		}()

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 5, countCreatedViews)
		assert.Empty(t, releasedViewIds)
	})

	t.Run("multiple consecutive Invalidate calls do not create new consistent view", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		view := cache.Get()
		defer view.ReleaseView()
		assertViewId(t, 1, view)

		for range 3 {
			cache.Invalidate()
		}

		view = cache.Get()
		defer view.ReleaseView()
		assertViewId(t, 2, view)

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 2, countCreatedViews)
		assert.Empty(t, releasedViewIds)
	})

	t.Run("consistent view is released only after invalidation", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		view := cache.Get()
		view.ReleaseView()

		_, releasedViewIds := getStats()
		assert.Empty(t, releasedViewIds)

		cache.Invalidate()

		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, releasedViewIds = getStats()
			assert.Equal(ct, []uint16{1}, releasedViewIds)
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("consistent view is released once after multiple invalidations", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		view := cache.Get()
		view.ReleaseView()

		_, releasedViewIds := getStats()
		assert.Empty(t, releasedViewIds)

		for range 3 {
			cache.Invalidate()
		}

		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, releasedViewIds = getStats()
			assert.Equal(ct, []uint16{1}, releasedViewIds)
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("consistent view is not released until cached view not released and cache invalidated", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		view1 := cache.Get()
		assertViewId(t, uint16(1), view1)
		cache.Invalidate()

		view2 := cache.Get()
		assertViewId(t, uint16(2), view2)
		cache.Invalidate()

		view3 := cache.Get()
		assertViewId(t, uint16(3), view3)
		cache.Invalidate()

		countCreatedViews, releasedViewIds := getStats()
		assert.Equal(t, 3, countCreatedViews)
		assert.Empty(t, releasedViewIds)

		view3.ReleaseView()
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, releasedViewIds = getStats()
			assert.Equal(ct, []uint16{3}, releasedViewIds)
		}, 5*time.Second, 50*time.Millisecond)

		view1.ReleaseView()
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, releasedViewIds = getStats()
			assert.Equal(ct, []uint16{3, 1}, releasedViewIds)
		}, 5*time.Second, 50*time.Millisecond)

		view2.ReleaseView()
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, releasedViewIds = getStats()
			assert.Equal(ct, []uint16{3, 1, 2}, releasedViewIds)
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("Get calls return only new view after invalidation", func(t *testing.T) {
		createConsistentView, getStats := newCreateConsistentView()
		cache := NewConsistentViewCache(logger, createConsistentView)

		invalidatedCh := make(chan struct{})
		wait1 := concurrently(50, 50, func() {
			// may be called before or after invalidation. view not determined (could be 1st or 2nd)
			view1or2 := cache.Get()
			view1or2.ReleaseView()
		})
		wait2 := concurrently(50, 50, func() {
			// should be called after invalidation. only 2nd view is expected
			<-invalidatedCh
			view2 := cache.Get()
			assertViewId(t, uint16(2), view2)
			view2.ReleaseView()
		})

		// ensure 1st view is created before invalidation
		view1 := cache.Get()
		view1.ReleaseView()

		cache.Invalidate()
		close(invalidatedCh)
		wait1()
		wait2()

		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			countCreatedViews, releasedViewIds := getStats()
			assert.Equal(t, 2, countCreatedViews)
			assert.Equal(ct, []uint16{1}, releasedViewIds)
		}, 5*time.Second, 50*time.Millisecond)
	})
}
