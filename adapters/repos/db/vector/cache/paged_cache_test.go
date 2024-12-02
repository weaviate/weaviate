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

package cache

import (
	"context"
	"math"
	"math/rand"
	"path"
	"runtime"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestFloat32Page(t *testing.T) {
	t.Run("extract vector", func(t *testing.T) {
		page := newFloat32Page()
		page.add(0, []float32{3, 1, 5})
		page.add(1, []float32{2, 3, 1})
		page.add(2, []float32{3, 4, 6})

		_, err := page.extractVector(10)
		assert.NotNil(t, err)

		v, err := page.extractVector(1)
		assert.Nil(t, err)
		assert.Equal(t, float32(2), v[0])
		assert.Equal(t, float32(3), v[1])
		assert.Equal(t, float32(1), v[2])
	})

	t.Run("store/load", func(t *testing.T) {
		page := newFloat32Page()
		page.add(0, []float32{3, 1, 5})
		page.add(1, []float32{2, 3, 1})
		page.add(2, []float32{3, 4, 6})
		path := path.Join(t.TempDir(), "0.bin")
		page.store(path)

		page = newFloat32Page()
		page.dimensions = 3
		page.load(path, 3)

		_, err := page.extractVector(10)
		assert.NotNil(t, err)

		v, err := page.extractVector(1)
		assert.Nil(t, err)
		assert.Equal(t, float32(2), v[0])
		assert.Equal(t, float32(3), v[1])
		assert.Equal(t, float32(1), v[2])
	})
}

func TestBytePage(t *testing.T) {
	t.Run("extract vector", func(t *testing.T) {
		page := newBytePage()
		page.add(0, []byte{3, 1, 5})
		page.add(1, []byte{2, 3, 1})
		page.add(2, []byte{3, 4, 6})

		_, err := page.extractVector(10)
		assert.NotNil(t, err)

		v, err := page.extractVector(1)
		assert.Nil(t, err)
		assert.Equal(t, byte(2), v[0])
		assert.Equal(t, byte(3), v[1])
		assert.Equal(t, byte(1), v[2])
	})
}

func TestUint64Page(t *testing.T) {
	t.Run("extract vector", func(t *testing.T) {
		page := newUint64Page()
		page.add(0, []uint64{3, 1, 5})
		page.add(1, []uint64{2, 3, 1})
		page.add(2, []uint64{3, 4, 6})

		_, err := page.extractVector(10)
		assert.NotNil(t, err)

		v, err := page.extractVector(1)
		assert.Nil(t, err)
		assert.Equal(t, uint64(2), v[0])
		assert.Equal(t, uint64(3), v[1])
		assert.Equal(t, uint64(1), v[2])
	})
}

func TestFloat32PagedCache(t *testing.T) {
	logger := logrus.New()
	t.Run("preload and get", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewCosineDistanceProvider(), logger)
		cache.Preload(0, []float32{0, 2, 16})
		cache.Preload(1, []float32{1, 12, 6})

		callerId := cache.GetFreeCallerId()
		defer cache.ReturnCallerId(callerId)

		vec, err := cache.Get(context.Background(), callerId, 0)
		assert.Nil(t, err)
		assert.Equal(t, []float32{0, 2, 16}, vec)

		vec, err = cache.Get(context.Background(), callerId, 1)
		assert.Nil(t, err)
		assert.Equal(t, []float32{1, 12, 6}, vec)

		_, err = cache.Get(context.Background(), callerId, 2)
		assert.NotNil(t, err)
	})

	t.Run("preload and multiget", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewCosineDistanceProvider(), logger)
		cache.Preload(0, []float32{0, 2, 16})
		cache.Preload(1, []float32{1, 12, 6})

		vecs, errs := cache.MultiGet(context.Background(), 0, []uint64{0, 1})
		assert.Equal(t, int(2), len(errs))
		for _, err := range errs {
			assert.Nil(t, err)
		}
		assert.Equal(t, []float32{0, 2, 16}, vecs[0])
		assert.Equal(t, []float32{1, 12, 6}, vecs[1])
	})

	t.Run("connect", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewCosineDistanceProvider(), logger)
		cache.Preload(0, []float32{0, 2, 16})
		cache.Preload(1, []float32{1, 12, 6})

		callerId := cache.GetFreeCallerId()

		cache.Connect(callerId, 0, 0)
		cacheId := cache.pageForId[0]
		assert.Equal(t, int(0), cacheId)
		cache.Connect(callerId, 1, 0)
		cacheId = cache.pageForId[1]
		assert.Equal(t, int(0), cacheId)

		vec, err := cache.Get(context.Background(), callerId, 0)
		assert.Nil(t, err)
		assert.Equal(t, []float32{0, 2, 16}, vec)

		vec, err = cache.Get(context.Background(), callerId, 1)
		assert.Nil(t, err)
		assert.Equal(t, []float32{1, 12, 6}, vec)

		_, err = cache.Get(context.Background(), callerId, 2)
		assert.NotNil(t, err)
	})

	t.Run("splits", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewCosineDistanceProvider(), logger)
		callerId := cache.GetFreeCallerId()
		for i := 0; i < 110; i++ {
			cache.Preload(uint64(i), []float32{rand.Float32(), rand.Float32(), rand.Float32()})
			cache.Connect(callerId, uint64(i), 0)
		}
		total := 0
		for i := 0; i < 110; i++ {
			pageId := cache.pageForId[i]
			assert.True(t, pageId < 10)
			total += pageId
		}
		assert.True(t, total > 0)
	})

	t.Run("prefetch", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewCosineDistanceProvider(), logger)
		cache.Preload(0, []float32{0, 2, 16})
		cache.Preload(1, []float32{1, 12, 6})
		callerId := cache.GetFreeCallerId()
		cache.Connect(callerId, 0, 0)
		cache.Connect(callerId, 1, 0)
		cache.pages[0].store(cache.fileNameForId(0))
		cache.pages[0] = nil
		cache.Prefetch(callerId, 0)
		assert.NotNil(t, cache.pages[0])
		vec, err := cache.Get(context.Background(), callerId, 0)
		assert.Nil(t, err)
		assert.Equal(t, []float32{0, 2, 16}, vec)
	})

	t.Run("clean memory", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewCosineDistanceProvider(), logger)
		callerId := cache.GetFreeCallerId()
		cache.Preload(0, []float32{0, 2, 16})
		cache.Preload(1, []float32{1, 12, 6})
		cache.Connect(callerId, 0, 0)
		cache.Connect(callerId, 1, 0)
		cache.ReturnCallerId(callerId)
		cache.CollectMemory()
		assert.Nil(t, cache.pages[0])
		callerId = cache.GetFreeCallerId()
		cache.Prefetch(callerId, 0)
		assert.NotNil(t, cache.pages[0])
		vec, err := cache.Get(context.Background(), callerId, 0)
		assert.Nil(t, err)
		assert.Equal(t, []float32{0, 2, 16}, vec)
	})

	t.Run("clean memory keeps what is in use", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewCosineDistanceProvider(), logger)
		callerId := cache.GetFreeCallerId()
		cache.Preload(0, []float32{0, 2, 16})
		cache.Preload(1, []float32{1, 12, 6})
		cache.Connect(callerId, 0, 0)
		cache.Connect(callerId, 1, 0)
		cache.ReturnCallerId(callerId)
		cache.CollectMemory()
		assert.Nil(t, cache.pages[0])
		callerId = cache.GetFreeCallerId()
		cache.Prefetch(callerId, 0)
		assert.NotNil(t, cache.pages[0])
		vec, err := cache.Get(context.Background(), callerId, 0)
		assert.Nil(t, err)
		assert.Equal(t, []float32{0, 2, 16}, vec)
		cache.CollectMemory()
		assert.NotNil(t, cache.pages[0])
	})

	t.Run("concurrent chaos", func(t *testing.T) {
		cache := NewFloat32PagedCache(t.TempDir(), 100, distancer.NewL2SquaredProvider(), logger)
		n := 300
		n64 := float64(n)
		workerCount := runtime.GOMAXPROCS(0)
		wg := &sync.WaitGroup{}
		split := uint64(math.Ceil(n64 / float64(workerCount)))
		for worker := uint64(0); worker < uint64(workerCount); worker++ {
			workerID := worker

			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				callerId := cache.GetFreeCallerId()
				for i := workerID * split; i < uint64(math.Min(float64((workerID+1)*split), n64)); i++ {
					cache.Preload(i, []float32{rand.Float32(), rand.Float32(), rand.Float32()})
					cache.Connect(callerId, i, 0)
				}
				cache.ReturnCallerId(callerId)
			}, logger)
		}
		wg.Wait()
		callerId := cache.GetFreeCallerId()
		vec, err := cache.Get(context.Background(), callerId, 0)
		cache.ReturnCallerId(callerId)
		assert.Nil(t, err)
		assert.NotNil(t, vec)
	})
}

func TestUint64PagedCache(t *testing.T) {
	cache := NewUint64PagedCache(t.TempDir(), 100)
	cache.Preload(0, []uint64{0, 2, 16})
	cache.Preload(1, []uint64{1, 12, 6})

	callerId := cache.GetFreeCallerId()
	defer cache.ReturnCallerId(callerId)
	vec, err := cache.Get(context.Background(), callerId, 0)
	assert.Nil(t, err)
	assert.Equal(t, []uint64{0, 2, 16}, vec)

	vec, err = cache.Get(context.Background(), callerId, 1)
	assert.Nil(t, err)
	assert.Equal(t, []uint64{1, 12, 6}, vec)

	_, err = cache.Get(context.Background(), callerId, 2)
	assert.NotNil(t, err)
}

func TestBytePagedCache(t *testing.T) {
	cache := NewBytePagedCache(t.TempDir(), 100)
	cache.Preload(0, []byte{0, 2, 16})
	cache.Preload(1, []byte{1, 12, 6})

	callerId := cache.GetFreeCallerId()
	defer cache.ReturnCallerId(callerId)
	vec, err := cache.Get(context.Background(), callerId, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0, 2, 16}, vec)

	vec, err = cache.Get(context.Background(), callerId, 1)
	assert.Nil(t, err)
	assert.Equal(t, []byte{1, 12, 6}, vec)

	_, err = cache.Get(context.Background(), callerId, 2)
	assert.NotNil(t, err)
}
