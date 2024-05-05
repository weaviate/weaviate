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

package classcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
)

func Test_ContextWithClassCache(t *testing.T) {
	t.Run("adds cache to context if not present", func(t *testing.T) {
		ctx := context.Background()
		cacheCtx := ContextWithClassCache(ctx)

		assert.Nil(t, ctx.Value(classCacheKey))
		assert.NotNil(t, cacheCtx.Value(classCacheKey))
	})

	t.Run("does not add cache to context if already present", func(t *testing.T) {
		ctx := context.Background()
		cacheCtx1 := ContextWithClassCache(ctx)
		cacheCtx2 := ContextWithClassCache(cacheCtx1)

		cache1 := cacheCtx1.Value(classCacheKey)
		cache2 := cacheCtx2.Value(classCacheKey)

		assert.NotNil(t, cache1)
		assert.NotNil(t, cache2)
		assert.True(t, cacheCtx1 == cacheCtx2) // same context instance
		assert.True(t, cache1 == cache2)       // same cache instance
	})
}

func Test_ClassesFromContext(t *testing.T) {
	t.Run("fails getting class from context without cache", func(t *testing.T) {
		noCacheCtx := context.Background()

		vclasses, err := ClassesFromContext(noCacheCtx, noopGetter, "class1")
		_, exists := vclasses["class1"]
		assert.False(t, exists)
		assert.NotContains(t, vclasses, "class1")
		assert.ErrorContains(t, err, "context does not contain classCache")
	})

	t.Run("fails getting class from context with invalid cache", func(t *testing.T) {
		invalidCacheCtx := context.WithValue(context.Background(), classCacheKey, "stringInsteadClassCache")

		vclasses, err := ClassesFromContext(invalidCacheCtx, noopGetter, "class1")
		_, exists := vclasses["class1"]
		assert.False(t, exists)
		assert.NotContains(t, vclasses, "class1")
		assert.ErrorContains(t, err, "context does not contain classCache")
	})

	t.Run("uses getter to init class cache if miss", func(t *testing.T) {
		cacheCtx := ContextWithClassCache(context.Background())
		getter := createCounterGetter(0)

		vclasses_1, err_1 := ClassesFromContext(cacheCtx, getter, "class1", "class2")
		assert.NoError(t, err_1)

		vclass1 := vclasses_1["class1"]
		assert.Equal(t, uint64(1), vclass1.Version)
		require.NotNil(t, vclass1.Class)
		assert.Equal(t, "class1", vclass1.Class.Class)

		vclass2 := vclasses_1["class2"]
		assert.Equal(t, uint64(2), vclass2.Version)
		require.NotNil(t, vclass2)
		assert.Equal(t, "class2", vclass2.Class.Class)

		vclasses_2, err_2 := ClassesFromContext(cacheCtx, getter, "class1", "class2")
		assert.NoError(t, err_2)

		vclass1 = vclasses_2["class1"]
		assert.Equal(t, uint64(1), vclass1.Version)
		require.NotNil(t, vclass1.Class)
		assert.Equal(t, "class1", vclass1.Class.Class)

		vclass2 = vclasses_2["class2"]
		assert.Equal(t, uint64(2), vclass2.Version)
		require.NotNil(t, vclass2)
		assert.Equal(t, "class2", vclass2.Class.Class)
	})

	t.Run("does not cache class if getter fails", func(t *testing.T) {
		cacheCtx := ContextWithClassCache(context.Background())
		getter := createErrorGetter()

		vclasses, err1_1 := ClassesFromContext(cacheCtx, getter, "class1")
		class1_1, exists1_1 := vclasses["class1"]
		assert.False(t, exists1_1)
		assert.Equal(t, uint64(0), class1_1.Version)
		assert.ErrorContains(t, err1_1, "error getting class class1, count_1")

		vclasses, err1_2 := ClassesFromContext(cacheCtx, getter, "class1")
		class1_2, exists_1_2 := vclasses["class1"]
		assert.False(t, exists_1_2)
		assert.Equal(t, uint64(0), class1_2.Version)
		assert.ErrorContains(t, err1_2, "error getting class class1, count_2")

		vclasses, err1_3 := ClassesFromContext(cacheCtx, getter, "class1")
		class1_3, exists_1_3 := vclasses["class1"]
		assert.False(t, exists_1_3)
		assert.Equal(t, uint64(0), class1_3.Version)
		assert.ErrorContains(t, err1_3, "error getting class class1, count_3")
	})

	t.Run("does not overwrite cache when multiple concurrent loads", func(t *testing.T) {
		cacheCtx := ContextWithClassCache(context.Background())
		getter := createCounterGetter(50 * time.Millisecond)

		concurrency := 20
		classes := make([]*models.Class, concurrency)
		versions := make([]uint64, concurrency)
		errors := make([]error, concurrency)

		wg := new(sync.WaitGroup)
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			i := i
			go func() {
				vclasses, err := ClassesFromContext(cacheCtx, getter, "class1")
				errors[i] = err
				classes[i] = vclasses["class1"].Class
				versions[i] = vclasses["class1"].Version
				wg.Done()
			}()
		}
		wg.Wait()

		// Same class for all calls, same versions for all calls, no errors.
		// It is undetermined which getter call will be stored, but it should be shared
		// across all results
		for i := 0; i < concurrency; i++ {
			assert.NoError(t, errors[i])
			assert.Equal(t, versions[0], versions[i])
			require.NotNil(t, classes[i])
			assert.Equal(t, fmt.Sprintf("description_%d", versions[0]), classes[i].Description)
		}
	})
}

func noopGetter(names ...string) (map[string]versioned.Class, error) {
	return nil, nil
}

func createErrorGetter() func(names ...string) (map[string]versioned.Class, error) {
	errorCounter := uint64(0)
	return func(names ...string) (map[string]versioned.Class, error) {
		return nil, fmt.Errorf("error getting class %s, count_%d", names[0], atomic.AddUint64(&errorCounter, 1))
	}
}

func createCounterGetter(sleep time.Duration) func(names ...string) (map[string]versioned.Class, error) {
	versionCounter := uint64(0)
	return func(names ...string) (map[string]versioned.Class, error) {
		if sleep > 0 {
			time.Sleep(sleep)
		}
		res := make(map[string]versioned.Class, len(names))

		for _, name := range names {
			version := atomic.AddUint64(&versionCounter, 1)
			res[name] = versioned.Class{
				Version: version,
				Class: &models.Class{
					Class:       name,
					Description: fmt.Sprintf("description_%d", version),
				},
			}
		}

		return res, nil
	}
}
