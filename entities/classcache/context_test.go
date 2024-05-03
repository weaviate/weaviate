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

func Test_ClassFromContext(t *testing.T) {
	t.Run("fails getting class from context without cache", func(t *testing.T) {
		noCacheCtx := context.Background()

		class, version, err := ClassFromContext(noCacheCtx, "class1", noopGetter)
		assert.Nil(t, class)
		assert.Equal(t, uint64(0), version)
		assert.ErrorContains(t, err, "context does not contain classCache")
	})

	t.Run("fails getting class from context with invalid cache", func(t *testing.T) {
		invalidCacheCtx := context.WithValue(context.Background(), classCacheKey, "stringInsteadClassCache")

		class, version, err := ClassFromContext(invalidCacheCtx, "class1", noopGetter)
		assert.Nil(t, class)
		assert.Equal(t, uint64(0), version)
		assert.ErrorContains(t, err, "context does not contain classCache")
	})

	t.Run("uses getter to init class cache if miss", func(t *testing.T) {
		cacheCtx := ContextWithClassCache(context.Background())
		getter := createCounterGetter(0)

		class1_1, version1_1, err1_1 := ClassFromContext(cacheCtx, "class1", getter)
		assert.NoError(t, err1_1)
		assert.Equal(t, uint64(1), version1_1)
		require.NotNil(t, class1_1)
		assert.Equal(t, "class1", class1_1.Class)

		class2_1, version2_1, err2_1 := ClassFromContext(cacheCtx, "class2", getter)
		assert.NoError(t, err2_1)
		assert.Equal(t, uint64(2), version2_1)
		require.NotNil(t, class2_1)
		assert.Equal(t, "class2", class2_1.Class)

		class1_2, version1_2, err1_2 := ClassFromContext(cacheCtx, "class1", getter)
		assert.NoError(t, err1_2)
		assert.Equal(t, uint64(1), version1_2)
		require.NotNil(t, class1_2)
		assert.Equal(t, "class1", class1_2.Class)

		class2_2, version2_2, err2_2 := ClassFromContext(cacheCtx, "class2", getter)
		assert.NoError(t, err2_2)
		assert.Equal(t, uint64(2), version2_2)
		require.NotNil(t, class2_2)
		assert.Equal(t, "class2", class2_2.Class)
	})

	t.Run("does not cache class if getter fails", func(t *testing.T) {
		cacheCtx := ContextWithClassCache(context.Background())
		getter := createErrorGetter()

		class1_1, version1_1, err1_1 := ClassFromContext(cacheCtx, "class1", getter)
		assert.Nil(t, class1_1)
		assert.Equal(t, uint64(0), version1_1)
		assert.ErrorContains(t, err1_1, "error getting class class1, count_1")

		class1_2, version1_2, err1_2 := ClassFromContext(cacheCtx, "class1", getter)
		assert.Nil(t, class1_2)
		assert.Equal(t, uint64(0), version1_2)
		assert.ErrorContains(t, err1_2, "error getting class class1, count_2")

		class1_3, version1_3, err1_3 := ClassFromContext(cacheCtx, "class1", getter)
		assert.Nil(t, class1_3)
		assert.Equal(t, uint64(0), version1_3)
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
				classes[i], versions[i], errors[i] = ClassFromContext(cacheCtx, "class1", getter)
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

func noopGetter(name string) (*models.Class, uint64, error) {
	return nil, 0, nil
}

func createErrorGetter() func(name string) (*models.Class, uint64, error) {
	errorCounter := uint64(0)
	return func(name string) (*models.Class, uint64, error) {
		return nil, 0, fmt.Errorf("error getting class %s, count_%d", name, atomic.AddUint64(&errorCounter, 1))
	}
}

func createCounterGetter(sleep time.Duration) func(name string) (*models.Class, uint64, error) {
	versionCounter := uint64(0)
	return func(name string) (*models.Class, uint64, error) {
		if sleep > 0 {
			time.Sleep(sleep)
		}
		version := atomic.AddUint64(&versionCounter, 1)
		return &models.Class{
			Class:       name,
			Description: fmt.Sprintf("description_%d", version),
		}, version, nil
	}
}
