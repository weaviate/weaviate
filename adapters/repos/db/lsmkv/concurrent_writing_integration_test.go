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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// This test continuously writes into a bucket with a small memtable threshold,
// so that a lot of flushing is happening while writing. This is to ensure that
// there will be no lost writes or other inconsistencies under load
func TestConcurrentWriting_Replace(t *testing.T) {
	dirName := t.TempDir()

	amount := 2000
	sizePerValue := 128

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	bucket, err := NewBucketCreator().NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithMemtableThreshold(10000))
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		for i := range keys {
			uuid, err := uuid.New().MarshalBinary()
			require.Nil(t, err)
			keys[i] = uuid

			values[i] = make([]byte, sizePerValue)
			crand.Read(values[i])
		}
	})

	t.Run("import", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := range keys {
			time.Sleep(50 * time.Microsecond)
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := bucket.Put(keys[index], values[index])
				assert.Nil(t, err)
			}(i)
		}
		wg.Wait()
	})

	t.Run("verify get", func(t *testing.T) {
		correct := 0
		var missingKeys []int

		for i := range keys {
			value, err := bucket.Get(keys[i])
			assert.Nil(t, err)
			if bytes.Equal(values[i], value) {
				correct++
			} else {
				missingKeys = append(missingKeys, i)
			}
		}

		if len(missingKeys) > 0 {
			fmt.Printf("missing keys: %v\n", missingKeys)
		}
		assert.Equal(t, amount, correct)
	})

	t.Run("verify cursor", func(t *testing.T) {
		correct := 0
		// put all key value/pairs in a map so we can access them by key
		targets := map[string][]byte{}

		for i := range keys {
			targets[string(keys[i])] = values[i]
		}

		c := bucket.Cursor()
		defer c.Close()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			control := targets[string(k)]
			if bytes.Equal(control, v) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

// This test continuously writes into a bucket with a small memtable threshold,
// so that a lot of flushing is happening while writing. This is to ensure that
// there will be no lost writes or other inconsistencies under load
func TestConcurrentWriting_Set(t *testing.T) {
	dirName := t.TempDir()

	amount := 2000
	valuesPerKey := 4
	sizePerValue := 32

	keys := make([][]byte, amount)
	values := make([][][]byte, amount)

	flushGroup := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(5*time.Millisecond),
		flushGroup.CycleCallback,
		nullLogger()).Start()
	bucket, err := NewBucketCreator().NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), flushGroup,
		WithStrategy(StrategySetCollection),
		WithMemtableThreshold(10000))
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		for i := range keys {
			uuid, err := uuid.New().MarshalBinary()
			require.Nil(t, err)
			keys[i] = uuid

			values[i] = make([][]byte, valuesPerKey)
			for j := range values[i] {
				values[i][j] = make([]byte, sizePerValue)
				crand.Read(values[i][j])
			}
		}
	})

	t.Run("import", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := range keys {
			time.Sleep(50 * time.Microsecond)
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := bucket.SetAdd(keys[index], values[index])
				assert.Nil(t, err)
			}(i)
		}
		wg.Wait()
	})

	t.Run("verify get", func(t *testing.T) {
		correct := 0

		for i := range keys {
			value, err := bucket.SetList(keys[i])
			assert.Nil(t, err)
			if reflect.DeepEqual(values[i], value) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	t.Run("verify cursor", func(t *testing.T) {
		correct := 0
		// put all key value/pairs in a map so we can access them by key
		targets := map[string][][]byte{}

		for i := range keys {
			targets[string(keys[i])] = values[i]
		}

		c := bucket.SetCursor()
		defer c.Close()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			control := targets[string(k)]
			if reflect.DeepEqual(control, v) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

// This test continuously writes into a bucket with a small memtable threshold,
// so that a lot of flushing is happening while writing. This is to ensure that
// there will be no lost writes or other inconsistencies under load
func TestConcurrentWriting_RoaringSet(t *testing.T) {
	dirName := t.TempDir()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	amount := 2000
	valuesPerKey := 4

	keys := make([][]byte, amount)
	values := make([][]uint64, amount)

	flushGroup := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(5*time.Millisecond),
		flushGroup.CycleCallback,
		logger).Start()
	bucket, err := NewBucketCreator().NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), flushGroup,
		WithStrategy(StrategyRoaringSet),
		WithMemtableThreshold(1000))
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		value := uint64(0)
		for i := range keys {
			uuid, err := uuid.New().MarshalBinary()
			require.Nil(t, err)
			keys[i] = uuid

			values[i] = make([]uint64, valuesPerKey)
			for j := range values[i] {
				values[i][j] = value
				value += uint64(r.Intn(10) + 1)
			}
		}
	})

	t.Run("import", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := range keys {
			time.Sleep(50 * time.Microsecond)
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := bucket.RoaringSetAddList(keys[index], values[index])
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()
	})

	t.Run("verify get", func(t *testing.T) {
		for i := range keys {
			value, err := bucket.RoaringSetGet(keys[i])
			require.NoError(t, err)
			assert.ElementsMatch(t, values[i], value.ToArray())
		}
	})

	t.Run("verify cursor", func(t *testing.T) {
		// put all key value/pairs in a map so we can access them by key
		targets := map[string][]uint64{}

		for i := range keys {
			targets[string(keys[i])] = values[i]
		}

		c := bucket.CursorRoaringSet()
		defer c.Close()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			control := targets[string(k)]
			assert.ElementsMatch(t, control, v.ToArray())
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

// This test continuously writes into a bucket with a small memtable threshold,
// so that a lot of flushing is happening while writing. This is to ensure that
// there will be no lost writes or other inconsistencies under load
func TestConcurrentWriting_RoaringSetRange(t *testing.T) {
	dirName := t.TempDir()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	amount := 2000
	valuesPerKey := 4

	keys := make([]uint64, amount)
	values := make([][]uint64, amount)

	flushGroup := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	cyclemanager.NewManager(
		cyclemanager.NewFixedTicker(5*time.Millisecond),
		flushGroup.CycleCallback,
		nullLogger()).Start()
	bucket, err := NewBucketCreator().NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), flushGroup,
		WithStrategy(StrategyRoaringSetRange),
		WithMemtableThreshold(1000),
		WithUseBloomFilter(false))
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		uniques := map[uint64]struct{}{}
		uniqueKey := func() uint64 {
			val := r.Uint64()
			for _, ok := uniques[val]; ok; _, ok = uniques[val] {
				val = r.Uint64()
			}
			uniques[val] = struct{}{}
			return val
		}

		value := uint64(0)
		for i := range keys {
			keys[i] = uniqueKey()
			values[i] = make([]uint64, valuesPerKey)
			for j := range values[i] {
				values[i][j] = value
				value += uint64(r.Intn(10) + 1)
			}
		}
	})

	t.Run("import", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := range keys {
			time.Sleep(50 * time.Microsecond)
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := bucket.RoaringSetRangeAdd(keys[index], values[index]...)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()
	})

	t.Run("verify reader", func(t *testing.T) {
		reader := NewBucketReaderRoaringSetRange(bucket.CursorRoaringSetRange, logger)

		for i := range keys {
			// verify every 5th key to save time
			if i%5 == 0 {
				value, err := reader.Read(testCtx(), keys[i], filters.OperatorEqual)
				require.NoError(t, err)
				assert.ElementsMatch(t, values[i], value.ToArray())
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

// This test continuously writes into a bucket with a small memtable threshold,
// so that a lot of flushing is happening while writing. This is to ensure that
// there will be no lost writes or other inconsistencies under load
func TestConcurrentWriting_Map(t *testing.T) {
	dirName := t.TempDir()

	amount := 2000
	valuesPerKey := 4
	sizePerKey := 8
	sizePerValue := 32

	keys := make([][]byte, amount)
	values := make([][]MapPair, amount)

	bucket, err := NewBucketCreator().NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyMapCollection),
		WithMemtableThreshold(5000))
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		for i := range keys {
			uuid, err := uuid.New().MarshalBinary()
			require.Nil(t, err)
			keys[i] = uuid

			values[i] = make([]MapPair, valuesPerKey)
			for j := range values[i] {
				values[i][j] = MapPair{
					Key:   make([]byte, sizePerKey),
					Value: make([]byte, sizePerValue),
				}
				crand.Read(values[i][j].Key)
				crand.Read(values[i][j].Value)
			}
		}
	})

	t.Run("import", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := range keys {
			for j := 0; j < valuesPerKey; j++ {
				time.Sleep(50 * time.Microsecond)
				wg.Add(1)
				go func(rowIndex, valueIndex int) {
					defer wg.Done()
					err := bucket.MapSet(keys[rowIndex], values[rowIndex][valueIndex])
					assert.Nil(t, err)
				}(i, j)
			}
		}
		wg.Wait()
	})

	t.Run("verify cursor", func(t *testing.T) {
		correct := 0
		// put all key value/pairs in a map so we can access them by key
		targets := map[string][]MapPair{}

		for i := range keys {
			targets[string(keys[i])] = values[i]
		}

		c := bucket.MapCursor()
		defer c.Close()

		ctx := context.Background()

		for k, v := c.First(ctx); k != nil; k, v = c.Next(ctx) {
			control := targets[string(k)]
			if mapElementsMatch(control, v) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	t.Run("verify get", func(t *testing.T) {
		correct := 0

		for i := range keys {
			value, err := bucket.MapList(context.Background(), keys[i])
			assert.Nil(t, err)
			if mapElementsMatch(values[i], value) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

func mapElementsMatch(a, b []MapPair) bool {
	if len(a) != len(b) {
		return false
	}

	aMap := map[string][]byte{}

	for _, kv := range a {
		aMap[string(kv.Key)] = kv.Value
	}

	for _, kv := range b {
		control := aMap[string(kv.Key)]
		if !bytes.Equal(kv.Value, control) {
			return false
		}
	}

	return true
}
