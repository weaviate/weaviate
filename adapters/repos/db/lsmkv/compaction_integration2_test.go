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

package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCompactionReplaceStrategyStraggler(t *testing.T) {
	opts := []BucketOption{WithStrategy(StrategyReplace)}
	size := 200

	type kv struct {
		key    []byte
		value  []byte
		delete bool
	}

	var segment1 []kv
	var segment2 []kv
	var segment3 []kv
	var expected []kv
	var bucket *Bucket

	dirName := t.TempDir()

	t.Run("create test data", func(t *testing.T) {
		// The test data is split into 4 scenarios evenly:
		//
		// 1.) created in the first segment, never touched again
		// 2.) created in the first segment, updated in the second
		// 3.) created in the first segment, deleted in the second
		// 4.) not present in the first segment, created in the second
		for i := 0; i < size; i++ {
			key := []byte(fmt.Sprintf("key-%3d", i))
			originalValue := []byte(fmt.Sprintf("value-%3d-original", i))

			switch i % 4 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:   key,
					value: originalValue,
				})

				// leave this element untouched in the second segment
				expected = append(expected, kv{
					key:   key,
					value: originalValue,
				})
			case 1:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:   key,
					value: originalValue,
				})

				// update in the second segment
				updatedValue := []byte(fmt.Sprintf("value-%3d-updated", i))
				segment2 = append(segment2, kv{
					key:   key,
					value: updatedValue,
				})
				// update in the third segment
				updatedValue = []byte(fmt.Sprintf("value-%3d-updated-twice", i))
				segment3 = append(segment3, kv{
					key:   key,
					value: updatedValue,
				})

				expected = append(expected, kv{
					key:   key,
					value: updatedValue,
				})
			case 2:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:   key,
					value: originalValue,
				})

				// delete in the third segment
				segment3 = append(segment3, kv{
					key:    key,
					delete: true,
				})

				// do not add to expected at all

			case 3:
				// do not add to segment 1

				// only add to segment 3 (first entry)
				segment3 = append(segment3, kv{
					key:   key,
					value: originalValue,
				})

				expected = append(expected, kv{
					key:   key,
					value: originalValue,
				})
			}
		}
	})

	t.Run("shuffle the import order for each segment", func(t *testing.T) {
		// this is to make sure we don't accidentally rely on the import order
		rand.Shuffle(len(segment1), func(i, j int) {
			segment1[i], segment1[j] = segment1[j], segment1[i]
		})
		rand.Shuffle(len(segment2), func(i, j int) {
			segment2[i], segment2[j] = segment2[j], segment2[i]
		})
	})

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(context.TODO(), dirName, "", nullLogger2(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, pair := range segment1 {
			if !pair.delete {
				err := bucket.Put(pair.key, pair.value)
				require.Nil(t, err)
			} else {
				err := bucket.Delete(pair.key)
				require.Nil(t, err)

			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 2", func(t *testing.T) {
		for _, pair := range segment2 {
			if !pair.delete {
				err := bucket.Put(pair.key, pair.value)
				require.Nil(t, err)
			} else {
				err := bucket.Delete(pair.key)
				require.Nil(t, err)

			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 3", func(t *testing.T) {
		for _, pair := range segment3 {
			if !pair.delete {
				err := bucket.Put(pair.key, pair.value)
				require.Nil(t, err)
			} else {
				err := bucket.Delete(pair.key)
				require.Nil(t, err)

			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			keyCopy := copyByteSlice2(k)
			valueCopy := copyByteSlice2(v)
			retrieved = append(retrieved, kv{
				key:   keyCopy,
				value: valueCopy,
			})
		}

		assert.Equal(t, expected, retrieved)
	})

	t.Run("verify count control before compaction", func(*testing.T) {
		assert.Equal(t, len(expected), bucket.Count())
	})

	t.Run("compact until no longer eligible", func(t *testing.T) {
		var compacted bool
		var err error
		for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			keyCopy := copyByteSlice2(k)
			valueCopy := copyByteSlice2(v)
			retrieved = append(retrieved, kv{
				key:   keyCopy,
				value: valueCopy,
			})
		}

		assert.Equal(t, expected, retrieved)
	})

	t.Run("verify control using individual get operations",
		func(t *testing.T) {
			for _, pair := range expected {
				retrieved, err := bucket.Get(pair.key)
				require.NoError(t, err)

				assert.Equal(t, pair.value, retrieved)
			}
		})

	t.Run("verify count after compaction", func(*testing.T) {
		assert.Equal(t, len(expected), bucket.Count())
	})
}

func nullLogger2() logrus.FieldLogger {
	log, _ := test.NewNullLogger()
	return log
}

func copyByteSlice2(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
