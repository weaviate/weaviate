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
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func compactionReplaceStrategy(ctx context.Context, t *testing.T, opts []BucketOption,
	expectedMinSize, expectedMaxSize int64,
) {
	size := 200

	type kv struct {
		key    []byte
		value  []byte
		delete bool
	}

	var segment1 []kv
	var segment2 []kv
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

				// delete in the second segment
				segment2 = append(segment2, kv{
					key:    key,
					delete: true,
				})

				// do not add to expected at all

			case 3:
				// do not add to segment 1

				// only add to segment 2 (first entry)
				segment2 = append(segment2, kv{
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
		b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
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

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			keyCopy := copyByteSlice(k)
			valueCopy := copyByteSlice(v)
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
			keyCopy := copyByteSlice(k)
			valueCopy := copyByteSlice(v)
			retrieved = append(retrieved, kv{
				key:   keyCopy,
				value: valueCopy,
			})
		}

		assert.Equal(t, expected, retrieved)
		assertSingleSegmentOfSize(t, bucket, expectedMinSize, expectedMaxSize)
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

func compactionReplaceStrategy_WithSecondaryKeys(ctx context.Context, t *testing.T, opts []BucketOption) {
	size := 4

	type kv struct {
		key           []byte
		value         []byte
		secondaryKeys [][]byte
		delete        bool
	}

	var segment1 []kv
	var segment2 []kv
	var expected []kv
	var expectedNotPresent []kv
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
			key := []byte(fmt.Sprintf("key-%02d", i))
			secondaryKey := []byte(fmt.Sprintf("secondary-key-%02d", i))
			originalValue := []byte(fmt.Sprintf("value-%2d-original", i))

			switch i % 4 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:           key,
					secondaryKeys: [][]byte{secondaryKey},
					value:         originalValue,
				})

				// leave this element untouched in the second segment
				expected = append(expected, kv{
					key:   secondaryKey,
					value: originalValue,
				})
			case 1:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:           key,
					secondaryKeys: [][]byte{secondaryKey},
					value:         originalValue,
				})

				// update in the second segment
				updatedValue := []byte(fmt.Sprintf("value-%2d-updated", i))
				segment2 = append(segment2, kv{
					key:           key,
					secondaryKeys: [][]byte{secondaryKey},
					value:         updatedValue,
				})

				expected = append(expected, kv{
					key:   secondaryKey,
					value: updatedValue,
				})
			case 2:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:           key,
					secondaryKeys: [][]byte{secondaryKey},
					value:         originalValue,
				})

				// delete in the second segment
				segment2 = append(segment2, kv{
					key:           key,
					secondaryKeys: [][]byte{secondaryKey},
					delete:        true,
				})

				expectedNotPresent = append(expectedNotPresent, kv{
					key: secondaryKey,
				})

			case 3:
				// do not add to segment 1

				// only add to segment 2 (first entry)
				segment2 = append(segment2, kv{
					key:           key,
					secondaryKeys: [][]byte{secondaryKey},
					value:         originalValue,
				})

				expected = append(expected, kv{
					key:   secondaryKey,
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
		b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, pair := range segment1 {
			if !pair.delete {
				err := bucket.Put(pair.key, pair.value,
					WithSecondaryKey(0, pair.secondaryKeys[0]))
				require.Nil(t, err)
			} else {
				err := bucket.Delete(pair.key,
					WithSecondaryKey(0, pair.secondaryKeys[0]))
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
				err := bucket.Put(pair.key, pair.value,
					WithSecondaryKey(0, pair.secondaryKeys[0]))
				require.Nil(t, err)
			} else {
				err := bucket.Delete(pair.key,
					WithSecondaryKey(0, pair.secondaryKeys[0]))
				require.Nil(t, err)

			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		t.Run("verify the ones that should exist", func(t *testing.T) {
			for _, pair := range expected {
				res, err := bucket.GetBySecondary(0, pair.key)
				require.Nil(t, err)

				assert.Equal(t, pair.value, res)
			}
		})

		t.Run("verify the ones that should NOT exist", func(t *testing.T) {
			for _, pair := range expectedNotPresent {
				res, err := bucket.GetBySecondary(0, pair.key)
				require.Nil(t, err)
				assert.Nil(t, res)
			}
		})
	})

	t.Run("compact until no longer eligible", func(t *testing.T) {
		var compacted bool
		var err error
		for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction", func(t *testing.T) {
		t.Run("verify the ones that should exist", func(t *testing.T) {
			for _, pair := range expected {
				res, err := bucket.GetBySecondary(0, pair.key)
				require.Nil(t, err)

				assert.Equal(t, pair.value, res)
			}
		})

		t.Run("verify the ones that should NOT exist", func(t *testing.T) {
			for _, pair := range expectedNotPresent {
				res, err := bucket.GetBySecondary(0, pair.key)
				require.Nil(t, err)
				assert.Nil(t, res)
			}
		})
	})
}

func compactionReplaceStrategy_RemoveUnnecessaryDeletes(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key   []byte
		value []byte
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("write segments", func(t *testing.T) {
		for i := 0; i < size; i++ {
			if i != 0 {
				// we can only update an existing value if this isn't the first write
				err := bucket.Delete(key)
				require.Nil(t, err)
			}

			err := bucket.Put(key, []byte(fmt.Sprintf("set in round %d", i)))
			require.Nil(t, err)

			require.Nil(t, bucket.FlushAndSwitch())
		}
	})

	expected := []kv{
		{
			key:   key,
			value: []byte(fmt.Sprintf("set in round %d", size-1)),
		},
	}

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:   k,
				value: v,
			})
		}

		assert.Equal(t, expected, retrieved)
	})

	t.Run("compact until no longer eligible", func(t *testing.T) {
		var compacted bool
		var err error
		for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
		}
		require.Nil(t, err)
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:   k,
				value: v,
			})
		}

		assert.Equal(t, expected, retrieved)
	})
}

func compactionReplaceStrategy_RemoveUnnecessaryUpdates(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key   []byte
		value []byte
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("write segments", func(t *testing.T) {
		for i := 0; i < size; i++ {
			err := bucket.Put(key, []byte(fmt.Sprintf("set in round %d", i)))
			require.Nil(t, err)

			require.Nil(t, bucket.FlushAndSwitch())
		}
	})

	expected := []kv{
		{
			key:   key,
			value: []byte(fmt.Sprintf("set in round %d", size-1)),
		},
	}

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.Cursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:   k,
				value: v,
			})
		}

		assert.Equal(t, expected, retrieved)
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
			retrieved = append(retrieved, kv{
				key:   k,
				value: v,
			})
		}

		assert.Equal(t, expected, retrieved)
	})
}

func compactionReplaceStrategy_FrequentPutDeleteOperations(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction doesn't make the object to disappear
	// We are creating even number of segments in which first we create an object
	// then we in the next segment with delete it and we do this operation in loop
	// we make sure that the last operation done in the last segment is create object operation
	// In this situation after the compaction the object has to exist
	size := 100

	key := []byte("my-key")

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("write segments, leave the last segment with value", func(t *testing.T) {
		for i := 0; i < size; i++ {
			err := bucket.Put(key, []byte(fmt.Sprintf("set in round %d", i)))
			require.Nil(t, err)

			if i != size-1 {
				// don't delete from the last segment
				err := bucket.Delete(key)
				require.Nil(t, err)
			}

			require.Nil(t, bucket.FlushAndSwitch())
		}
	})

	t.Run("verify that the object exists before compaction", func(t *testing.T) {
		res, err := bucket.Get(key)
		assert.Nil(t, err)
		assert.NotNil(t, res)
	})

	t.Run("compact until no longer eligible", func(t *testing.T) {
		var compacted bool
		var err error
		for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
		}
		require.Nil(t, err)
	})

	t.Run("verify that the object still exists after compaction", func(t *testing.T) {
		res, err := bucket.Get(key)
		assert.Nil(t, err)
		assert.NotNil(t, res)
	})
}

func compactionReplaceStrategy_FrequentPutDeleteOperations_WithSecondaryKeys(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction doesn't make the object to disappear
	// We are creating even number of segments in which first we create an object
	// then we in the next segment with delete it and we do this operation in loop
	// we make sure that the last operation done in the last segment is create object operation
	// We are doing this for 4 to 10 segments scenarios, without the fix for firstWithAllKeys
	// cursor method that now sets the nextOffset properly, we got discrepancies
	// after compaction on 4 and 8 segments scenario.
	maxSize := 10

	for size := 4; size < maxSize; size++ {
		t.Run(fmt.Sprintf("compact %v segments", size), func(t *testing.T) {
			var bucket *Bucket

			key := []byte("key-original")
			keySecondary := []byte(fmt.Sprintf("secondary-key-%02d", size-1))

			dirName := t.TempDir()

			t.Run("init bucket", func(t *testing.T) {
				b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.Nil(t, err)

				// so big it effectively never triggers as part of this test
				b.SetMemtableThreshold(1e9)

				bucket = b
			})

			t.Run("write segments, leave the last segment with value", func(t *testing.T) {
				for i := 0; i < size; i++ {
					secondaryKey := []byte(fmt.Sprintf("secondary-key-%02d", i))
					originalValue := []byte(fmt.Sprintf("value-%2d-original", i))

					err := bucket.Put(key, originalValue, WithSecondaryKey(0, secondaryKey))
					require.Nil(t, err)

					if i != size-1 {
						// don't delete from the last segment
						err := bucket.Delete(key, WithSecondaryKey(0, secondaryKey))
						require.Nil(t, err)
					}

					require.Nil(t, bucket.FlushAndSwitch())
				}
			})

			t.Run("verify that the object exists before compaction", func(t *testing.T) {
				res, err := bucket.GetBySecondary(0, keySecondary)
				assert.Nil(t, err)
				assert.NotNil(t, res)
				res, err = bucket.Get(key)
				assert.Nil(t, err)
				assert.NotNil(t, res)
			})

			t.Run("compact until no longer eligible", func(t *testing.T) {
				var compacted bool
				var err error
				for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
				}
				require.Nil(t, err)
			})

			t.Run("verify that the object still exists after compaction", func(t *testing.T) {
				res, err := bucket.GetBySecondary(0, keySecondary)
				assert.Nil(t, err)
				assert.NotNil(t, res)
				res, err = bucket.Get(key)
				assert.Nil(t, err)
				assert.NotNil(t, res)
			})
		})
	}
}
