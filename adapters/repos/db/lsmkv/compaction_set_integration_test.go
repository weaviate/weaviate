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

func compactionSetStrategy(ctx context.Context, t *testing.T, opts []BucketOption,
	expectedMinSize, expectedMaxSize int64,
) {
	size := 100

	type kv struct {
		key    []byte
		values [][]byte
		delete bool
	}
	// this segment is not part of the merge, but might still play a role in
	// overall results. For example if one of the later segments has a tombstone
	// for it
	var previous1 []kv
	var previous2 []kv

	var segment1 []kv
	var segment2 []kv
	var expected []kv
	var bucket *Bucket

	dirName := t.TempDir()

	t.Run("create test data", func(t *testing.T) {
		// The test data is split into 4 scenarios evenly:
		//
		// 0.) created in the first segment, never touched again
		// 1.) created in the first segment, appended to it in the second
		// 2.) created in the first segment, first element deleted in the second
		// 3.) created in the first segment, second element deleted in the second
		// 4.) not present in the first segment, created in the second
		// 5.) present in an unrelated previous segment, deleted in the first
		// 6.) present in an unrelated previous segment, deleted in the second
		// 7.) present in an unrelated previous segment, never touched again
		for i := 0; i < size; i++ {
			key := []byte(fmt.Sprintf("key-%02d", i))

			value1 := []byte(fmt.Sprintf("value-%02d-01", i))
			value2 := []byte(fmt.Sprintf("value-%02d-02", i))
			values := [][]byte{value1, value2}

			switch i % 8 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    key,
					values: values[:1],
				})

				// leave this element untouched in the second segment
				expected = append(expected, kv{
					key:    key,
					values: values[:1],
				})

			case 1:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    key,
					values: values[:1],
				})

				// update in the second segment
				segment2 = append(segment2, kv{
					key:    key,
					values: values[1:2],
				})

				expected = append(expected, kv{
					key:    key,
					values: values,
				})

			case 2:
				// add both to segment 1, delete the first
				segment1 = append(segment1, kv{
					key:    key,
					values: values,
				})

				// delete first element in the second segment
				segment2 = append(segment2, kv{
					key:    key,
					values: values[:1],
					delete: true,
				})

				// only the 2nd element should be left in the expected
				expected = append(expected, kv{
					key:    key,
					values: values[1:2],
				})

			case 3:
				// add both to segment 1, delete the second
				segment1 = append(segment1, kv{
					key:    key,
					values: values,
				})

				// delete second element in the second segment
				segment2 = append(segment2, kv{
					key:    key,
					values: values[1:],
					delete: true,
				})

				// only the 1st element should be left in the expected
				expected = append(expected, kv{
					key:    key,
					values: values[:1],
				})

			case 4:
				// do not add to segment 1

				// only add to segment 2 (first entry)
				segment2 = append(segment2, kv{
					key:    key,
					values: values,
				})

				expected = append(expected, kv{
					key:    key,
					values: values,
				})

			case 5:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    key,
					values: values[:1],
				})
				previous2 = append(previous2, kv{
					key:    key,
					values: values[1:],
				})

				// delete in segment 1
				segment1 = append(segment1, kv{
					key:    key,
					values: values[:1],
					delete: true,
				})
				segment1 = append(segment1, kv{
					key:    key,
					values: values[1:],
					delete: true,
				})

				// should not have any values in expected at all, not even a key

			case 6:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    key,
					values: values[:1],
				})
				previous2 = append(previous2, kv{
					key:    key,
					values: values[1:],
				})

				// delete in segment 2
				segment2 = append(segment2, kv{
					key:    key,
					values: values[:1],
					delete: true,
				})
				segment2 = append(segment2, kv{
					key:    key,
					values: values[1:],
					delete: true,
				})

				// should not have any values in expected at all, not even a key

			case 7:
				// part of a previous segment
				previous1 = append(previous1, kv{
					key:    key,
					values: values[:1],
				})
				previous2 = append(previous2, kv{
					key:    key,
					values: values[1:],
				})

				expected = append(expected, kv{
					key:    key,
					values: values,
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

	t.Run("import and flush previous segments", func(t *testing.T) {
		for _, pair := range previous1 {
			err := bucket.SetAdd(pair.key, pair.values)
			require.Nil(t, err)
		}

		require.Nil(t, bucket.FlushAndSwitch())

		for _, pair := range previous2 {
			err := bucket.SetAdd(pair.key, pair.values)
			require.Nil(t, err)
		}

		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, pair := range segment1 {
			if !pair.delete {
				err := bucket.SetAdd(pair.key, pair.values)
				require.Nil(t, err)
			} else {
				err := bucket.SetDeleteSingle(pair.key, pair.values[0])
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
				err := bucket.SetAdd(pair.key, pair.values)
				require.Nil(t, err)
			} else {
				err := bucket.SetDeleteSingle(pair.key, pair.values[0])
				require.Nil(t, err)
			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.SetCursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:    k,
				values: v,
			})
		}

		assert.Equal(t, expected, retrieved)
	})

	t.Run("compact until no longer eligible", func(t *testing.T) {
		i := 0
		var compacted bool
		var err error
		for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
			if i == 1 {
				// segment1 and segment2 merged
				// none of them is root segment, so tombstones
				// will not be removed regardless of keepTombstones setting
				assertSecondSegmentOfSize(t, bucket, 8556, 8556)
			}
			i++
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.SetCursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:    k,
				values: v,
			})
		}

		assert.Equal(t, expected, retrieved)
		assertSingleSegmentOfSize(t, bucket, expectedMinSize, expectedMaxSize)
	})
}

func compactionSetStrategy_RemoveUnnecessary(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key    []byte
		values [][]byte
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
				// we can only delete an existing value if this isn't the first write
				value := []byte(fmt.Sprintf("value-%05d", i-1))
				err := bucket.SetDeleteSingle(key, value)
				require.Nil(t, err)
			}

			value := []byte(fmt.Sprintf("value-%05d", i))
			err := bucket.SetAdd(key, [][]byte{value})
			require.Nil(t, err)

			require.Nil(t, bucket.FlushAndSwitch())
		}
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv
		expected := []kv{
			{
				key:    key,
				values: [][]byte{[]byte(fmt.Sprintf("value-%05d", size-1))},
			},
		}

		c := bucket.SetCursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:    k,
				values: v,
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
		expected := []kv{
			{
				key:    key,
				values: [][]byte{[]byte(fmt.Sprintf("value-%05d", size-1))},
			},
		}

		c := bucket.SetCursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:    k,
				values: v,
			})
		}

		assert.Equal(t, expected, retrieved)
	})
}

func compactionSetStrategy_FrequentPutDeleteOperations(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction works well for set collection
	maxSize := 10

	for size := 4; size < maxSize; size++ {
		t.Run(fmt.Sprintf("compact %v segments", size), func(t *testing.T) {
			var bucket *Bucket

			key := []byte("key-original")
			value1 := []byte("value-01")
			value2 := []byte("value-02")
			values := [][]byte{value1, value2}

			dirName := t.TempDir()

			t.Run("init bucket", func(t *testing.T) {
				b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.Nil(t, err)

				// so big it effectively never triggers as part of this test
				b.SetMemtableThreshold(1e9)

				bucket = b
			})

			t.Run("import and flush segments", func(t *testing.T) {
				for i := 0; i < size; i++ {
					err := bucket.SetAdd(key, values)
					require.Nil(t, err)

					if size == 5 {
						// delete all
						err := bucket.SetDeleteSingle(key, values[0])
						require.Nil(t, err)
						err = bucket.SetDeleteSingle(key, values[1])
						require.Nil(t, err)
					} else if size == 6 {
						// delete only one value
						err := bucket.SetDeleteSingle(key, values[0])
						require.Nil(t, err)
					} else if i != size-1 {
						// don't delete from the last segment
						err := bucket.SetDeleteSingle(key, values[0])
						require.Nil(t, err)
						err = bucket.SetDeleteSingle(key, values[1])
						require.Nil(t, err)
					}

					require.Nil(t, bucket.FlushAndSwitch())
				}
			})

			t.Run("verify that objects exist before compaction", func(t *testing.T) {
				res, err := bucket.SetList(key)
				assert.Nil(t, err)
				if size == 5 {
					assert.Len(t, res, 0)
				} else if size == 6 {
					assert.Len(t, res, 1)
				} else {
					assert.Len(t, res, 2)
				}
			})

			t.Run("compact until no longer eligible", func(t *testing.T) {
				var compacted bool
				var err error
				for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
				}
				require.Nil(t, err)
			})

			t.Run("verify that objects exist after compaction", func(t *testing.T) {
				res, err := bucket.SetList(key)
				assert.Nil(t, err)
				if size == 5 {
					assert.Len(t, res, 0)
				} else if size == 6 {
					assert.Len(t, res, 1)
				} else {
					assert.Len(t, res, 2)
				}
			})
		})
	}
}
