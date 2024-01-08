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
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func compactionMapStrategy(ctx context.Context, t *testing.T, opts []BucketOption,
	expectedMinSize, expectedMaxSize int64,
) {
	size := 100

	type kv struct {
		key    []byte
		values []MapPair
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
		// 2.) created in the first segment, first element updated in the second
		// 3.) created in the first segment, second element updated in the second
		// 4.) created in the first segment, first element deleted in the second
		// 5.) created in the first segment, second element deleted in the second
		// 6.) not present in the first segment, created in the second
		// 7.) present in an unrelated previous segment, deleted in the first
		// 8.) present in an unrelated previous segment, deleted in the second
		// 9.) present in an unrelated previous segment, never touched again
		for i := 0; i < size; i++ {
			rowKey := []byte(fmt.Sprintf("row-%03d", i))

			pair1 := MapPair{
				Key:   []byte(fmt.Sprintf("value-%03d-01", i)),
				Value: []byte(fmt.Sprintf("value-%03d-01-original", i)),
			}
			pair2 := MapPair{
				Key:   []byte(fmt.Sprintf("value-%03d-02", i)),
				Value: []byte(fmt.Sprintf("value-%03d-02-original", i)),
			}
			pairs := []MapPair{pair1, pair2}

			switch i % 10 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs[:1],
				})

				// leave this element untouched in the second segment
				expected = append(expected, kv{
					key:    rowKey,
					values: pairs[:1],
				})
			case 1:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs[:1],
				})

				// add extra pair in the second segment
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: pairs[1:2],
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})
			case 2:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// update first key in the second segment
				updated := pair1
				updated.Value = []byte("updated")

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair2, updated},
				})

			case 3:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// update first key in the second segment
				updated := pair2
				updated.Value = []byte("updated")

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair1, updated},
				})

			case 4:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// delete first key in the second segment
				updated := pair1
				updated.Value = nil
				updated.Tombstone = true

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair2},
				})

			case 5:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// delete second key in the second segment
				updated := pair2
				updated.Value = nil
				updated.Tombstone = true

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair1},
				})

			case 6:
				// do not add to segment 2

				// only add to segment 2 (first entry)
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: pairs,
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})

			case 7:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				// delete in segment 1
				deleted1 := pair1
				deleted1.Value = nil
				deleted1.Tombstone = true

				deleted2 := pair2
				deleted2.Value = nil
				deleted2.Tombstone = true

				segment1 = append(segment1, kv{
					key:    rowKey,
					values: []MapPair{deleted1},
				})
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: []MapPair{deleted2},
				})

				// should not have any values in expected at all, not even a key

			case 8:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				// delete in segment 1
				deleted1 := pair1
				deleted1.Value = nil
				deleted1.Tombstone = true

				deleted2 := pair2
				deleted2.Value = nil
				deleted2.Tombstone = true

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{deleted1},
				})
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{deleted2},
				})

				// should not have any values in expected at all, not even a key

			case 9:
				// only part of a previous segment
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
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
		for _, kvs := range previous1 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}

		require.Nil(t, bucket.FlushAndSwitch())

		for _, kvs := range previous2 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}

		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, kvs := range segment1 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 2", func(t *testing.T) {
		for _, kvs := range segment2 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("within control make sure map keys are sorted", func(t *testing.T) {
		for i := range expected {
			sort.Slice(expected[i].values, func(a, b int) bool {
				return bytes.Compare(expected[i].values[a].Key, expected[i].values[b].Key) < 0
			})
		}
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
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
				assertSecondSegmentOfSize(t, bucket, 11876, 11876)
			}
			i++
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction using a cursor", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
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

	t.Run("verify control using individual get (MapList) operations",
		func(t *testing.T) {
			// Previously the only verification was done using the cursor. That
			// guaranteed that all pairs are present in the payload, but it did not
			// guarantee the integrity of the index (DiskTree) which is used to access
			// _individual_ keys. Corrupting this index is exactly what happened in
			// https://github.com/weaviate/weaviate/issues/3517
			for _, pair := range expected {
				retrieved, err := bucket.MapList(pair.key)
				require.NoError(t, err)

				assert.Equal(t, pair.values, retrieved)
			}
		})
}

func compactionMapStrategy_RemoveUnnecessary(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key    []byte
		values []MapPair
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
				pair := MapPair{
					Key:   []byte(fmt.Sprintf("value-%05d", i-1)),
					Value: []byte(fmt.Sprintf("updated in round %d", i)),
				}
				err := bucket.MapSet(key, pair)
				require.Nil(t, err)
			}

			if i > 1 {
				// we can only delete two back an existing value if this isn't the
				// first or second write
				pair := MapPair{
					Key:       []byte(fmt.Sprintf("value-%05d", i-2)),
					Tombstone: true,
				}
				err := bucket.MapSet(key, pair)
				require.Nil(t, err)
			}

			pair := MapPair{
				Key:   []byte(fmt.Sprintf("value-%05d", i)),
				Value: []byte("original value"),
			}
			err := bucket.MapSet(key, pair)
			require.Nil(t, err)

			require.Nil(t, bucket.FlushAndSwitch())
		}
	})

	expected := []kv{
		{
			key: key,
			values: []MapPair{
				{
					Key:   []byte(fmt.Sprintf("value-%05d", size-2)),
					Value: []byte(fmt.Sprintf("updated in round %d", size-1)),
				},
				{
					Key:   []byte(fmt.Sprintf("value-%05d", size-1)),
					Value: []byte("original value"),
				},
			},
		},
	}

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
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

		c := bucket.MapCursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:    k,
				values: v,
			})
		}

		assert.Equal(t, expected, retrieved)
	})

	t.Run("verify control using individual get (MapList) operations",
		func(t *testing.T) {
			// Previously the only verification was done using the cursor. That
			// guaranteed that all pairs are present in the payload, but it did not
			// guarantee the integrity of the index (DiskTree) which is used to access
			// _individual_ keys. Corrupting this index is exactly what happened in
			// https://github.com/weaviate/weaviate/issues/3517
			for _, pair := range expected {
				retrieved, err := bucket.MapList(pair.key)
				require.NoError(t, err)

				assert.Equal(t, pair.values, retrieved)
			}
		})
}

func compactionMapStrategy_FrequentPutDeleteOperations(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction works well for map collection
	maxSize := 10

	key := []byte("my-key")
	mapKey := []byte("value-1")

	for size := 4; size < maxSize; size++ {
		t.Run(fmt.Sprintf("compact %v segments", size), func(t *testing.T) {
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
					value := []byte(fmt.Sprintf("updated in round %d", i))
					pair := MapPair{Key: mapKey, Value: value}

					err := bucket.MapSet(key, pair)
					require.Nil(t, err)

					if size == 5 || size == 6 {
						// delete all
						err = bucket.MapDeleteKey(key, mapKey)
						require.Nil(t, err)
					} else if i != size-1 {
						// don't delete at the end
						err := bucket.MapDeleteKey(key, mapKey)
						require.Nil(t, err)
					}

					require.Nil(t, bucket.FlushAndSwitch())
				}
			})

			t.Run("check entries before compaction", func(t *testing.T) {
				res, err := bucket.MapList(key)
				assert.Nil(t, err)
				if size == 5 || size == 6 {
					assert.Empty(t, res)
				} else {
					assert.Len(t, res, 1)
					assert.Equal(t, false, res[0].Tombstone)
				}
			})

			t.Run("compact until no longer eligible", func(t *testing.T) {
				var compacted bool
				var err error
				for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
				}
				require.Nil(t, err)
			})

			t.Run("check entries after compaction", func(t *testing.T) {
				res, err := bucket.MapList(key)
				assert.Nil(t, err)
				if size == 5 || size == 6 {
					assert.Empty(t, res)
				} else {
					assert.Len(t, res, 1)
					assert.Equal(t, false, res[0].Tombstone)
				}
			})
		})
	}
}
