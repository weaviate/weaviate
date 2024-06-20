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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestCompactionInvertedStrategy(t *testing.T) {
	t.Run("compactionInvertedStrategy", func(t *testing.T) {
		compactionInverted(context.Background(), t, []BucketOption{WithStrategy(StrategyInverted)}, 7076, 7076)
	})

	t.Run("compactionInvertedStrategy_KeepTombstones", func(t *testing.T) {
		compactionInverted(context.Background(), t, []BucketOption{WithKeepTombstones(true), WithStrategy(StrategyInverted)}, 7380, 7380)
	})

	t.Run("compactionInvertedStrategy_RemoveUnnecessary", func(t *testing.T) {
		compactionInvertedStrategy_RemoveUnnecessary(context.Background(), t, []BucketOption{WithStrategy(StrategyInverted)})
	})

	t.Run("compactionInvertedStrategy_FrequentPutDeleteOperations", func(t *testing.T) {
		compactionInvertedStrategy_FrequentPutDeleteOperations(context.Background(), t, []BucketOption{WithStrategy(StrategyInverted)})
	})
}

func compactionInverted(ctx context.Context, t *testing.T, opts []BucketOption,
	expectedMinSize, expectedMaxSize int64,
) {
	size := 100

	addedDocIds := make(map[uint64]struct{})
	removedDocIds := make(map[uint64]struct{})

	type kv struct {
		key    []byte
		values []InvertedPair
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

			pair1 := InvertedPair{}

			pair2 := InvertedPair{}

			docId1 := uint64(i)
			docId2 := uint64(i + 10000)

			pair1.FromDocIdAndTf(docId1, float32(i+1), float32(i+2))
			pair2.FromDocIdAndTf(docId2, float32(i+1), float32(i+2))

			pairs := []InvertedPair{pair1, pair2}

			switch i % 10 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs[:1],
				})

				addedDocIds[docId1] = struct{}{}
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

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

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
				updated.UpdateTf(float32(i*1000), float32(i*1000))

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []InvertedPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []InvertedPair{pair2, updated},
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

			case 3:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// update first key in the second segment
				updated := pair2
				updated.UpdateTf(float32(i*1000), float32(i*1000))

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []InvertedPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []InvertedPair{pair1, updated},
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

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
					values: []InvertedPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []InvertedPair{pair2},
				})

				removedDocIds[docId1] = struct{}{}
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
					values: []InvertedPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []InvertedPair{pair1},
				})

				removedDocIds[docId2] = struct{}{}

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

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

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
					values: []InvertedPair{deleted1},
				})
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: []InvertedPair{deleted2},
				})

				removedDocIds[docId1] = struct{}{}
				removedDocIds[docId2] = struct{}{}

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

				removedDocIds[docId1] = struct{}{}
				removedDocIds[docId2] = struct{}{}

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []InvertedPair{deleted1},
				})
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []InvertedPair{deleted2},
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

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}
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
		logger, _ := test.NewNullLogger()
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("import and flush previous segments", func(t *testing.T) {
		for _, kvs := range previous1 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, MapPair{
					Key:       pair.Key,
					Value:     pair.Value,
					Tombstone: pair.Tombstone,
				})

				require.Nil(t, err)
			}
		}

		require.Nil(t, bucket.FlushAndSwitch())

		for _, kvs := range previous2 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, MapPair{
					Key:       pair.Key,
					Value:     pair.Value,
					Tombstone: pair.Tombstone,
				})
				require.Nil(t, err)
			}
		}

		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, kvs := range segment1 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, MapPair{
					Key:       pair.Key,
					Value:     pair.Value,
					Tombstone: pair.Tombstone,
				})
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
				err := bucket.MapSet(kvs.key, MapPair{
					Key:       pair.Key,
					Value:     pair.Value,
					Tombstone: pair.Tombstone,
				})
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

		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			mkvs := make([]InvertedPair, len(kvs))
			for i := range kvs {
				mkvs[i] = InvertedPair{
					Key:       kvs[i].Key,
					Value:     kvs[i].Value,
					Tombstone: kvs[i].Tombstone,
				}
			}
			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: mkvs,
				})
			}
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
				// assertSecondSegmentOfSize(t, bucket, 11876, 11876)
			}
			i++
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction using a cursor", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()

		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			mkvs := make([]InvertedPair, len(kvs))
			for i := range kvs {
				mkvs[i] = InvertedPair{
					Key:       kvs[i].Key,
					Value:     kvs[i].Value,
					Tombstone: kvs[i].Tombstone,
				}
			}

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: mkvs,
				})
			}
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
				kvs, err := bucket.MapList(ctx, pair.key)
				require.NoError(t, err)

				retrieved := make([]InvertedPair, len(kvs))
				for i := range kvs {
					retrieved[i] = InvertedPair{
						Key:       kvs[i].Key,
						Value:     kvs[i].Value,
						Tombstone: kvs[i].Tombstone,
					}
				}

				assert.Equal(t, pair.values, retrieved)
			}
		})
}

func compactionInvertedStrategy_RemoveUnnecessary(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key    []byte
		values []InvertedPair
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("write segments", func(t *testing.T) {
		for i := 0; i < size; i++ {
			if i != 0 {
				// we can only update an existing value if this isn't the first write
				pair := InvertedPair{}
				pair.FromDocIdAndTf(uint64(i-1), float32(i), float32(i))
				err := bucket.MapSet(key, MapPair{
					Key:       pair.Key,
					Value:     pair.Value,
					Tombstone: pair.Tombstone,
				})
				require.Nil(t, err)
			}

			if i > 1 {
				// we can only delete two back an existing value if this isn't the
				// first or second write
				pair := InvertedPair{}

				pair.FromDocIdAndTf(uint64(i-2), float32(i), float32(i))
				err := bucket.MapSet(key, MapPair{
					Key:       pair.Key,
					Tombstone: true,
				})
				require.Nil(t, err)
			}

			pair := InvertedPair{}

			pair.FromDocIdAndTf(uint64(i), float32(i), float32(i))
			err := bucket.MapSet(key, MapPair{
				Key:   pair.Key,
				Value: pair.Value,
			})
			require.Nil(t, err)
			require.Nil(t, bucket.FlushAndSwitch())
		}
	})

	expectedPair := InvertedPair{}

	expectedPair.FromDocIdAndTf(uint64(size-2), float32(size-1), float32(size-1))

	expectedPair2 := InvertedPair{}

	expectedPair2.FromDocIdAndTf(uint64(size-1), float32(size-1), float32(size-1))

	expected := []kv{
		{
			key: key,
			values: []InvertedPair{
				expectedPair,
				expectedPair2,
			},
		},
	}

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()

		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			vals := make([]InvertedPair, len(kvs))
			for i := range kvs {
				vals[i] = InvertedPair{
					Key:       kvs[i].Key,
					Value:     kvs[i].Value,
					Tombstone: kvs[i].Tombstone,
				}
			}
			retrieved = append(retrieved, kv{
				key:    k,
				values: vals,
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

		c := bucket.MapCursor()
		defer c.Close()

		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {
			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			vals := make([]InvertedPair, len(kvs))
			for i := range kvs {
				vals[i] = InvertedPair{
					Key:       kvs[i].Key,
					Value:     kvs[i].Value,
					Tombstone: kvs[i].Tombstone,
				}
			}
			retrieved = append(retrieved, kv{
				key:    k,
				values: vals,
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
				kvs, err := bucket.MapList(ctx, pair.key)
				require.NoError(t, err)

				mkvs := make([]InvertedPair, len(kvs))
				for i := range kvs {
					mkvs[i] = InvertedPair{
						Key:       kvs[i].Key,
						Value:     kvs[i].Value,
						Tombstone: kvs[i].Tombstone,
					}
				}

				assert.Equal(t, pair.values, mkvs)
			}
		})
}

func compactionInvertedStrategy_FrequentPutDeleteOperations(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction works well for map collection
	maxSize := 10

	key := []byte("my-key")
	mapKey := make([]byte, 8)
	binary.BigEndian.PutUint64(mapKey, 1)

	for size := 4; size < maxSize; size++ {
		t.Run(fmt.Sprintf("compact %v segments", size), func(t *testing.T) {
			var bucket *Bucket
			dirName := t.TempDir()

			t.Run("init bucket", func(t *testing.T) {
				logger, _ := test.NewNullLogger()

				b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.Nil(t, err)

				// so big it effectively never triggers as part of this test
				b.SetMemtableThreshold(1e9)

				bucket = b
			})

			t.Run("write segments", func(t *testing.T) {
				for i := 0; i < size; i++ {
					pair := InvertedPair{}
					pair.FromDocIdAndTf(0, float32(i), float32(i))

					err := bucket.MapSet(key, MapPair{
						Key:       mapKey,
						Value:     pair.Value,
						Tombstone: pair.Tombstone,
					})
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
				res, err := bucket.MapList(ctx, key)
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
				res, err := bucket.MapList(ctx, key)
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

func assertSingleSegmentOfSize(t *testing.T, bucket *Bucket, expectedMinSize, expectedMaxSize int64) {
	files, err := bucket.ListFiles(context.Background(), bucket.dir)
	require.NoError(t, err)

	dbFiles := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Ext(f) == ".db" {
			dbFiles = append(dbFiles, f)
		}
	}
	require.Len(t, dbFiles, 1)

	fi, err := os.Stat(dbFiles[0])
	require.NoError(t, err)
	assert.LessOrEqual(t, expectedMinSize, fi.Size())
	assert.GreaterOrEqual(t, expectedMaxSize, fi.Size())
}

func assertSecondSegmentOfSize(t *testing.T, bucket *Bucket, expectedMinSize, expectedMaxSize int64) {
	files, err := bucket.ListFiles(context.Background(), bucket.dir)
	require.NoError(t, err)

	dbFiles := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Ext(f) == ".db" {
			dbFiles = append(dbFiles, f)
		}
	}
	require.Len(t, dbFiles, 2)

	fi, err := os.Stat(dbFiles[1])
	require.NoError(t, err)
	assert.LessOrEqual(t, expectedMinSize, fi.Size())
	assert.GreaterOrEqual(t, expectedMaxSize, fi.Size())
}
