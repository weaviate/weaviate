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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

func compactionRoaringSetRangeStrategy_Random(ctx context.Context, t *testing.T, opts []BucketOption) {
	maxID := uint64(100)
	maxElement := uint64(1e6)
	iterations := uint64(100_000)

	deleteRatio := 0.2   // 20% of all operations will be deletes, 80% additions
	flushChance := 0.001 // on average one flush per 1000 iterations

	r := getRandomSeed()

	instr := generateRandomRangeInstructions(r, maxID, maxElement, iterations, deleteRatio)
	control := controlFromRangeInstructions(instr, maxID)

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)

	defer b.Shutdown(testCtx())

	// so big it effectively never triggers as part of this test
	b.SetMemtableThreshold(1e9)

	compactions := 0
	for _, inst := range instr {
		if inst.addition {
			b.RoaringSetRangeAdd(inst.key, inst.element)
		} else {
			b.RoaringSetRangeRemove(inst.key, inst.element)
		}

		if r.Float64() < flushChance {
			require.Nil(t, b.FlushAndSwitch())

			var compacted bool
			var err error
			for compacted, err = b.disk.compactOnce(); err == nil && compacted; compacted, err = b.disk.compactOnce() {
				compactions++
			}
			require.Nil(t, err)
		}

	}

	// // this is a sanity check to make sure the test setup actually does what we
	// // want. With the current setup, we expect on average to have ~100
	// // compactions. It would be extremely unexpected to have fewer than 25.
	assert.Greater(t, compactions, 25)

	verifyBucketRangeAgainstControl(t, b, control)
}

func verifyBucketRangeAgainstControl(t *testing.T, b *Bucket, control []*sroar.Bitmap) {
	logger, _ := test.NewNullLogger()
	reader := NewBucketReaderRoaringSetRange(b.CursorRoaringSetRange, logger)

	for i, controlBM := range control {
		actual, err := reader.Read(context.Background(), uint64(i), filters.OperatorEqual)
		require.Nil(t, err)

		assert.Equal(t, controlBM.ToArray(), actual.ToArray())
	}
}

type roaringSetRangeInstruction struct {
	// is a []byte in reality, but makes the test setup easier if we pretent
	// its an int
	key     uint64
	element uint64

	// true=addition, false=deletion
	addition bool
}

func generateRandomRangeInstructions(r *rand.Rand, maxID, maxElement, iterations uint64,
	deleteRatio float64,
) []roaringSetRangeInstruction {
	instr := make([]roaringSetRangeInstruction, iterations)

	for i := range instr {
		instr[i].key = uint64(r.Intn(int(maxID)))
		instr[i].element = uint64(r.Intn(int(maxElement)))

		if r.Float64() > deleteRatio {
			instr[i].addition = true
		} else {
			instr[i].addition = false
		}
	}

	return instr
}

func controlFromRangeInstructions(instr []roaringSetRangeInstruction, maxID uint64) []*sroar.Bitmap {
	unique := make(map[uint64]uint64)
	for _, inst := range instr {
		if inst.addition {
			unique[inst.element] = inst.key
		} else {
			delete(unique, inst.element)
		}
	}

	out := make([]*sroar.Bitmap, maxID)
	for i := range out {
		out[i] = sroar.NewBitmap()
	}
	for element, key := range unique {
		out[key].Set(element)
	}

	return out
}

func compactionRoaringSetRangeStrategy(ctx context.Context, t *testing.T, opts []BucketOption,
	expectedMinSize, expectedMaxSize int64,
) {
	logger, _ := test.NewNullLogger()

	maxKey := uint64(100)

	type kv struct {
		key       uint64
		additions []uint64
		deletions []uint64
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
		for k := uint64(0); k < maxKey; k++ {
			key := k
			values := []uint64{k + 10_000, k + 20_000}

			switch k % 8 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:       key,
					additions: values[:1],
				})

				// leave this element untouched in the second segment
				expected = append(expected, kv{
					key:       key,
					additions: values[:1],
				})

			case 1:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:       key,
					additions: values[:1],
				})

				// update in the second segment
				segment2 = append(segment2, kv{
					key:       key,
					additions: values[1:],
				})

				expected = append(expected, kv{
					key:       key,
					additions: values,
				})

			case 2:
				// add both to segment 1, delete the first
				segment1 = append(segment1, kv{
					key:       key,
					additions: values,
				})

				// delete first element in the second segment
				segment2 = append(segment2, kv{
					key:       key,
					deletions: values[:1],
				})

				// only the 2nd element should be left in the expected
				expected = append(expected, kv{
					key:       key,
					additions: values[1:],
				})

			case 3:
				// add both to segment 1, delete the second
				segment1 = append(segment1, kv{
					key:       key,
					additions: values,
				})

				// delete second element in the second segment
				segment2 = append(segment2, kv{
					key:       key,
					deletions: values[1:],
				})

				// only the 1st element should be left in the expected
				expected = append(expected, kv{
					key:       key,
					additions: values[:1],
				})

			case 4:
				// do not add to segment 1

				// only add to segment 2 (first entry)
				segment2 = append(segment2, kv{
					key:       key,
					additions: values,
				})

				expected = append(expected, kv{
					key:       key,
					additions: values,
				})

			case 5:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:       key,
					additions: values[:1],
				})
				previous2 = append(previous2, kv{
					key:       key,
					additions: values[1:],
				})

				// delete in segment 1
				segment1 = append(segment1, kv{
					key:       key,
					deletions: values,
				})

				// should not have any values in expected at all, not even a key

			case 6:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:       key,
					additions: values[:1],
				})
				previous2 = append(previous2, kv{
					key:       key,
					additions: values[1:],
				})

				// delete in segment 2
				segment2 = append(segment2, kv{
					key:       key,
					deletions: values,
				})

				// should not have any values in expected at all, not even a key

			case 7:
				// part of a previous segment
				previous1 = append(previous1, kv{
					key:       key,
					additions: values[:1],
				})
				previous2 = append(previous2, kv{
					key:       key,
					additions: values[1:],
				})

				expected = append(expected, kv{
					key:       key,
					additions: values,
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
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("import and flush previous segments", func(t *testing.T) {
		for _, kv := range previous1 {
			err := bucket.RoaringSetRangeAdd(kv.key, kv.additions...)
			require.NoError(t, err)
		}

		require.NoError(t, bucket.FlushAndSwitch())

		for _, kv := range previous2 {
			err := bucket.RoaringSetRangeAdd(kv.key, kv.additions...)
			require.NoError(t, err)
		}

		require.NoError(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, kv := range segment1 {
			if len(kv.additions) > 0 {
				err := bucket.RoaringSetRangeAdd(kv.key, kv.additions...)
				require.NoError(t, err)
			}
			if len(kv.deletions) > 0 {
				err := bucket.RoaringSetRangeRemove(kv.key, kv.deletions...)
				require.NoError(t, err)
			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.NoError(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 2", func(t *testing.T) {
		for _, kv := range segment2 {
			if len(kv.additions) > 0 {
				err := bucket.RoaringSetRangeAdd(kv.key, kv.additions...)
				require.NoError(t, err)
			}
			if len(kv.deletions) > 0 {
				err := bucket.RoaringSetRangeRemove(kv.key, kv.deletions...)
				require.NoError(t, err)
			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.NoError(t, bucket.FlushAndSwitch())
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		reader := NewBucketReaderRoaringSetRange(bucket.CursorRoaringSetRange, logger)
		for k := uint64(0); k < maxKey; k++ {
			bm, err := reader.Read(context.Background(), k, filters.OperatorEqual)
			require.NoError(t, err)

			if !bm.IsEmpty() {
				retrieved = append(retrieved, kv{
					key:       k,
					additions: bm.ToArray(),
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
				assertSecondSegmentOfSize(t, bucket, 2256, 2256)
			}
			i++
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction", func(t *testing.T) {
		var retrieved []kv

		reader := NewBucketReaderRoaringSetRange(bucket.CursorRoaringSetRange, logger)
		for k := uint64(0); k < maxKey; k++ {
			bm, err := reader.Read(context.Background(), k, filters.OperatorEqual)
			require.NoError(t, err)

			if !bm.IsEmpty() {
				retrieved = append(retrieved, kv{
					key:       k,
					additions: bm.ToArray(),
				})
			}
		}

		assert.Equal(t, expected, retrieved)
		assertSingleSegmentOfSize(t, bucket, expectedMinSize, expectedMaxSize)
	})
}

func compactionRoaringSetRangeStrategy_RemoveUnnecessary(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	iterations := uint64(100)
	key := uint64(1)

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("write segments", func(t *testing.T) {
		for v := uint64(0); v < iterations; v++ {
			if v != 0 {
				// we can only delete an existing value if this isn't the first write
				err := bucket.RoaringSetRangeRemove(key, v-1)
				require.NoError(t, err)
			}

			err := bucket.RoaringSetRangeAdd(key, v)
			require.NoError(t, err)

			require.NoError(t, bucket.FlushAndSwitch())
		}
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		reader := NewBucketReaderRoaringSetRange(bucket.CursorRoaringSetRange, logger)
		bm, err := reader.Read(context.Background(), key, filters.OperatorEqual)
		require.NoError(t, err)

		assert.Equal(t, []uint64{iterations - 1}, bm.ToArray())
	})

	// t.Run("compact until no longer eligible", func(t *testing.T) {
	// 	var compacted bool
	// 	var err error
	// 	for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
	// 	}
	// 	require.Nil(t, err)
	// })

	// t.Run("verify control before compaction", func(t *testing.T) {
	// 	reader := NewBucketReaderRoaringSetRange(bucket.CursorRoaringSetRange)
	// 	bm, err := reader.Read(context.Background(), key, filters.OperatorEqual)
	// 	require.NoError(t, err)

	// 	assert.Equal(t, []uint64{iterations - 1}, bm.ToArray())
	// })
}

func compactionRoaringSetRangeStrategy_FrequentPutDeleteOperations(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction works well for set collection
	maxSegments := 10

	for segments := 4; segments < maxSegments; segments++ {
		t.Run(fmt.Sprintf("compact %v segments", segments), func(t *testing.T) {
			var bucket *Bucket

			key := uint64(1)
			values := []uint64{1, 2}

			dirName := t.TempDir()

			t.Run("init bucket", func(t *testing.T) {
				b, err := NewBucketCreator().NewBucket(ctx, dirName, "", nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.Nil(t, err)

				// so big it effectively never triggers as part of this test
				b.SetMemtableThreshold(1e9)

				bucket = b
			})

			t.Run("import and flush segments", func(t *testing.T) {
				for segment := 0; segment < segments; segment++ {
					err := bucket.RoaringSetRangeAdd(key, values...)
					require.Nil(t, err)

					if segments == 5 {
						// delete all
						err := bucket.RoaringSetRangeRemove(key, values[0])
						require.Nil(t, err)
						err = bucket.RoaringSetRangeRemove(key, values[1])
						require.Nil(t, err)
					} else if segments == 6 {
						// delete only one value
						err := bucket.RoaringSetRangeRemove(key, values[0])
						require.Nil(t, err)
					} else if segment != segments-1 {
						// don't delete from the last segment
						err := bucket.RoaringSetRangeRemove(key, values[0])
						require.Nil(t, err)
						err = bucket.RoaringSetRangeRemove(key, values[1])
						require.Nil(t, err)
					}

					require.Nil(t, bucket.FlushAndSwitch())
				}
			})

			t.Run("verify that objects exist before compaction", func(t *testing.T) {
				reader := NewBucketReaderRoaringSetRange(bucket.CursorRoaringSetRange, logger)
				bm, err := reader.Read(context.Background(), key, filters.OperatorEqual)
				require.NoError(t, err)

				if segments == 5 {
					assert.Equal(t, 0, bm.GetCardinality())
				} else if segments == 6 {
					assert.Equal(t, 1, bm.GetCardinality())
				} else {
					assert.Equal(t, 2, bm.GetCardinality())
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
				reader := NewBucketReaderRoaringSetRange(bucket.CursorRoaringSetRange, logger)
				bm, err := reader.Read(context.Background(), key, filters.OperatorEqual)
				require.NoError(t, err)

				if segments == 5 {
					assert.Equal(t, 0, bm.GetCardinality())
				} else if segments == 6 {
					assert.Equal(t, 1, bm.GetCardinality())
				} else {
					assert.Equal(t, 2, bm.GetCardinality())
				}
			})
		})
	}
}
