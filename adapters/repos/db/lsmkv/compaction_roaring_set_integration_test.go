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
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func compactionRoaringSetStrategy_Random(ctx context.Context, t *testing.T, opts []BucketOption) {
	maxID := uint64(100)
	maxElement := uint64(1e6)
	iterations := uint64(100_000)

	deleteRatio := 0.2   // 20% of all operations will be deletes, 80% additions
	flushChance := 0.001 // on average one flush per 1000 iterations

	r := getRandomSeed()

	instr := generateRandomInstructions(r, maxID, maxElement, iterations, deleteRatio)
	control := controlFromInstructions(instr, maxID)

	b, err := NewBucket(ctx, t.TempDir(), "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)

	defer b.Shutdown(testCtx())

	// so big it effectively never triggers as part of this test
	b.SetMemtableThreshold(1e9)

	compactions := 0
	for _, inst := range instr {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, inst.key)
		if inst.addition {
			b.RoaringSetAddOne(key, inst.element)
		} else {
			b.RoaringSetRemoveOne(key, inst.element)
		}

		if r.Float64() < flushChance {
			require.Nil(t, b.FlushAndSwitch())

			for compacted, err := b.disk.compactOnce(); err == nil && compacted; compacted, err = b.disk.compactOnce() {
				require.Nil(t, err)
				compactions++
			}
		}

	}

	// this is a sanity check to make sure the test setup actually does what we
	// want. With the current setup, we expect on average to have ~100
	// compactions. It would be extremely unexpected to have fewer than 25.
	assert.Greater(t, compactions, 25)

	verifyBucketAgainstControl(t, b, control)
}

func verifyBucketAgainstControl(t *testing.T, b *Bucket, control []*sroar.Bitmap) {
	// This test was built before the bucket had cursors, so we are retrieving
	// each key individually, rather than cursing over the entire bucket.
	// However, this is also good for isolation purposes, this test tests
	// compactions, not cursors.

	for i, controlBM := range control {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))

		actual, err := b.RoaringSetGet(key)
		require.Nil(t, err)

		assert.Equal(t, controlBM.ToArray(), actual.ToArray())

	}
}

type roaringSetInstruction struct {
	// is a []byte in reality, but makes the test setup easier if we pretent
	// its an int
	key     uint64
	element uint64

	// true=addition, false=deletion
	addition bool
}

func generateRandomInstructions(r *rand.Rand, maxID, maxElement, iterations uint64,
	deleteRatio float64,
) []roaringSetInstruction {
	instr := make([]roaringSetInstruction, iterations)

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

func controlFromInstructions(instr []roaringSetInstruction, maxID uint64) []*sroar.Bitmap {
	out := make([]*sroar.Bitmap, maxID)
	for i := range out {
		out[i] = sroar.NewBitmap()
	}

	for _, inst := range instr {
		if inst.addition {
			out[inst.key].Set(inst.element)
		} else {
			out[inst.key].Remove(inst.element)
		}
	}

	return out
}

func compactionRoaringSetStrategy(ctx context.Context, t *testing.T, opts []BucketOption,
	expectedMinSize, expectedMaxSize int64,
) {
	size := 100

	type kv struct {
		key       []byte
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
		for i := 0; i < size; i++ {
			key := []byte(fmt.Sprintf("key-%02d", i))
			value1 := uint64(i) + 1
			value2 := uint64(i) + 2
			values := []uint64{value1, value2}

			switch i % 8 {
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
		b, err := NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("import and flush previous segments", func(t *testing.T) {
		for _, kv := range previous1 {
			err := bucket.RoaringSetAddList(kv.key, kv.additions)
			require.NoError(t, err)
		}

		require.NoError(t, bucket.FlushAndSwitch())

		for _, kv := range previous2 {
			err := bucket.RoaringSetAddList(kv.key, kv.additions)
			require.NoError(t, err)
		}

		require.NoError(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, kv := range segment1 {
			if len(kv.additions) > 0 {
				err := bucket.RoaringSetAddList(kv.key, kv.additions)
				require.NoError(t, err)
			}
			for i := range kv.deletions {
				err := bucket.RoaringSetRemoveOne(kv.key, kv.deletions[i])
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
				err := bucket.RoaringSetAddList(kv.key, kv.additions)
				require.NoError(t, err)
			}
			for i := range kv.deletions {
				err := bucket.RoaringSetRemoveOne(kv.key, kv.deletions[i])
				require.NoError(t, err)
			}
		}
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.NoError(t, bucket.FlushAndSwitch())
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.CursorRoaringSet()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:       k,
				additions: v.ToArray(),
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
				assertSecondSegmentOfSize(t, bucket, 26768, 26768)
			}
			i++
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.CursorRoaringSet()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:       k,
				additions: v.ToArray(),
			})
		}

		assert.Equal(t, expected, retrieved)
		assertSingleSegmentOfSize(t, bucket, expectedMinSize, expectedMaxSize)
	})
}

func compactionRoaringSetStrategy_RemoveUnnecessary(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key    []byte
		values []uint64
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
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
				err := bucket.RoaringSetRemoveOne(key, uint64(i)-1)
				require.NoError(t, err)
			}

			err := bucket.RoaringSetAddOne(key, uint64(i))
			require.NoError(t, err)

			require.NoError(t, bucket.FlushAndSwitch())
		}
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv
		expected := []kv{
			{
				key:    key,
				values: []uint64{uint64(size) - 1},
			},
		}

		c := bucket.CursorRoaringSet()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:    k,
				values: v.ToArray(),
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
				values: []uint64{uint64(size) - 1},
			},
		}

		c := bucket.CursorRoaringSet()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			retrieved = append(retrieved, kv{
				key:    k,
				values: v.ToArray(),
			})
		}

		assert.Equal(t, expected, retrieved)
	})
}

func compactionRoaringSetStrategy_FrequentPutDeleteOperations(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction works well for set collection
	maxSize := 10

	for size := 4; size < maxSize; size++ {
		t.Run(fmt.Sprintf("compact %v segments", size), func(t *testing.T) {
			var bucket *Bucket

			key := []byte("key-original")
			value1 := uint64(1)
			value2 := uint64(2)
			values := []uint64{value1, value2}

			dirName := t.TempDir()

			t.Run("init bucket", func(t *testing.T) {
				b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.Nil(t, err)

				// so big it effectively never triggers as part of this test
				b.SetMemtableThreshold(1e9)

				bucket = b
			})

			t.Run("import and flush segments", func(t *testing.T) {
				for i := 0; i < size; i++ {
					err := bucket.RoaringSetAddList(key, values)
					require.Nil(t, err)

					if size == 5 {
						// delete all
						err := bucket.RoaringSetRemoveOne(key, values[0])
						require.Nil(t, err)
						err = bucket.RoaringSetRemoveOne(key, values[1])
						require.Nil(t, err)
					} else if size == 6 {
						// delete only one value
						err := bucket.RoaringSetRemoveOne(key, values[0])
						require.Nil(t, err)
					} else if i != size-1 {
						// don't delete from the last segment
						err := bucket.RoaringSetRemoveOne(key, values[0])
						require.Nil(t, err)
						err = bucket.RoaringSetRemoveOne(key, values[1])
						require.Nil(t, err)
					}

					require.Nil(t, bucket.FlushAndSwitch())
				}
			})

			t.Run("verify that objects exist before compaction", func(t *testing.T) {
				res, err := bucket.RoaringSetGet(key)
				require.NoError(t, err)
				if size == 5 {
					assert.Equal(t, 0, res.GetCardinality())
				} else if size == 6 {
					assert.Equal(t, 1, res.GetCardinality())
				} else {
					assert.Equal(t, 2, res.GetCardinality())
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
				res, err := bucket.RoaringSetGet(key)
				require.NoError(t, err)
				if size == 5 {
					assert.Equal(t, 0, res.GetCardinality())
				} else if size == 6 {
					assert.Equal(t, 1, res.GetCardinality())
				} else {
					assert.Equal(t, 2, res.GetCardinality())
				}
			})
		})
	}
}
