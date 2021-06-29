//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCtx() context.Context {
	return context.Background()
}

func Test_CompactionReplaceStrategy(t *testing.T) {
	size := 20

	type kv struct {
		key    []byte
		value  []byte
		delete bool
	}

	var segment1 []kv
	var segment2 []kv
	var expected []kv
	var bucket *Bucket

	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("create test data", func(t *testing.T) {
		// The test data is split into 4 scenarios evenly:
		//
		// 1.) created in the first segment, never touched again
		// 2.) created in the first segment, updated in the second
		// 3.) created in the first segment, deleted in the second
		// 4.) not present in the first segment, created in the second
		for i := 0; i < size; i++ {
			key := []byte(fmt.Sprintf("key-%2d", i))
			originalValue := []byte(fmt.Sprintf("value-%2d-original", i))

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
				updatedValue := []byte(fmt.Sprintf("value-%2d-updated", i))
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
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategyReplace))
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
			retrieved = append(retrieved, kv{
				key:   k,
				value: v,
			})
		}

		assert.Equal(t, expected, retrieved)
	})

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
		}
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

func Test_CompactionReplaceStrategy_WithSecondaryKeys(t *testing.T) {
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

	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

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
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategyReplace),
			WithSecondaryIndicies(1))
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

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
		}
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

func Test_CompactionReplaceStrategy_RemoveUnnecessaryDeletes(t *testing.T) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the indivudal segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key   []byte
		value []byte
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategyReplace))
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

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
		}
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

func Test_CompactionReplaceStrategy_RemoveUnnecessaryUpdates(t *testing.T) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the indivudal segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key   []byte
		value []byte
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategyReplace))
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

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
		}
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

func Test_CompactionSetStrategy(t *testing.T) {
	size := 30

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

	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("create test data", func(t *testing.T) {
		// The test data is split into 4 scenarios evenly:
		//
		// 1.) created in the first segment, never touched again
		// 2.) created in the first segment, appended to it in the second
		// 3.) created in the first segment, first element deleted in the second
		// 3.) created in the first segment, second element deleted in the second
		// 4.) not present in the first segment, created in the second
		// 5.) present in an unrelated previous segment, deleted in the first
		// 6.) present in an unrelated previous segment, deleted in the second
		for i := 0; i < size; i++ {
			key := []byte(fmt.Sprintf("key-%2d", i))

			value1 := []byte(fmt.Sprintf("value-%2d-01", i))
			value2 := []byte(fmt.Sprintf("value-%2d-02", i))
			values := [][]byte{value1, value2}

			switch i % 7 {
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

				// only the 2nd element should be left in the expected
				expected = append(expected, kv{
					key:    key,
					values: values[:1],
				})

			case 4:
				// do not add to segment 2

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

				// should not have any values in expected at all
				expected = append(expected, kv{
					key:    key,
					values: [][]byte{},
				})

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

				// should not have any values in expected at all
				expected = append(expected, kv{
					key:    key,
					values: [][]byte{},
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
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategySetCollection))
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

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
		}
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
	})
}

func Test_CompactionSetStrategy_RemoveUnnecessary(t *testing.T) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the indivudal segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key    []byte
		values [][]byte
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategySetCollection))
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

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
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
}

func Test_CompactionMapStrategy(t *testing.T) {
	size := 9

	type kv struct {
		key    []byte
		values []MapPair
	}

	// this segment is not part of the merge, but might still play a role in
	// overall results. For example if one of the later segments has a tombstone
	// for it
	var previous1 []kv
	var previous2 []kv

	// TODO
	_, _ = previous1, previous2

	var segment1 []kv
	var segment2 []kv
	var expected []kv
	var bucket *Bucket

	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("create test data", func(t *testing.T) {
		// The test data is split into 4 scenarios evenly:
		//
		// 1.) created in the first segment, never touched again
		// 2.) created in the first segment, appended to it in the second
		// 3.) created in the first segment, first element updated in the second
		// 4.) created in the first segment, second element updated in the second
		// 5.) created in the first segment, first element deleted in the second
		// 6.) created in the first segment, second element deleted in the second
		// 7.) not present in the first segment, created in the second
		// 8.) present in an unrelated previous segment, deleted in the first
		// 9.) present in an unrelated previous segment, deleted in the second
		for i := 0; i < size; i++ {
			rowKey := []byte(fmt.Sprintf("row-%2d", i))

			pair1 := MapPair{
				Key:   []byte(fmt.Sprintf("value-%2d-01", i)),
				Value: []byte(fmt.Sprintf("value-%2d-01-original", i)),
			}
			pair2 := MapPair{
				Key:   []byte(fmt.Sprintf("value-%2d-02", i)),
				Value: []byte(fmt.Sprintf("value-%2d-02-original", i)),
			}
			pairs := []MapPair{pair1, pair2}

			switch i % 9 {
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

				// should not have any values in expected at all
				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{},
				})

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

				// should not have any values in expected at all
				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{},
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
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategyMapCollection))
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

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
		}
	})

	t.Run("verify control after compaction", func(t *testing.T) {
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
}

func Test_CompactionMapStrategy_RemoveUnnecessary(t *testing.T) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the indivudal segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	type kv struct {
		key    []byte
		values []MapPair
	}

	key := []byte("my-key")

	var bucket *Bucket
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucket(testCtx(), dirName, nullLogger(), WithStrategy(StrategyMapCollection))
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

	t.Run("check if eligble for compaction", func(t *testing.T) {
		assert.True(t, bucket.disk.eligbleForCompaction(), "check eligle before")
	})

	t.Run("compact until no longer eligble", func(t *testing.T) {
		for bucket.disk.eligbleForCompaction() {
			require.Nil(t, bucket.disk.compactOnce())
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
}

func nullLogger() logrus.FieldLogger {
	log, _ := test.NewNullLogger()
	return log
}
