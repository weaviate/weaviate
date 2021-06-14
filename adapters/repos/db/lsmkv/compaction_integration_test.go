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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	rand.Seed(time.Now().UnixNano())
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
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
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

	rand.Seed(time.Now().UnixNano())
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
		b, err := NewBucketWithStrategy(dirName, StrategySetCollection)
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
