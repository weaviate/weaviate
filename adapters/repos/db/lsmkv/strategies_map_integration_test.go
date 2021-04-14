// +build integrationTest

package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapCollectionStrategy_InsertAndAppend(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})

	t.Run("with a single flush between updates", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test2-key-1")
		rowKey2 := []byte("test2-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})

	t.Run("with flushes after initial and upate", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test3-key-1")
		rowKey2 := []byte("test3-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)

			// Flush again!
			require.Nil(t, b.FlushAndSwitch())

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})

	t.Run("update in memtable, then do an orderly shutdown, and re-init", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test4-key-1")
		rowKey2 := []byte("test4-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)
		})

		t.Run("orderly shutdown", func(t *testing.T) {
			b.Shutdown(context.Background())
		})

		t.Run("init another bucket on the same files", func(t *testing.T) {
			b2, err := NewBucketWithStrategy(dirName, StrategySetCollection)
			require.Nil(t, err)

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b2.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b2.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})
}

func TestMapCollectionStrategy_InsertAndDelete(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("delete some keys, re-add one of them", func(t *testing.T) {
			err := b.MapDeleteKey(rowKey1, []byte("row1-key1"))
			require.Nil(t, err)
			err = b.MapDeleteKey(rowKey2, []byte("row2-key2"))
			require.Nil(t, err)
			err = b.MapSet(rowKey2, MapPair{
				Key:   []byte("row2-key2"),
				Value: []byte("row2-key2-reinserted"),
			})
		})

		t.Run("validate the results", func(t *testing.T) {
			row1Updated := []MapPair{
				// key 1 was deleted
				{
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Updated := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-reinserted"),
				},
			}

			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.ElementsMatch(t, row2Updated, res)
		})
	})

	t.Run("with flushes between updates", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete some keys, re-add one of them", func(t *testing.T) {
			err := b.MapDeleteKey(rowKey1, []byte("row1-key1"))
			require.Nil(t, err)
			err = b.MapDeleteKey(rowKey2, []byte("row2-key2"))
			require.Nil(t, err)
			err = b.MapSet(rowKey2, MapPair{
				Key:   []byte("row2-key2"),
				Value: []byte("row2-key2-reinserted"),
			})
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("validate the results", func(t *testing.T) {
			row1Updated := []MapPair{
				// key 1 was deleted
				{
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Updated := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-reinserted"),
				},
			}

			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.ElementsMatch(t, row2Updated, res)
		})
	})

	t.Run("with memtable only, then an orderly shutdown and restart", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("delete some keys, re-add one of them", func(t *testing.T) {
			err := b.MapDeleteKey(rowKey1, []byte("row1-key1"))
			require.Nil(t, err)
			err = b.MapDeleteKey(rowKey2, []byte("row2-key2"))
			require.Nil(t, err)
			err = b.MapSet(rowKey2, MapPair{
				Key:   []byte("row2-key2"),
				Value: []byte("row2-key2-reinserted"),
			})
		})

		t.Run("orderly shutdown", func(t *testing.T) {
			b.Shutdown(context.Background())
		})

		t.Run("init another bucket on the same files", func(t *testing.T) {
			b2, err := NewBucketWithStrategy(dirName, StrategySetCollection)
			require.Nil(t, err)

			row1Updated := []MapPair{
				// key 1 was deleted
				{
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Updated := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-reinserted"),
				},
			}

			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			res, err := b2.MapList(rowKey1)
			require.Nil(t, err)
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b2.MapList(rowKey2)
			require.Nil(t, err)
			assert.ElementsMatch(t, row2Updated, res)
		})
	})
}
