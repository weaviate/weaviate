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

	// t.Run("with a single flush between updates", func(t *testing.T) {
	// 	b, err := NewBucketWithStrategy(dirName, StrategyMapCollection)
	// 	require.Nil(t, err)

	// 	// so big it effectively never triggers as part of this test
	// 	b.SetMemtableThreshold(1e9)

	// 	rowKey1 := []byte("test1-key-1")
	// 	rowKey2 := []byte("test1-key-2")

	// 	t.Run("set original values and verify", func(t *testing.T) {
	// 		row1Map := []MapPair{
	// 			{
	// 				Key:   []byte("row1-key1"),
	// 				Value: []byte("row1-key1-value1"),
	// 			}, {
	// 				Key:   []byte("row1-key2"),
	// 				Value: []byte("row1-key2-value1"),
	// 			},
	// 		}

	// 		row2Map := []MapPair{
	// 			{
	// 				Key:   []byte("row2-key1"),
	// 				Value: []byte("row2-key1-value1"),
	// 			}, {
	// 				Key:   []byte("row2-key2"),
	// 				Value: []byte("row2-key2-value1"),
	// 			},
	// 		}

	// 		for _, pair := range row1Map {
	// 			err = b.MapSet(rowKey1, pair)
	// 			require.Nil(t, err)
	// 		}

	// 		for _, pair := range row2Map {
	// 			err = b.MapSet(rowKey2, pair)
	// 			require.Nil(t, err)
	// 		}

	// 		res, err := b.MapList(rowKey1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, row1Map, res)
	// 		res, err = b.MapList(rowKey2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, row2Map)
	// 	})

	// 	t.Run("flush to disk", func(t *testing.T) {
	// 		require.Nil(t, b.FlushAndSwitch())
	// 	})

	// 	t.Run("replace an existing map key", func(t *testing.T) {
	// 		err = b.MapSet(rowKey1, MapPair{
	// 			Key:   []byte("row1-key1"),        // existing key
	// 			Value: []byte("row1-key1-value2"), // updated value
	// 		})
	// 		require.Nil(t, err)

	// 		row1Updated := []MapPair{
	// 			{
	// 				Key:   []byte("row1-key1"),
	// 				Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
	// 			}, {
	// 				Key:   []byte("row1-key2"),
	// 				Value: []byte("row1-key2-value1"),
	// 			},
	// 		}

	// 		row2Unchanged := []MapPair{
	// 			{
	// 				Key:   []byte("row2-key1"),
	// 				Value: []byte("row2-key1-value1"),
	// 			}, {
	// 				Key:   []byte("row2-key2"),
	// 				Value: []byte("row2-key2-value1"),
	// 			},
	// 		}

	// 		res, err := b.MapList(rowKey1)
	// 		require.Nil(t, err)
	// 		// NOTE: We are accepting that the order is changed here. Given the name
	// 		// "MapCollection" there should be no expectations regarding the order,
	// 		// but we have yet to validate if this fits with all of the intended use
	// 		// cases.
	// 		assert.ElementsMatch(t, row1Updated, res)
	// 		res, err = b.MapList(rowKey2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, row2Unchanged)
	// 	})
	// })

	// t.Run("with flushes after initial and update", func(t *testing.T) {
	// 	b, err := NewBucketWithStrategy(dirName, StrategySetCollection)
	// 	require.Nil(t, err)

	// 	// so big it effectively never triggers as part of this test
	// 	b.SetMemtableThreshold(1e9)
	// 	key1 := []byte("test-3-key-1")
	// 	key2 := []byte("test-3-key-2")
	// 	key3 := []byte("test-3-key-3")

	// 	t.Run("set original values and verify", func(t *testing.T) {
	// 		orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
	// 		orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
	// 		orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

	// 		err = b.Append(key1, orig1)
	// 		require.Nil(t, err)
	// 		err = b.Append(key2, orig2)
	// 		require.Nil(t, err)
	// 		err = b.Append(key3, orig3)
	// 		require.Nil(t, err)

	// 		res, err := b.GetCollection(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig1)
	// 		res, err = b.GetCollection(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig2)
	// 		res, err = b.GetCollection(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig3)
	// 	})

	// 	t.Run("flush to disk", func(t *testing.T) {
	// 		require.Nil(t, b.FlushAndSwitch())
	// 	})

	// 	t.Run("replace some, keep one", func(t *testing.T) {
	// 		orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
	// 		orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
	// 		orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
	// 		append2 := [][]byte{[]byte("value 2.3")}
	// 		append3 := [][]byte{[]byte("value 3.3")}

	// 		err = b.Append(key2, append2)
	// 		require.Nil(t, err)
	// 		err = b.Append(key3, append3)
	// 		require.Nil(t, err)

	// 		// Flush again!
	// 		require.Nil(t, b.FlushAndSwitch())

	// 		res, err := b.GetCollection(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, orig1, res)
	// 		res, err = b.GetCollection(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, append(orig2, append2...), res)
	// 		res, err = b.GetCollection(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, append(orig3, append3...), res)
	// 	})
	// })

	// t.Run("update in memtable, then do an orderly shutdown, and re-init", func(t *testing.T) {
	// 	b, err := NewBucketWithStrategy(dirName, StrategySetCollection)
	// 	require.Nil(t, err)

	// 	// so big it effectively never triggers as part of this test
	// 	b.SetMemtableThreshold(1e9)

	// 	key1 := []byte("test4-key-1")
	// 	key2 := []byte("test4-key-2")
	// 	key3 := []byte("test4-key-3")

	// 	t.Run("set original values and verify", func(t *testing.T) {
	// 		orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
	// 		orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
	// 		orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

	// 		err = b.Append(key1, orig1)
	// 		require.Nil(t, err)
	// 		err = b.Append(key2, orig2)
	// 		require.Nil(t, err)
	// 		err = b.Append(key3, orig3)
	// 		require.Nil(t, err)

	// 		res, err := b.GetCollection(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig1)
	// 		res, err = b.GetCollection(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig2)
	// 		res, err = b.GetCollection(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig3)
	// 	})

	// 	t.Run("replace some, keep one", func(t *testing.T) {
	// 		orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
	// 		orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
	// 		orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
	// 		append2 := [][]byte{[]byte("value 2.3")}
	// 		append3 := [][]byte{[]byte("value 3.3")}

	// 		err = b.Append(key2, append2)
	// 		require.Nil(t, err)
	// 		err = b.Append(key3, append3)
	// 		require.Nil(t, err)

	// 		res, err := b.GetCollection(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, orig1, res)
	// 		res, err = b.GetCollection(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, append(orig2, append2...), res)
	// 		res, err = b.GetCollection(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, append(orig3, append3...), res)
	// 	})

	// 	t.Run("orderly shutdown", func(t *testing.T) {
	// 		b.Shutdown(context.Background())
	// 	})

	// 	t.Run("init another bucket on the same files", func(t *testing.T) {
	// 		b2, err := NewBucketWithStrategy(dirName, StrategySetCollection)
	// 		require.Nil(t, err)

	// 		orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
	// 		orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
	// 		orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
	// 		append2 := [][]byte{[]byte("value 2.3")}
	// 		append3 := [][]byte{[]byte("value 3.3")}

	// 		res, err := b2.GetCollection(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, orig1, res)
	// 		res, err = b2.GetCollection(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, append(orig2, append2...), res)
	// 		res, err = b2.GetCollection(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, append(orig3, append3...), res)
	// 	})
	// })
}
