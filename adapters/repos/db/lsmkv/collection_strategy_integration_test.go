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

func TestCollectionStrategy_InsertAndAppend(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyCollection)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.Append(key1, orig1)
			require.Nil(t, err)
			err = b.Append(key2, orig2)
			require.Nil(t, err)
			err = b.Append(key3, orig3)
			require.Nil(t, err)

			res, err := b.GetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.GetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.GetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
			append2 := [][]byte{[]byte("value 2.3")}
			append3 := [][]byte{[]byte("value 3.3")}

			err = b.Append(key2, append2)
			require.Nil(t, err)
			err = b.Append(key3, append3)
			require.Nil(t, err)

			res, err := b.GetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.GetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, append(orig2, append2...))
			res, err = b.GetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, append(orig3, append3...))
		})
	})

	// t.Run("with single flush in between updates", func(t *testing.T) {
	// 	b, err := NewBucketWithStrategy(dirName, StrategyReplace)
	// 	require.Nil(t, err)

	// 	// so big it effectively never triggers as part of this test
	// 	b.SetMemtableThreshold(1e9)

	// 	t.Run("set original values and verify", func(t *testing.T) {
	// 		key1 := []byte("key-1")
	// 		key2 := []byte("key-2")
	// 		key3 := []byte("key-3")
	// 		orig1 := []byte("original value for key1")
	// 		orig2 := []byte("original value for key2")
	// 		orig3 := []byte("original value for key3")

	// 		err = b.Put(key1, orig1)
	// 		require.Nil(t, err)
	// 		err = b.Put(key2, orig2)
	// 		require.Nil(t, err)
	// 		err = b.Put(key3, orig3)
	// 		require.Nil(t, err)

	// 		res, err := b.Get(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig1)
	// 		res, err = b.Get(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig2)
	// 		res, err = b.Get(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig3)
	// 	})

	// 	t.Run("flush memtable to disk", func(t *testing.T) {
	// 		require.Nil(t, b.FlushAndSwitch())
	// 	})

	// 	t.Run("replace some, keep one", func(t *testing.T) {
	// 		key1 := []byte("key-1")
	// 		key2 := []byte("key-2")
	// 		key3 := []byte("key-3")
	// 		orig1 := []byte("original value for key1")
	// 		replaced2 := []byte("updated value for key2")
	// 		replaced3 := []byte("updated value for key3")

	// 		err = b.Put(key2, replaced2)
	// 		require.Nil(t, err)
	// 		err = b.Put(key3, replaced3)
	// 		require.Nil(t, err)

	// 		res, err := b.Get(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, orig1, res)
	// 		res, err = b.Get(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, replaced2, res)
	// 		res, err = b.Get(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, replaced3, res)
	// 	})
	// })

	// t.Run("with a flush after the initial write and after the update", func(t *testing.T) {
	// 	b, err := NewBucketWithStrategy(dirName, StrategyReplace)
	// 	require.Nil(t, err)

	// 	// so big it effectively never triggers as part of this test
	// 	b.SetMemtableThreshold(1e9)

	// 	t.Run("set original values and verify", func(t *testing.T) {
	// 		key1 := []byte("key-1")
	// 		key2 := []byte("key-2")
	// 		key3 := []byte("key-3")
	// 		orig1 := []byte("original value for key1")
	// 		orig2 := []byte("original value for key2")
	// 		orig3 := []byte("original value for key3")

	// 		err = b.Put(key1, orig1)
	// 		require.Nil(t, err)
	// 		err = b.Put(key2, orig2)
	// 		require.Nil(t, err)
	// 		err = b.Put(key3, orig3)
	// 		require.Nil(t, err)

	// 		res, err := b.Get(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig1)
	// 		res, err = b.Get(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig2)
	// 		res, err = b.Get(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig3)
	// 	})

	// 	t.Run("flush memtable to disk", func(t *testing.T) {
	// 		require.Nil(t, b.FlushAndSwitch())
	// 	})

	// 	t.Run("replace some, keep one", func(t *testing.T) {
	// 		key1 := []byte("key-1")
	// 		key2 := []byte("key-2")
	// 		key3 := []byte("key-3")
	// 		orig1 := []byte("original value for key1")
	// 		replaced2 := []byte("updated value for key2")
	// 		replaced3 := []byte("updated value for key3")

	// 		err = b.Put(key2, replaced2)
	// 		require.Nil(t, err)
	// 		err = b.Put(key3, replaced3)
	// 		require.Nil(t, err)

	// 		// Flush before verifying!
	// 		require.Nil(t, b.FlushAndSwitch())

	// 		res, err := b.Get(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig1)
	// 		res, err = b.Get(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, replaced2)
	// 		res, err = b.Get(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, replaced3)
	// 	})
	// })

	// t.Run("update in memtable, then do an orderly shutdown, and re-init", func(t *testing.T) {
	// 	b, err := NewBucketWithStrategy(dirName, StrategyReplace)
	// 	require.Nil(t, err)

	// 	// so big it effectively never triggers as part of this test
	// 	b.SetMemtableThreshold(1e9)

	// 	t.Run("set original values and verify", func(t *testing.T) {
	// 		key1 := []byte("key-1")
	// 		key2 := []byte("key-2")
	// 		key3 := []byte("key-3")
	// 		orig1 := []byte("original value for key1")
	// 		orig2 := []byte("original value for key2")
	// 		orig3 := []byte("original value for key3")

	// 		err = b.Put(key1, orig1)
	// 		require.Nil(t, err)
	// 		err = b.Put(key2, orig2)
	// 		require.Nil(t, err)
	// 		err = b.Put(key3, orig3)
	// 		require.Nil(t, err)
	// 	})

	// 	t.Run("replace some, keep one", func(t *testing.T) {
	// 		key1 := []byte("key-1")
	// 		key2 := []byte("key-2")
	// 		key3 := []byte("key-3")
	// 		orig1 := []byte("original value for key1")
	// 		replaced2 := []byte("updated value for key2")
	// 		replaced3 := []byte("updated value for key3")

	// 		err = b.Put(key2, replaced2)
	// 		require.Nil(t, err)
	// 		err = b.Put(key3, replaced3)
	// 		require.Nil(t, err)

	// 		res, err := b.Get(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig1)
	// 		res, err = b.Get(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, replaced2)
	// 		res, err = b.Get(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, replaced3)
	// 	})

	// 	t.Run("orderly shutdown", func(t *testing.T) {
	// 		b.Shutdown(context.Background())
	// 	})

	// 	t.Run("init another bucket on the same files", func(t *testing.T) {
	// 		b2, err := NewBucketWithStrategy(dirName, StrategyReplace)
	// 		require.Nil(t, err)

	// 		key1 := []byte("key-1")
	// 		key2 := []byte("key-2")
	// 		key3 := []byte("key-3")
	// 		orig1 := []byte("original value for key1")
	// 		replaced2 := []byte("updated value for key2")
	// 		replaced3 := []byte("updated value for key3")

	// 		res, err := b2.Get(key1)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, orig1)
	// 		res, err = b2.Get(key2)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, replaced2)
	// 		res, err = b2.Get(key3)
	// 		require.Nil(t, err)
	// 		assert.Equal(t, res, replaced3)
	// 	})
	// })
}
