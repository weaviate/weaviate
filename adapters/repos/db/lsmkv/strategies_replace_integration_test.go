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

func TestReplaceStrategy_InsertAndUpdate(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})

	t.Run("with single flush in between updates", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, replaced2, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, replaced3, res)
		})
	})

	t.Run("with a flush after the initial write and after the update", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush memtable to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			// Flush before verifying!
			require.Nil(t, b.FlushAndSwitch())

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})

	t.Run("update in memtable, then do an orderly shutdown, and re-init", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values and verify", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			err = b.Put(key2, replaced2)
			require.Nil(t, err)
			err = b.Put(key3, replaced3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})

		t.Run("orderly shutdown", func(t *testing.T) {
			b.Shutdown(context.Background())
		})

		t.Run("init another bucket on the same files", func(t *testing.T) {
			b2, err := NewBucketWithStrategy(dirName, StrategyReplace)
			require.Nil(t, err)

			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			replaced2 := []byte("updated value for key2")
			replaced3 := []byte("updated value for key3")

			res, err := b2.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b2.Get(key2)
			require.Nil(t, err)
			assert.Equal(t, res, replaced2)
			res, err = b2.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, replaced3)
		})
	})
}

func TestReplaceStrategy_InsertAndDelete(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("delete some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")

			err = b.Delete(key2)
			require.Nil(t, err)
			err = b.Delete(key3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Nil(t, res)
		})
	})

	t.Run("with single flush in between updates", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")

			err = b.Delete(key2)
			require.Nil(t, err)
			err = b.Delete(key3)
			require.Nil(t, err)

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Nil(t, res)
		})
	})

	t.Run("with flushes after initial write and delete", func(t *testing.T) {
		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			orig2 := []byte("original value for key2")
			orig3 := []byte("original value for key3")

			err = b.Put(key1, orig1)
			require.Nil(t, err)
			err = b.Put(key2, orig2)
			require.Nil(t, err)
			err = b.Put(key3, orig3)
			require.Nil(t, err)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete some, keep one", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")

			err = b.Delete(key2)
			require.Nil(t, err)
			err = b.Delete(key3)
			require.Nil(t, err)

			// Flush again!
			require.Nil(t, b.FlushAndSwitch())

			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Nil(t, res)
		})
	})
}

func TestReplaceStrategy_Cursors(t *testing.T) {
	t.Run("memtable-only", func(t *testing.T) {
		rand.Seed(time.Now().UnixNano())
		dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
		os.MkdirAll(dirName, 0o777)
		defer func() {
			err := os.RemoveAll(dirName)
			fmt.Println(err)
		}()

		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			pairs := 20
			keys := make([][]byte, pairs)
			values := make([][]byte, pairs)

			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				values[i] = []byte(fmt.Sprintf("value-%03d", i))
			}

			// shuffle to make sure the BST isn't accidentally in order
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("value-018"),
				[]byte("value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][]byte{
				[]byte("value-000"),
				[]byte("value-001"),
				[]byte("value-002"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("replace a key", func(t *testing.T) {
			key := []byte("key-002")
			value := []byte("value-002-updated")

			err = b.Put(key, value)
			require.Nil(t, err)

			expectedKeys := [][]byte{
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][]byte{
				[]byte("value-001"),
				[]byte("value-002-updated"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			retrieved := 0
			for k, v := c.Seek([]byte("key-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})
	})

	t.Run("with a single flush", func(t *testing.T) {
		rand.Seed(time.Now().UnixNano())
		dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
		os.MkdirAll(dirName, 0o777)
		defer func() {
			err := os.RemoveAll(dirName)
			fmt.Println(err)
		}()

		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			pairs := 20
			keys := make([][]byte, pairs)
			values := make([][]byte, pairs)

			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				values[i] = []byte(fmt.Sprintf("value-%03d", i))
			}

			// shuffle to make sure the BST isn't accidentally in order
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("value-018"),
				[]byte("value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][]byte{
				[]byte("value-000"),
				[]byte("value-001"),
				[]byte("value-002"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})
	})

	t.Run("mixing several disk segments and memtable - with updates", func(t *testing.T) {
		rand.Seed(time.Now().UnixNano())
		dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
		os.MkdirAll(dirName, 0o777)
		defer func() {
			err := os.RemoveAll(dirName)
			fmt.Println(err)
		}()

		b, err := NewBucketWithStrategy(dirName, StrategyReplace)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("first third (%3==0)", func(t *testing.T) {
			pairs := 20
			var keys [][]byte
			var values [][]byte

			for i := 0; i < pairs; i++ {
				if i%3 == 0 {
					keys = append(keys, []byte(fmt.Sprintf("key-%03d", i)))
					values = append(values, []byte(fmt.Sprintf("value-%03d", i)))
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("second third (%3==1)", func(t *testing.T) {
			pairs := 20
			var keys [][]byte
			var values [][]byte

			for i := 0; i < pairs; i++ {
				if i%3 == 1 {
					keys = append(keys, []byte(fmt.Sprintf("key-%03d", i)))
					values = append(values, []byte(fmt.Sprintf("value-%03d", i)))
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("update something that was already written in segment 1", func(t *testing.T) {
			require.Nil(t, b.Put([]byte("key-000"), []byte("updated-value-000")))
			require.Nil(t, b.Delete([]byte("key-003")))
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("third third (%3==2) memtable only", func(t *testing.T) {
			pairs := 20
			var keys [][]byte
			var values [][]byte

			for i := 0; i < pairs; i++ {
				if i%3 == 2 {
					keys = append(keys, []byte(fmt.Sprintf("key-%03d", i)))
					values = append(values, []byte(fmt.Sprintf("value-%03d", i)))
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.Put(keys[i], values[i])
				require.Nil(t, err)
			}

			// no flush for this one, so this segment stays in the memtable
		})

		t.Run("update something that was already written previoulsy", func(t *testing.T) {
			require.Nil(t, b.Put([]byte("key-000"), []byte("twice-updated-value-000")))
			require.Nil(t, b.Put([]byte("key-001"), []byte("once-updated-value-001")))
			require.Nil(t, b.Put([]byte("key-019"), []byte("once-updated-value-019")))
			require.Nil(t, b.Delete([]byte("key-018")))
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				// key-018 deleted
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("once-updated-value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
				// key-003 was deleted
				[]byte("key-004"),
			}
			expectedValues := [][]byte{
				[]byte("twice-updated-value-000"),
				[]byte("once-updated-value-001"),
				[]byte("value-002"),
				[]byte("value-004"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 4; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("re-add the deleted keys", func(t *testing.T) {
			require.Nil(t, b.Put([]byte("key-003"), []byte("readded-003")))
			require.Nil(t, b.Put([]byte("key-018"), []byte("readded-018")))
			// tombstones are now only in memtable
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("readded-018"),
				[]byte("once-updated-value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
				[]byte("key-003"),
			}
			expectedValues := [][]byte{
				[]byte("twice-updated-value-000"),
				[]byte("once-updated-value-001"),
				[]byte("value-002"),
				[]byte("readded-003"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 4; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("perform a final flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][]byte{
				[]byte("value-016"),
				[]byte("value-017"),
				[]byte("readded-018"),
				[]byte("once-updated-value-019"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
				[]byte("key-003"),
			}
			expectedValues := [][]byte{
				[]byte("twice-updated-value-000"),
				[]byte("once-updated-value-001"),
				[]byte("value-002"),
				[]byte("readded-003"),
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]byte
			c := b.Cursor()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 4; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})
	})

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
