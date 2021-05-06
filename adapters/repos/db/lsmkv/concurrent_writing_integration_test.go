package lsmkv

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test continiuously writes into a bucket with a small memtable threshold,
// so that a lot of flushing is happening while writing. This is to ensure that
// there will be no lost writes or other inconsistencies under load
func TestConcurrentWriting_Replace(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	amount := 2000
	sizePerValue := 128

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	bucket, err := NewBucketWithStrategyAndThreshold(dirName, StrategyReplace, 10000)
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		for i := range keys {
			uuid, err := uuid.New().MarshalBinary()
			require.Nil(t, err)
			keys[i] = uuid

			values[i] = make([]byte, sizePerValue)
			rand.Read(values[i])
		}
	})

	t.Run("import", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := range keys {
			time.Sleep(50 * time.Microsecond)
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := bucket.Put(keys[index], values[index])
				assert.Nil(t, err)
			}(i)
		}
		wg.Wait()
	})

	t.Run("verify get", func(t *testing.T) {
		correct := 0
		var missingKeys []int

		for i := range keys {
			value, err := bucket.Get(keys[i])
			assert.Nil(t, err)
			if bytes.Equal(values[i], value) {
				correct++
			} else {
				missingKeys = append(missingKeys, i)
			}
		}

		if len(missingKeys) > 0 {
			fmt.Printf("missing keys: %v\n", missingKeys)
		}
		assert.Equal(t, amount, correct)
	})

	t.Run("verify cursor", func(t *testing.T) {
		correct := 0
		// put all key value/pairs in a map so we can access them by key
		targets := map[string][]byte{}

		for i := range keys {
			targets[string(keys[i])] = values[i]
		}

		c := bucket.Cursor()
		defer c.Close()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			control := targets[string(k)]
			if bytes.Equal(control, v) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

// This test continiuously writes into a bucket with a small memtable threshold,
// so that a lot of flushing is happening while writing. This is to ensure that
// there will be no lost writes or other inconsistencies under load
func TestConcurrentWriting_Set(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	amount := 2000
	valuesPerKey := 4
	sizePerValue := 32

	keys := make([][]byte, amount)
	values := make([][][]byte, amount)

	bucket, err := NewBucketWithStrategyAndThreshold(dirName, StrategySetCollection, 10000)
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		for i := range keys {
			uuid, err := uuid.New().MarshalBinary()
			require.Nil(t, err)
			keys[i] = uuid

			values[i] = make([][]byte, valuesPerKey)
			for j := range values[i] {
				values[i][j] = make([]byte, sizePerValue)
				rand.Read(values[i][j])
			}
		}
	})

	t.Run("import", func(t *testing.T) {
		wg := sync.WaitGroup{}

		for i := range keys {
			time.Sleep(50 * time.Microsecond)
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				err := bucket.SetAdd(keys[index], values[index])
				assert.Nil(t, err)
			}(i)
		}
		wg.Wait()
	})

	t.Run("verify get", func(t *testing.T) {
		correct := 0

		for i := range keys {
			value, err := bucket.SetList(keys[i])
			assert.Nil(t, err)
			if reflect.DeepEqual(values[i], value) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	t.Run("verify cursor", func(t *testing.T) {
		correct := 0
		// put all key value/pairs in a map so we can access them by key
		targets := map[string][][]byte{}

		for i := range keys {
			targets[string(keys[i])] = values[i]
		}

		c := bucket.SetCursor()
		defer c.Close()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			control := targets[string(k)]
			if reflect.DeepEqual(control, v) {
				correct++
			}
		}

		assert.Equal(t, amount, correct)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}
