//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package modulestorage

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ModuleStorage(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()

	r, err := NewRepo(dirName, logger)
	require.Nil(t, err)

	module1, err := r.Storage("my-module")
	require.Nil(t, err)
	module2, err := r.Storage("my-other-module")
	require.Nil(t, err)

	t.Run("storing two k/v pairs for each bucket", func(t *testing.T) {
		err := module1.Put([]byte("module1-key1"), []byte("module1-value1"))
		require.Nil(t, err)
		err = module1.Put([]byte("module1-key2"), []byte("module1-value2"))
		require.Nil(t, err)
		err = module2.Put([]byte("module2-key1"), []byte("module2-value1"))
		require.Nil(t, err)
		err = module2.Put([]byte("module2-key2"), []byte("module2-value2"))
		require.Nil(t, err)
	})

	t.Run("retrieving values across buckets and keys", func(t *testing.T) {
		var v []byte
		var err error

		// on module 1 bucket
		v, err = module1.Get([]byte("module1-key1"))
		require.Nil(t, err)
		assert.Equal(t, []byte("module1-value1"), v)

		v, err = module1.Get([]byte("module1-key2"))
		require.Nil(t, err)
		assert.Equal(t, []byte("module1-value2"), v)

		v, err = module1.Get([]byte("module2-key1"))
		require.Nil(t, err)
		assert.Equal(t, []byte(nil), v)

		v, err = module1.Get([]byte("module2-key2"))
		require.Nil(t, err)
		assert.Equal(t, []byte(nil), v)

		// on module 2 bucket
		v, err = module2.Get([]byte("module1-key1"))
		require.Nil(t, err)
		assert.Equal(t, []byte(nil), v)

		v, err = module2.Get([]byte("module1-key2"))
		require.Nil(t, err)
		assert.Equal(t, []byte(nil), v)

		v, err = module2.Get([]byte("module2-key1"))
		require.Nil(t, err)
		assert.Equal(t, []byte("module2-value1"), v)

		v, err = module2.Get([]byte("module2-key2"))
		require.Nil(t, err)
		assert.Equal(t, []byte("module2-value2"), v)
	})

	t.Run("scanning all k/v for a bucket", func(t *testing.T) {
		t.Run("module1 - full range", func(t *testing.T) {
			var (
				keys   [][]byte
				values [][]byte
			)
			expectedKeys := [][]byte{
				[]byte("module1-key1"),
				[]byte("module1-key2"),
			}
			expectedValues := [][]byte{
				[]byte("module1-value1"),
				[]byte("module1-value2"),
			}

			err := module1.Scan(func(k, v []byte) (bool, error) {
				keys = append(keys, k)
				values = append(values, v)
				return true, nil
			})

			require.Nil(t, err)

			assert.Equal(t, expectedKeys, keys)
			assert.Equal(t, expectedValues, values)
		})

		t.Run("module2 - full range", func(t *testing.T) {
			var (
				keys   [][]byte
				values [][]byte
			)
			expectedKeys := [][]byte{
				[]byte("module2-key1"),
				[]byte("module2-key2"),
			}
			expectedValues := [][]byte{
				[]byte("module2-value1"),
				[]byte("module2-value2"),
			}

			err := module2.Scan(func(k, v []byte) (bool, error) {
				keys = append(keys, k)
				values = append(values, v)
				return true, nil
			})

			require.Nil(t, err)

			assert.Equal(t, expectedKeys, keys)
			assert.Equal(t, expectedValues, values)
		})

		t.Run("module2 - stop after single row", func(t *testing.T) {
			var (
				keys   [][]byte
				values [][]byte
			)
			expectedKeys := [][]byte{
				[]byte("module2-key1"),
			}
			expectedValues := [][]byte{
				[]byte("module2-value1"),
			}

			err := module2.Scan(func(k, v []byte) (bool, error) {
				keys = append(keys, k)
				values = append(values, v)
				return false, nil
			})

			require.Nil(t, err)

			assert.Equal(t, expectedKeys, keys)
			assert.Equal(t, expectedValues, values)
		})

		t.Run("module2 - with scan error", func(t *testing.T) {
			err := module2.Scan(func(k, v []byte) (bool, error) {
				return false, fmt.Errorf("oops")
			})

			assert.Equal(t, "read item \"module2-key1\": oops", err.Error())
		})
	})
}
