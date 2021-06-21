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
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplaceStrategy_RecoverFromWAL(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirNameOriginal := fmt.Sprintf("./testdata/%d-original", rand.Intn(10000000))
	dirNameRecovered := fmt.Sprintf("./testdata/%d-recovered", rand.Intn(10000000))
	os.MkdirAll(dirNameOriginal, 0o777)
	os.MkdirAll(dirNameRecovered, 0o777)
	defer func() {
		err := os.RemoveAll(dirNameOriginal)
		fmt.Println(err)
		err = os.RemoveAll(dirNameRecovered)
		fmt.Println(err)
	}()

	t.Run("without previous state", func(t *testing.T) {
		b, err := NewBucket(dirNameOriginal, WithStrategy(StrategyReplace))
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

		t.Run("delete one, update one", func(t *testing.T) {
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			updated3 := []byte("updated value for key 3")

			err = b.Delete(key2)
			require.Nil(t, err)

			err = b.Put(key3, updated3)
			require.Nil(t, err)
		})

		t.Run("verify control", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			updated3 := []byte("updated value for key 3")
			res, err := b.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = b.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, updated3)
		})

		t.Run("make sure the WAL is flushed", func(t *testing.T) {
			require.Nil(t, b.WriteWAL())
		})

		t.Run("copy state into recovery folder and destroy original", func(t *testing.T) {
			cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("cp -r %s/*.wal %s",
				dirNameOriginal, dirNameRecovered))
			var out bytes.Buffer
			cmd.Stderr = &out
			err := cmd.Run()
			if err != nil {
				fmt.Println(out.String())
				t.Fatal(err)
			}
			b = nil
			require.Nil(t, os.RemoveAll(dirNameOriginal))
		})

		var bRec *Bucket

		t.Run("create new bucket from existing state", func(t *testing.T) {
			b, err := NewBucket(dirNameRecovered, WithStrategy(StrategyReplace))
			require.Nil(t, err)

			// so big it effectively never triggers as part of this test
			b.SetMemtableThreshold(1e9)

			bRec = b
		})

		t.Run("verify all data is present", func(t *testing.T) {
			key1 := []byte("key-1")
			key2 := []byte("key-2")
			key3 := []byte("key-3")
			orig1 := []byte("original value for key1")
			updated3 := []byte("updated value for key 3")
			res, err := bRec.Get(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = bRec.Get(key2)
			require.Nil(t, err)
			assert.Nil(t, res)
			res, err = bRec.Get(key3)
			require.Nil(t, err)
			assert.Equal(t, res, updated3)
		})
	})
}

func TestSetStrategy_RecoverFromWAL(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirNameOriginal := fmt.Sprintf("./testdata/%d-original", rand.Intn(10000000))
	dirNameRecovered := fmt.Sprintf("./testdata/%d-recovered", rand.Intn(10000000))
	os.MkdirAll(dirNameOriginal, 0o777)
	os.MkdirAll(dirNameRecovered, 0o777)
	defer func() {
		err := os.RemoveAll(dirNameOriginal)
		fmt.Println(err)
		err = os.RemoveAll(dirNameRecovered)
		fmt.Println(err)
	}()

	t.Run("without prior state", func(t *testing.T) {
		b, err := NewBucket(dirNameOriginal, WithStrategy(StrategySetCollection))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test1-key-1")
		key2 := []byte("test1-key-2")
		key3 := []byte("test1-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, orig2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, orig3, res)
		})

		t.Run("delete individual keys", func(t *testing.T) {
			delete2 := []byte("value 2.1")
			delete3 := []byte("value 3.2")

			err = b.SetDeleteSingle(key2, delete2)
			require.Nil(t, err)
			err = b.SetDeleteSingle(key3, delete3)
			require.Nil(t, err)
		})

		t.Run("re-add keys which were previously deleted and new ones", func(t *testing.T) {
			readd2 := [][]byte{[]byte("value 2.1"), []byte("value 2.3")}
			readd3 := [][]byte{[]byte("value 3.2"), []byte("value 3.3")}

			err = b.SetAdd(key2, readd2)
			require.Nil(t, err)
			err = b.SetAdd(key3, readd3)
			require.Nil(t, err)
		})

		t.Run("validate the results prior to recovery", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{
				[]byte("value 2.2"), // from original import
				[]byte("value 2.1"), // added again after initial deletion
				[]byte("value 2.3"), // newly added
			}
			expected3 := [][]byte{
				[]byte("value 3.1"), // form original import
				[]byte("value 3.2"), // added again after initial deletion
				[]byte("value 3.3"), // newly added
			} // value2 deleted

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})

		t.Run("make sure the WAL is flushed", func(t *testing.T) {
			require.Nil(t, b.WriteWAL())
		})

		t.Run("copy state into recovery folder and destroy original", func(t *testing.T) {
			cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("cp -r %s/*.wal %s",
				dirNameOriginal, dirNameRecovered))
			var out bytes.Buffer
			cmd.Stderr = &out
			err := cmd.Run()
			if err != nil {
				fmt.Println(out.String())
				t.Fatal(err)
			}
			b = nil
			require.Nil(t, os.RemoveAll(dirNameOriginal))
		})

		var bRec *Bucket

		t.Run("create new bucket from existing state", func(t *testing.T) {
			b, err := NewBucket(dirNameRecovered, WithStrategy(StrategySetCollection))
			require.Nil(t, err)

			// so big it effectively never triggers as part of this test
			b.SetMemtableThreshold(1e9)

			bRec = b
		})

		t.Run("validate the results after recovery", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{
				[]byte("value 2.2"), // from original import
				[]byte("value 2.1"), // added again after initial deletion
				[]byte("value 2.3"), // newly added
			}
			expected3 := [][]byte{
				[]byte("value 3.1"), // form original import
				[]byte("value 3.2"), // added again after initial deletion
				[]byte("value 3.3"), // newly added
			} // value2 deleted

			res, err := bRec.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = bRec.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = bRec.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})
	})
}
