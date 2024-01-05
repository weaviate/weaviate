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

package segmentindex

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func TestTree(t *testing.T) {
	type elem struct {
		key   []byte
		start uint64
		end   uint64
	}

	tree := NewTree(4)

	elements := []elem{
		{
			key:   []byte("foobar"),
			start: 17,
			end:   18,
		},
		{
			key:   []byte("abc"),
			start: 4,
			end:   5,
		},
		{
			key:   []byte("zzz"),
			start: 34,
			end:   35,
		},
		{
			key:   []byte("aaa"),
			start: 1,
			end:   2,
		},
		{
			// makes the tree slightly imbalanced to the right, which in turn assures
			// that we have a nil node in between
			key:   []byte("zzzz"),
			start: 100,
			end:   102,
		},
	}

	t.Run("inserting", func(t *testing.T) {
		for _, elem := range elements {
			tree.Insert(elem.key, elem.start, elem.end)
		}
	})

	t.Run("exact get", func(t *testing.T) {
		key, start, end := tree.Get([]byte("foobar"))
		assert.Equal(t, []byte("foobar"), key)
		assert.Equal(t, uint64(17), start)
		assert.Equal(t, uint64(18), end)

		key, start, end = tree.Get([]byte("abc"))
		assert.Equal(t, []byte("abc"), key)
		assert.Equal(t, uint64(4), start)
		assert.Equal(t, uint64(5), end)

		key, start, end = tree.Get([]byte("zzz"))
		assert.Equal(t, []byte("zzz"), key)
		assert.Equal(t, uint64(34), start)
		assert.Equal(t, uint64(35), end)

		key, start, end = tree.Get([]byte("aaa"))
		assert.Equal(t, []byte("aaa"), key)
		assert.Equal(t, uint64(1), start)
		assert.Equal(t, uint64(2), end)

		key, start, end = tree.Get([]byte("zzzz"))
		assert.Equal(t, []byte("zzzz"), key)
		assert.Equal(t, uint64(100), start)
		assert.Equal(t, uint64(102), end)
	})

	t.Run("marshalling and then reading the byte representation", func(t *testing.T) {
		bytes, err := tree.MarshalBinary()
		require.Nil(t, err)

		dTree := NewDiskTree(bytes)

		t.Run("get", func(t *testing.T) {
			n, err := dTree.Get([]byte("foobar"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("foobar"), n.Key)
			assert.Equal(t, uint64(17), n.Start)
			assert.Equal(t, uint64(18), n.End)

			n, err = dTree.Get([]byte("abc"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("abc"), n.Key)
			assert.Equal(t, uint64(4), n.Start)
			assert.Equal(t, uint64(5), n.End)

			n, err = dTree.Get([]byte("zzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzz"), n.Key)
			assert.Equal(t, uint64(34), n.Start)
			assert.Equal(t, uint64(35), n.End)

			n, err = dTree.Get([]byte("aaa"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("aaa"), n.Key)
			assert.Equal(t, uint64(1), n.Start)
			assert.Equal(t, uint64(2), n.End)

			n, err = dTree.Get([]byte("zzzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzzz"), n.Key)
			assert.Equal(t, uint64(100), n.Start)
			assert.Equal(t, uint64(102), n.End)
		})

		t.Run("seek", func(t *testing.T) {
			n, err := dTree.Seek([]byte("foobar"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("foobar"), n.Key)
			assert.Equal(t, uint64(17), n.Start)
			assert.Equal(t, uint64(18), n.End)

			n, err = dTree.Seek([]byte("f"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("foobar"), n.Key)
			assert.Equal(t, uint64(17), n.Start)
			assert.Equal(t, uint64(18), n.End)

			n, err = dTree.Seek([]byte("abc"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("abc"), n.Key)
			assert.Equal(t, uint64(4), n.Start)
			assert.Equal(t, uint64(5), n.End)

			n, err = dTree.Seek([]byte("ab"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("abc"), n.Key)
			assert.Equal(t, uint64(4), n.Start)
			assert.Equal(t, uint64(5), n.End)

			n, err = dTree.Seek([]byte("zzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzz"), n.Key)
			assert.Equal(t, uint64(34), n.Start)
			assert.Equal(t, uint64(35), n.End)

			n, err = dTree.Seek([]byte("z"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzz"), n.Key)
			assert.Equal(t, uint64(34), n.Start)
			assert.Equal(t, uint64(35), n.End)

			n, err = dTree.Seek([]byte("aaa"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("aaa"), n.Key)
			assert.Equal(t, uint64(1), n.Start)
			assert.Equal(t, uint64(2), n.End)

			n, err = dTree.Seek([]byte("a"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("aaa"), n.Key)
			assert.Equal(t, uint64(1), n.Start)
			assert.Equal(t, uint64(2), n.End)

			n, err = dTree.Seek([]byte("zzzz"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzzz"), n.Key)
			assert.Equal(t, uint64(100), n.Start)
			assert.Equal(t, uint64(102), n.End)

			n, err = dTree.Seek([]byte("zzza"))
			assert.Nil(t, err)
			assert.Equal(t, []byte("zzzz"), n.Key)
			assert.Equal(t, uint64(100), n.Start)
			assert.Equal(t, uint64(102), n.End)

			n, err = dTree.Seek([]byte("zzzzz"))
			assert.Equal(t, lsmkv.NotFound, err)
		})

		t.Run("get all keys (for building bloom filters at segment init time)", func(t *testing.T) {
			expected := [][]byte{
				[]byte("aaa"),
				[]byte("abc"),
				[]byte("foobar"),
				[]byte("zzz"),
				[]byte("zzzz"),
			}

			keys, err := dTree.AllKeys()

			require.Nil(t, err)
			assert.ElementsMatch(t, expected, keys)
		})
	})
}
