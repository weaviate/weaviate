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

package roaringsetrange

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestSegmentCursorMmap(t *testing.T) {
	seg := createDummySegment(t, 5)

	t.Run("starting from beginning", func(t *testing.T) {
		c := NewSegmentCursorMmap(seg)
		key, layer, ok := c.First()
		require.True(t, ok)
		assert.Equal(t, uint8(0), key)
		assert.Equal(t, []uint64{0, 1}, layer.Additions.ToArray())
		assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
	})

	t.Run("starting from beginning, page through all", func(t *testing.T) {
		c := NewSegmentCursorMmap(seg)
		i := uint64(0)
		for key, layer, ok := c.First(); ok; key, layer, ok = c.Next() {
			assert.Equal(t, uint8(i), key)
			assert.Equal(t, []uint64{i * 4, i*4 + 1}, layer.Additions.ToArray())

			if i == 0 {
				assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
			} else {
				assert.True(t, layer.Deletions.IsEmpty())
			}
			i++
		}

		assert.Equal(t, uint64(5), i)
	})

	t.Run("no first, page through all", func(t *testing.T) {
		c := NewSegmentCursorMmap(seg)
		i := uint64(0)
		for key, layer, ok := c.Next(); ok; key, layer, ok = c.Next() {
			assert.Equal(t, uint8(i), key)
			assert.Equal(t, []uint64{i * 4, i*4 + 1}, layer.Additions.ToArray())

			if i == 0 {
				assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
			} else {
				assert.True(t, layer.Deletions.IsEmpty())
			}
			i++
		}

		assert.Equal(t, uint64(5), i)
	})
}

func TestSegmentCursorPread(t *testing.T) {
	seg := createDummySegment(t, 5)
	readSeeker := bytes.NewReader(seg)

	t.Run("starting from beginning", func(t *testing.T) {
		c := NewSegmentCursorPread(readSeeker, 1)
		key, layer, ok := c.First()
		require.True(t, ok)
		assert.Equal(t, uint8(0), key)
		assert.Equal(t, []uint64{0, 1}, layer.Additions.ToArray())
		assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
	})

	t.Run("starting from beginning, page through all", func(t *testing.T) {
		c := NewSegmentCursorPread(readSeeker, 1)
		i := uint64(0)
		for key, layer, ok := c.First(); ok; key, layer, ok = c.Next() {
			assert.Equal(t, uint8(i), key)
			assert.Equal(t, []uint64{i * 4, i*4 + 1}, layer.Additions.ToArray())

			if i == 0 {
				assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
			} else {
				assert.True(t, layer.Deletions.IsEmpty())
			}
			i++
		}

		assert.Equal(t, uint64(5), i)
	})

	t.Run("no first, page through all", func(t *testing.T) {
		c := NewSegmentCursorPread(readSeeker, 1)
		i := uint64(0)
		for key, layer, ok := c.Next(); ok; key, layer, ok = c.Next() {
			assert.Equal(t, uint8(i), key)
			assert.Equal(t, []uint64{i * 4, i*4 + 1}, layer.Additions.ToArray())

			if i == 0 {
				assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
			} else {
				assert.True(t, layer.Deletions.IsEmpty())
			}
			i++
		}

		assert.Equal(t, uint64(5), i)
	})
}

func createDummySegment(t *testing.T, count uint64) []byte {
	out := []byte{}

	for i := uint64(0); i < count; i++ {
		key := uint8(i)
		add := roaringset.NewBitmap(i*4, i*4+1)
		del := roaringset.NewBitmap(i*4+2, i*4+3) // ignored for key != 0
		sn, err := NewSegmentNode(key, add, del)
		require.Nil(t, err)

		out = append(out, sn.ToBuffer()...)
	}

	return out
}

func TestGaplessSegmentCursor(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("with empty SegmentCursor", func(t *testing.T) {
		cur := &GaplessSegmentCursor{cursor: newFakeSegmentCursor(NewMemtable(logger))}

		k, v, ok := cur.First()
		require.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.Nil(t, v.Additions)
		assert.Nil(t, v.Deletions)

		for i := uint8(1); i < 65; i++ {
			k, v, ok = cur.Next()
			require.Equal(t, i, k)
			require.True(t, ok)
			assert.Nil(t, v.Additions)
			assert.Nil(t, v.Deletions)
		}

		k, v, ok = cur.Next()
		require.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v.Additions)
		assert.Nil(t, v.Deletions)
	})

	t.Run("with populated SegmentCursor", func(t *testing.T) {
		mem := NewMemtable(logger)
		mem.Insert(0, []uint64{10, 20})    // 0000
		mem.Insert(5, []uint64{15, 25})    // 0101
		mem.Insert(13, []uint64{113, 213}) // 1101
		cur := &GaplessSegmentCursor{cursor: newFakeSegmentCursor(mem)}

		k, v, ok := cur.First()
		require.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, v.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, v.Deletions.ToArray())

		expected := map[uint8][]uint64{
			1: {15, 25, 113, 213},
			3: {15, 25, 113, 213},
			4: {113, 213},
		}

		for i := uint8(1); i < 65; i++ {
			k, v, ok := cur.Next()
			require.Equal(t, i, k)
			require.True(t, ok)

			if _, ok := expected[i]; ok {
				assert.ElementsMatch(t, expected[i], v.Additions.ToArray())
			} else {
				assert.Nil(t, v.Additions)
			}
			assert.Nil(t, v.Deletions)
		}

		k, v, ok = cur.Next()
		require.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v.Additions)
		assert.Nil(t, v.Deletions)
	})
}

type fakeSegmentCursor struct {
	nodes   []*MemtableNode
	nextPos int
}

func newFakeSegmentCursor(memtable *Memtable) *fakeSegmentCursor {
	return &fakeSegmentCursor{nodes: memtable.Nodes()}
}

func (c *fakeSegmentCursor) First() (uint8, roaringset.BitmapLayer, bool) {
	c.nextPos = 0
	return c.Next()
}

func (c *fakeSegmentCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	if c.nextPos >= len(c.nodes) {
		return 0, roaringset.BitmapLayer{}, false
	}

	mn := c.nodes[c.nextPos]
	c.nextPos++

	return mn.Key, roaringset.BitmapLayer{
		Additions: mn.Additions,
		Deletions: mn.Deletions,
	}, true
}
