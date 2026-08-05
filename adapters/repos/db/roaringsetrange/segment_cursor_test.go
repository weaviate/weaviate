//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	segSize := int64(len(seg))

	t.Run("starting from beginning", func(t *testing.T) {
		c := NewSegmentCursorPread(readSeeker, segSize, 1)
		key, layer, ok := c.First()
		require.True(t, ok)
		assert.Equal(t, uint8(0), key)
		assert.Equal(t, []uint64{0, 1}, layer.Additions.ToArray())
		assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
	})

	t.Run("starting from beginning, page through all", func(t *testing.T) {
		c := NewSegmentCursorPread(readSeeker, segSize, 1)
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
		c := NewSegmentCursorPread(readSeeker, segSize, 1)
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

	t.Run("empty segment", func(t *testing.T) {
		c := NewSegmentCursorPread(bytes.NewReader(createDummySegment(t, 0)), 0, 1)

		_, _, ok := c.First()
		assert.False(t, ok)

		_, _, ok = c.Next()
		assert.False(t, ok)
	})

	// once a segment holds real data it outgrows the read buffer, so paging has
	// to survive refills and nodes that do not fit in the buffer at all
	t.Run("segment larger than the read buffer", func(t *testing.T) {
		tests := []struct {
			name           string
			nodes          int
			bufferMultiple int
		}{
			{name: "payload spans several buffer fills", nodes: 8, bufferMultiple: 3},
			{name: "a single node exceeds the buffer", nodes: 1, bufferMultiple: 2},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				seg, additions := createOversizedSegment(t, test.nodes, test.bufferMultiple)
				c := NewSegmentCursorPread(bytes.NewReader(seg), int64(len(seg)), 1)

				i := 0
				for key, layer, ok := c.First(); ok; key, layer, ok = c.Next() {
					require.Equal(t, uint8(i), key)
					assert.Equal(t, additions[i], layer.Additions.ToArray())

					if i == 0 {
						assert.Equal(t, []uint64{2, 3}, layer.Deletions.ToArray())
					} else {
						assert.True(t, layer.Deletions.IsEmpty())
					}
					i++
				}

				assert.Equal(t, test.nodes, i)
			})
		}
	})
}

// TestSegmentCursorPreadReadBufferSize pins the read buffer to the smaller of
// the payload and the cap, so a cursor neither allocates more than it pages
// through nor grows back to a size that is expensive once per segment.
func TestSegmentCursorPreadReadBufferSize(t *testing.T) {
	small := createDummySegment(t, 5)
	large, _ := createOversizedSegment(t, 8, 3)
	require.Less(t, len(small), segmentCursorPreadMaxBufferSize)

	tests := []struct {
		name        string
		payloadSize int64
		expected    int
	}{
		{name: "payload below the cap", payloadSize: int64(len(small)), expected: len(small)},
		{name: "payload above the cap", payloadSize: int64(len(large)), expected: segmentCursorPreadMaxBufferSize},
		{name: "payload size unknown", payloadSize: 0, expected: segmentCursorPreadMaxBufferSize},
		{name: "payload size negative", payloadSize: -1, expected: segmentCursorPreadMaxBufferSize},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := NewSegmentCursorPread(bytes.NewReader(small), test.payloadSize, 1)

			assert.Equal(t, test.expected, c.reader.Size())
		})
	}
}

// An understated payload only costs extra reads, as NewSegmentCursorPread
// promises. A size of 1 drops the buffer to the 16 bytes bufio floors it at,
// below even a single node, so every node arrives across several refills.
func TestSegmentCursorPreadUnderstatedPayloadSize(t *testing.T) {
	seg := createDummySegment(t, 5)
	c := NewSegmentCursorPread(bytes.NewReader(seg), 1, 1)

	i := uint64(0)
	for key, layer, ok := c.First(); ok; key, layer, ok = c.Next() {
		require.Equal(t, uint8(i), key)
		assert.Equal(t, []uint64{i * 4, i*4 + 1}, layer.Additions.ToArray())
		i++
	}

	assert.Equal(t, uint64(5), i)
}

func createDummySegment(t *testing.T, count uint64) []byte {
	entries := make([]segmentEntry, count)
	for i := range entries {
		entries[i] = segmentEntry{
			key:       uint8(i),
			additions: []uint64{uint64(i) * 4, uint64(i)*4 + 1},
			deletions: []uint64{uint64(i)*4 + 2, uint64(i)*4 + 3}, // stored only for key 0
		}
	}

	return createSegmentsFromEntries(t, entries)
}

// createOversizedSegment builds a segment bufferMultiple times larger than the
// cursor's read buffer, spread over the given node count, so paging it crosses
// buffer refills and the direct reads bufio uses for nodes that do not fit.
func createOversizedSegment(t *testing.T, nodes, bufferMultiple int) ([]byte, [][]uint64) {
	// the spread-out ids below serialize to just under a byte each, so one id
	// per wanted byte clears the target with headroom
	idsPerNode := bufferMultiple * segmentCursorPreadMaxBufferSize / nodes

	entries := make([]segmentEntry, nodes)
	additions := make([][]uint64, nodes)
	for i := range entries {
		ids := make([]uint64, idsPerNode)
		for j := range ids {
			// spread the ids out so the bitmaps stay too large to compress away
			ids[j] = uint64(i*idsPerNode+j) * 7
		}
		additions[i] = ids
		entries[i] = segmentEntry{key: uint8(i), additions: ids, deletions: []uint64{2, 3}}
	}

	seg := createSegmentsFromEntries(t, entries)
	require.Greater(t, len(seg), segmentCursorPreadMaxBufferSize,
		"fixture must exceed the cursor read buffer or it tests nothing")

	return seg, additions
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
