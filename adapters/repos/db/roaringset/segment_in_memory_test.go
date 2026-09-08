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

package roaringset

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/concurrency"
)

func TestSegmentInMemory(t *testing.T) {
	logger, _ := test.NewNullLogger()

	// layer 1 -> 2 -> 3 covers add -> delete -> re-add of the same id
	// (key-a: 2, key-b: 10), a deletion of a never-added id (key-c: 999)
	// and a key whose merged bitmap empties out completely (key-b after layer 2)
	expectedFinal := map[string][]uint64{
		"key-a": {2, 3},
		"key-b": {10},
		"key-c": {100, 101},
		"key-d": {5},
	}

	t.Run("bitmaps start empty", func(t *testing.T) {
		s := NewSegmentInMemory(logger)

		assert.Empty(t, s.bitmaps)
		assert.Equal(t, 0, s.Size())
	})

	t.Run("size is sum of bitmap sizes", func(t *testing.T) {
		l1, _, _ := createTestLayers()
		expected := NewBitmap(1, 2, 3).LenInBytes() +
			NewBitmap(10).LenInBytes() +
			NewBitmap(100, 101).LenInBytes()

		s := NewSegmentInMemory(logger)
		require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(l1)))

		assert.Equal(t, expected, s.Size())
	})

	t.Run("merging", func(t *testing.T) {
		t.Run("segments", func(t *testing.T) {
			l1, l2, l3 := createTestLayers()

			seg := NewSegmentInMemory(logger)
			require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(l1)))
			assertMergedBitmaps(t, seg, map[string][]uint64{
				"key-a": {1, 2, 3},
				"key-b": {10},
				"key-c": {100, 101},
			})

			require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(l2)))
			// key-b emptied out and was dropped from the map
			assertMergedBitmaps(t, seg, map[string][]uint64{
				"key-a": {1, 3},
				"key-c": {100, 101},
				"key-d": {5},
			})

			require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(l3)))
			assertMergedBitmaps(t, seg, expectedFinal)
		})

		t.Run("memtables", func(t *testing.T) {
			l1, l2, l3 := createTestLayers()

			seg := NewSegmentInMemory(logger)
			seg.MergeMemtableEventually(layersToTree(l1))
			seg.MergeMemtableEventually(layersToTree(l2))
			seg.MergeMemtableEventually(layersToTree(l3))

			waitUntilDrained(t, seg)
			assertMergedBitmaps(t, seg, expectedFinal)
		})

		t.Run("segments + memtable", func(t *testing.T) {
			l1, l2, l3 := createTestLayers()

			seg := NewSegmentInMemory(logger)
			require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(l1)))
			require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(l2)))
			seg.MergeMemtableEventually(layersToTree(l3))

			waitUntilDrained(t, seg)
			assertMergedBitmaps(t, seg, expectedFinal)
		})

		t.Run("cursor error is propagated", func(t *testing.T) {
			l1, _, _ := createTestLayers()

			cursor := newFakeSegmentCursor(l1)
			cursor.err = fmt.Errorf("cursor failed")
			cursor.errPos = 1

			seg := NewSegmentInMemory(logger)
			require.ErrorContains(t, seg.MergeSegmentByCursor(cursor), "cursor failed")
		})
	})

	t.Run("get", func(t *testing.T) {
		t.Run("missing key", func(t *testing.T) {
			seg := NewSegmentInMemory(logger)

			root, release, found, pending := seg.Get([]byte("key-a"), NewBitmapBufPoolNoop())
			defer release()

			assert.False(t, found)
			assert.Nil(t, root.Additions)
			assert.Empty(t, pending)
		})

		t.Run("cloned root is stable across merges", func(t *testing.T) {
			l1, l2, _ := createTestLayers()

			seg := NewSegmentInMemory(logger)
			seg.MergeMemtableEventually(layersToTree(l1))
			waitUntilDrained(t, seg)

			root, release, found, pending := seg.Get([]byte("key-a"), NewBitmapBufPoolNoop())
			require.True(t, found)
			require.Empty(t, pending)
			snapshot := root.Additions.ToArray()

			// merging a deletion for the same key must not touch the handed-out
			// clone, proving the shared bitmap never escapes
			seg.MergeMemtableEventually(layersToTree(l2))
			waitUntilDrained(t, seg)

			assert.Equal(t, []uint64{1, 2, 3}, snapshot)
			assert.Equal(t, []uint64{1, 2, 3}, root.Additions.ToArray())
			release()

			root2, release2, found2, _ := seg.Get([]byte("key-a"), NewBitmapBufPoolNoop())
			defer release2()
			require.True(t, found2)
			assert.Equal(t, []uint64{1, 3}, root2.Additions.ToArray())
		})

		t.Run("pending memtables are part of the snapshot", func(t *testing.T) {
			l1, _, _ := createTestLayers()

			seg := NewSegmentInMemory(logger)
			seg.MergeMemtableEventually(layersToTree(l1))

			// the background merge may or may not have run yet; either way the
			// flattened snapshot (root + pending layers) must be complete
			merged, release := getFlattened(t, seg, []byte("key-a"))
			defer release()

			assert.ElementsMatch(t, []uint64{1, 2, 3}, merged.ToArray())
		})
	})

	t.Run("simultaneous read & write", func(t *testing.T) {
		l1, l2, l3 := createTestLayers()

		seg := NewSegmentInMemory(logger)
		assertKey := func(expected []uint64, key string) {
			t.Helper()
			merged, release := getFlattened(t, seg, []byte(key))
			defer release()
			assert.ElementsMatch(t, expected, merged.ToArray())
		}

		// every flattened snapshot is taken while a background merge may be in
		// flight; the results must be identical before, during and after it
		seg.MergeMemtableEventually(layersToTree(l1))
		assertKey([]uint64{1, 2, 3}, "key-a")
		assertKey([]uint64{10}, "key-b")

		seg.MergeMemtableEventually(layersToTree(l2))
		assertKey([]uint64{1, 3}, "key-a")
		assertKey([]uint64{}, "key-b")
		assertKey([]uint64{5}, "key-d")

		seg.MergeMemtableEventually(layersToTree(l3))
		assertKey([]uint64{2, 3}, "key-a")
		assertKey([]uint64{10}, "key-b")

		waitUntilDrained(t, seg)
		assertMergedBitmaps(t, seg, expectedFinal)

		// after the merge only the root serves the read, no pending layers
		_, release, found, pending := seg.Get([]byte("key-a"), NewBitmapBufPoolNoop())
		defer release()
		assert.True(t, found)
		assert.Empty(t, pending)
	})
}

// getFlattened mirrors the bucket's in-memory read path: merged root plus one
// layer per pending memtable, flattened with in-place mutation of the root
// clone
func getFlattened(t *testing.T, s *SegmentInMemory, key []byte) (*sroar.Bitmap, func()) {
	t.Helper()

	root, release, found, pending := s.Get(key, NewBitmapBufPoolNoop())
	layers := BitmapLayers{}
	if found {
		layers = append(layers, root)
	}
	for _, mt := range pending {
		layer, err := mt.Get(key)
		if err == nil {
			layers = append(layers, layer)
		}
	}
	return layers.Flatten(false, concurrency.SROAR_MERGE), release
}

func assertMergedBitmaps(t *testing.T, s *SegmentInMemory, expected map[string][]uint64) {
	t.Helper()

	// exact length pins emptied-out keys being dropped from the map
	require.Len(t, s.bitmaps, len(expected))
	for key, expectedElems := range expected {
		bm, ok := s.bitmaps[key]
		require.True(t, ok, "key %q missing", key)
		assert.ElementsMatch(t, expectedElems, bm.ToArray())
	}
}

func waitUntilDrained(t *testing.T, s *SegmentInMemory) {
	t.Helper()
	require.Eventually(t, func() bool { return s.pending.Len() == 0 }, time.Second, 10*time.Millisecond)
}

func createTestLayers() (map[string]BitmapLayer, map[string]BitmapLayer, map[string]BitmapLayer) {
	return map[string]BitmapLayer{
			"key-a": {Additions: NewBitmap(1, 2, 3), Deletions: NewBitmap()},
			"key-b": {Additions: NewBitmap(10), Deletions: NewBitmap()},
			"key-c": {Additions: NewBitmap(100, 101), Deletions: NewBitmap()},
		},
		map[string]BitmapLayer{
			"key-a": {Additions: NewBitmap(), Deletions: NewBitmap(2)},
			"key-b": {Additions: NewBitmap(), Deletions: NewBitmap(10)},
			"key-c": {Additions: NewBitmap(), Deletions: NewBitmap(999)},
			"key-d": {Additions: NewBitmap(5), Deletions: NewBitmap()},
		},
		map[string]BitmapLayer{
			"key-a": {Additions: NewBitmap(2), Deletions: NewBitmap(1)},
			"key-b": {Additions: NewBitmap(10), Deletions: NewBitmap()},
		}
}

func layersToTree(layers map[string]BitmapLayer) *BinarySearchTree {
	tree := &BinarySearchTree{}
	for key, layer := range layers {
		tree.Insert([]byte(key), Insert{
			Additions: layer.Additions.ToArray(),
			Deletions: layer.Deletions.ToArray(),
		})
	}
	return tree
}

type fakeSegmentCursor struct {
	keys   [][]byte
	layers []BitmapLayer
	pos    int
	err    error
	errPos int
}

func newFakeSegmentCursor(layersByKey map[string]BitmapLayer) *fakeSegmentCursor {
	keys := make([][]byte, 0, len(layersByKey))
	for key := range layersByKey {
		keys = append(keys, []byte(key))
	}
	slices.SortFunc(keys, bytes.Compare)

	c := &fakeSegmentCursor{pos: -1, errPos: -1}
	for _, key := range keys {
		c.keys = append(c.keys, key)
		c.layers = append(c.layers, layersByKey[string(key)])
	}
	return c
}

func (c *fakeSegmentCursor) First() ([]byte, BitmapLayer, error) {
	c.pos = 0
	return c.current()
}

func (c *fakeSegmentCursor) Next() ([]byte, BitmapLayer, error) {
	c.pos++
	return c.current()
}

func (c *fakeSegmentCursor) Seek(key []byte) ([]byte, BitmapLayer, error) {
	c.pos = sort.Search(len(c.keys), func(i int) bool { return bytes.Compare(c.keys[i], key) >= 0 })
	return c.current()
}

func (c *fakeSegmentCursor) current() ([]byte, BitmapLayer, error) {
	if c.err != nil && c.pos == c.errPos {
		return nil, BitmapLayer{}, c.err
	}
	if c.pos < 0 || c.pos >= len(c.keys) {
		return nil, BitmapLayer{}, nil
	}
	return c.keys[c.pos], c.layers[c.pos], nil
}
