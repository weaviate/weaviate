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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type cursorEntry struct {
	key  string
	adds []uint64
	dels []uint64
}

// drain walks a cursor to exhaustion, recording what it yields.
func drain(c InnerCursor) []cursorEntry {
	var out []cursorEntry
	for k, layer, err := c.First(); k != nil && err == nil; k, layer, err = c.Next() {
		e := cursorEntry{key: string(k)}
		if layer.Additions != nil {
			e.adds = layer.Additions.ToArray()
		}
		if layer.Deletions != nil {
			e.dels = layer.Deletions.ToArray()
		}
		out = append(out, e)
	}
	return out
}

func treeWith(t *testing.T, keys []string) *BinarySearchTree {
	t.Helper()
	bst := &BinarySearchTree{}
	for i, k := range keys {
		bst.Insert([]byte(k), Insert{Additions: []uint64{uint64(i)}})
		if i%3 == 0 { // some keys carry deletions too
			bst.Insert([]byte(k), Insert{Deletions: []uint64{uint64(1000 + i)}})
		}
	}
	return bst
}

// TestCursorNoCopyMatchesCopyingCursor pins the no-copy cursor against the
// copying one it shortcuts: walked from the start, both must yield the same
// keys and bitmaps. TestFailedSeekLeavesThePosition covers where each cursor
// is left afterwards, which this does not.
func TestCursorNoCopyMatchesCopyingCursor(t *testing.T) {
	t.Parallel()

	sizes := []int{0, 1, 2, 3, 7, 8, 100, 1000}

	for _, n := range sizes {
		t.Run(fmt.Sprintf("keys=%d", n), func(t *testing.T) {
			t.Parallel()

			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("key_%05d", i)
			}
			// insert out of order so the tree actually has to rebalance
			rnd := rand.New(rand.NewSource(int64(n)))
			rnd.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
			bst := treeWith(t, keys)

			want := drain(NewBinarySearchTreeCursor(bst))
			got := drain(NewBinarySearchTreeCursorNoCopy(bst))
			require.Equal(t, want, got, "the no-copy cursor must yield exactly what the copying one does")
			require.Len(t, got, n)
		})
	}
}

// TestFailedSeekLeavesThePosition pins that a Seek finding nothing changes
// nothing: whatever Next would have returned before it, Next returns after
// too. Runs both cursors, which reach that guarantee differently — the
// copying one leaves its index alone, the no-copy one assigns its node only
// once the descent finds something.
func TestFailedSeekLeavesThePosition(t *testing.T) {
	t.Parallel()

	cursors := []struct {
		name string
		open func(*BinarySearchTree) InnerCursor
	}{
		{"copying", func(bst *BinarySearchTree) InnerCursor { return NewBinarySearchTreeCursor(bst) }},
		{"no-copy", func(bst *BinarySearchTree) InnerCursor { return NewBinarySearchTreeCursorNoCopy(bst) }},
	}

	tests := []struct {
		name string
		// where the cursor is put before the failed seek
		position func(t *testing.T, c InnerCursor)
		wantNext string // "" for a cursor with nothing left
	}{
		{
			name:     "never read from",
			position: func(t *testing.T, c InnerCursor) {},
			wantNext: "b",
		},
		{
			name: "part way through",
			position: func(t *testing.T, c InnerCursor) {
				k, _, err := c.First()
				require.NoError(t, err)
				require.Equal(t, "b", string(k))
			},
			wantNext: "d",
		},
		{
			name: "seeked to a key it holds",
			position: func(t *testing.T, c InnerCursor) {
				k, _, err := c.Seek([]byte("e"))
				require.NoError(t, err)
				require.Equal(t, "f", string(k))
			},
			wantNext: "h",
		},
		{
			name: "already exhausted",
			position: func(t *testing.T, c InnerCursor) {
				require.Len(t, drain(c), 4)
			},
		},
	}

	for _, cursor := range cursors {
		t.Run(cursor.name, func(t *testing.T) {
			t.Parallel()

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					c := cursor.open(treeWith(t, []string{"b", "d", "f", "h"}))
					tt.position(t, c)

					_, _, err := c.Seek([]byte("z"))
					require.ErrorIs(t, err, lsmkv.NotFound)

					k, _, err := c.Next()
					require.NoError(t, err)
					require.Equal(t, tt.wantNext, string(k))
				})
			}
		})
	}
}

// TestCursorNoCopySeek covers seeking, which descends the tree rather than
// scanning a flattened slice.
func TestCursorNoCopySeek(t *testing.T) {
	t.Parallel()

	bst := treeWith(t, []string{"b", "d", "f", "h"})

	tests := []struct {
		name string
		// keys, when set, replaces the shared tree; the empty case needs a tree
		// with no root, which is a different tree rather than a different seek.
		keys     []string
		seek     string
		wantKey  string
		wantErr  error
		thenNext string
	}{
		{name: "exact match", seek: "d", wantKey: "d", thenNext: "f"},
		{name: "between keys", seek: "c", wantKey: "d", thenNext: "f"},
		{name: "before the first", seek: "a", wantKey: "b", thenNext: "d"},
		{name: "the last key", seek: "h", wantKey: "h"},
		{name: "past the end", seek: "z", wantErr: lsmkv.NotFound},
		// Reaches the same arm as "past the end" by never entering the descent
		// rather than by falling off it.
		{name: "empty tree", keys: []string{}, seek: "a", wantErr: lsmkv.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tree := bst
			if tt.keys != nil {
				tree = treeWith(t, tt.keys)
			}
			c := NewBinarySearchTreeCursorNoCopy(tree)
			k, _, err := c.Seek([]byte(tt.seek))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantKey, string(k))

			if tt.thenNext != "" {
				next, _, err := c.Next()
				require.NoError(t, err)
				require.Equal(t, tt.thenNext, string(next), "Next must continue from the seek")
			}
		})
	}
}

// TestCursorNoCopyExhaustionIsStable pins that reading past the end keeps
// reporting nothing rather than restarting or panicking.
func TestCursorNoCopyExhaustionIsStable(t *testing.T) {
	t.Parallel()

	c := NewBinarySearchTreeCursorNoCopy(treeWith(t, []string{"a", "b"}))
	require.Len(t, drain(c), 2)

	for i := 0; i < 3; i++ {
		k, _, err := c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
	}
}

// TestCursorNoCopyWalksARebalancedTree covers the tree this cursor is the first
// to depend on the shape of. It descends by following each node's parent
// pointer, where the copying cursor flattens the tree first and never reads
// them; a rotation that failed to re-point a parent would show up here as a
// skipped or repeated key and nowhere else.
//
// The other cursor tests use a handful of keys, which is too few to rotate.
func TestCursorNoCopyWalksARebalancedTree(t *testing.T) {
	t.Parallel()

	const nKeys = 500

	// Bounded, because a rotation that left a parent dangling makes successor()
	// cycle: an unbounded walk would hang instead of failing.
	collect := func(t *testing.T, c *BinarySearchTreeCursorNoCopy,
		start func() ([]byte, BitmapLayer, error),
	) []string {
		t.Helper()
		out := make([]string, 0, nKeys)
		for k, _, err := start(); k != nil && err == nil; k, _, err = c.Next() {
			out = append(out, string(k))
			require.LessOrEqualf(t, len(out), nKeys,
				"walked past %d keys without ending: a parent pointer cycles", nKeys)
		}
		return out
	}

	// Rotations depend on insertion order, and ascending is what a sorted bulk
	// load produces.
	orders := map[string]func(i int) int{
		"ascending":  func(i int) int { return i },
		"descending": func(i int) int { return nKeys - 1 - i },
		"shuffled":   func(i int) int { return (i*137 + 41) % nKeys },
	}

	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			keys := make([]string, nKeys)
			for i := range keys {
				keys[i] = fmt.Sprintf("k%04d", order(i))
			}
			bst := treeWith(t, keys)

			want := make([]string, nKeys)
			for i := range want {
				want[i] = fmt.Sprintf("k%04d", i)
			}

			c := NewBinarySearchTreeCursorNoCopy(bst)
			require.Equal(t, want, collect(t, c, c.First),
				"the walk must yield every key once, in order")

			// And from any starting point the rest of the walk is the tail from
			// there, which is what the parent pointers decide after a descent.
			for _, at := range []int{0, 1, nKeys / 3, nKeys / 2, nKeys - 2, nKeys - 1} {
				c := NewBinarySearchTreeCursorNoCopy(bst)
				rest := collect(t, c, func() ([]byte, BitmapLayer, error) {
					return c.Seek([]byte(want[at]))
				})
				require.Equalf(t, want[at:], rest, "the walk from %q must be the tail from there", want[at])
			}
		})
	}
}
