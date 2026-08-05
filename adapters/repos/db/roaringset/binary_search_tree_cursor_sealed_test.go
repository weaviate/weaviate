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

// TestSealedCursorMatchesCopyingCursor pins the fast cursor against the copying
// one it shortcuts. The two differ in whether they copy and condense, never in
// what they yield, and only this equivalence makes the fast one usable.
func TestSealedCursorMatchesCopyingCursor(t *testing.T) {
	sizes := []int{0, 1, 2, 3, 7, 8, 100, 1000}

	for _, n := range sizes {
		t.Run(fmt.Sprintf("keys=%d", n), func(t *testing.T) {
			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("key_%05d", i)
			}
			// insert out of order so the tree actually has to rebalance
			rnd := rand.New(rand.NewSource(int64(n)))
			rnd.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
			bst := treeWith(t, keys)

			want := drain(NewBinarySearchTreeCursor(bst))
			got := drain(NewSealedBinarySearchTreeCursor(bst))
			require.Equal(t, want, got, "sealed cursor must yield exactly what the copying one does")
			require.Len(t, got, n)
		})
	}
}

// TestSealedCursorSeek covers seeking, which descends the tree rather than
// scanning a flattened slice.
func TestSealedCursorSeek(t *testing.T) {
	bst := treeWith(t, []string{"b", "d", "f", "h"})

	tests := []struct {
		name     string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewSealedBinarySearchTreeCursor(bst)
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

// TestSealedCursorExhaustionIsStable pins that reading past the end keeps
// reporting nothing rather than restarting or panicking.
func TestSealedCursorExhaustionIsStable(t *testing.T) {
	c := NewSealedBinarySearchTreeCursor(treeWith(t, []string{"a", "b"}))
	require.Len(t, drain(c), 2)

	for i := 0; i < 3; i++ {
		k, _, err := c.Next()
		require.NoError(t, err)
		require.Nil(t, k)
	}
}
