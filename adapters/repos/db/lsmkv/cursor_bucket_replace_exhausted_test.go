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

package lsmkv

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// sliceInnerCursorReplace serves sorted keys and reports exhaustion with
// notFound, which a test can set to a wrapped sentinel.
type sliceInnerCursorReplace struct {
	keys       [][]byte
	tombstones map[string]struct{}
	notFound   error
	pos        int
}

func (c *sliceInnerCursorReplace) current() ([]byte, []byte, error) {
	if c.pos >= len(c.keys) {
		return nil, nil, c.notFound
	}
	key := c.keys[c.pos]
	if _, ok := c.tombstones[string(key)]; ok {
		return key, nil, lsmkv.Deleted
	}
	return key, key, nil
}

func (c *sliceInnerCursorReplace) first() ([]byte, []byte, error) {
	c.pos = 0
	return c.current()
}

func (c *sliceInnerCursorReplace) next() ([]byte, []byte, error) {
	c.pos++
	return c.current()
}

func (c *sliceInnerCursorReplace) seek(target []byte) ([]byte, []byte, error) {
	c.pos = sort.Search(len(c.keys), func(i int) bool {
		return bytes.Compare(c.keys[i], target) >= 0
	})
	return c.current()
}

// An exhausted inner cursor must never be picked as the lowest key, no matter
// how it spells NotFound: picking it serves a nil key, which ends the scan
// while other inner cursors still hold data.
func TestCursorReplace_ExhaustedInnerCursors(t *testing.T) {
	notFoundErrs := map[string]error{
		"bare sentinel":    lsmkv.NotFound,
		"wrapped sentinel": fmt.Errorf("segment 3: %w", lsmkv.NotFound),
	}

	type test struct {
		name       string
		inner      [][]string // oldest to newest
		tombstones []map[string]struct{}
		seek       string // empty means First()
		want       []string
	}

	tests := []test{
		{
			name:  "short cursor exhausts on next, long one continues",
			inner: [][]string{{"a"}, {"b", "c", "d"}},
			want:  []string{"a", "b", "c", "d"},
		},
		{
			name:  "empty cursor exhausts on first",
			inner: [][]string{{}, {"a", "b"}, {}},
			want:  []string{"a", "b"},
		},
		{
			name:  "cursor exhausts on seek",
			inner: [][]string{{"a", "b"}, {"c", "d", "e"}},
			seek:  "c",
			want:  []string{"c", "d", "e"},
		},
		{
			name:  "all cursors exhausted",
			inner: [][]string{{}, {}},
			want:  nil,
		},
		{
			name:  "seek past every key",
			inner: [][]string{{"a"}, {"b"}},
			seek:  "z",
			want:  nil,
		},
		{
			name:       "tombstone next to exhausted cursor is skipped, scan continues",
			inner:      [][]string{{"a"}, {"b", "c"}},
			tombstones: []map[string]struct{}{nil, {"b": {}}},
			want:       []string{"a", "c"},
		},
		{
			name:       "trailing tombstone ends the scan cleanly",
			inner:      [][]string{{"a"}, {"b"}},
			tombstones: []map[string]struct{}{nil, {"b": {}}},
			want:       []string{"a"},
		},
		{
			name:  "duplicate key across cursors, older one exhausts first",
			inner: [][]string{{"a"}, {"a", "b"}},
			want:  []string{"a", "b"},
		},
	}

	for errName, notFound := range notFoundErrs {
		for _, tt := range tests {
			t.Run(errName+"/"+tt.name, func(t *testing.T) {
				inner := make([]innerCursorReplace, len(tt.inner))
				for i, keys := range tt.inner {
					cur := &sliceInnerCursorReplace{notFound: notFound}
					for _, k := range keys {
						cur.keys = append(cur.keys, []byte(k))
					}
					if tt.tombstones != nil {
						cur.tombstones = tt.tombstones[i]
					}
					inner[i] = cur
				}
				c := &CursorReplace{innerCursors: inner, unlock: func() {}}
				defer c.Close()

				var got []string
				var k []byte
				if tt.seek == "" {
					k, _ = c.First()
				} else {
					k, _ = c.Seek([]byte(tt.seek))
				}
				for ; k != nil; k, _ = c.Next() {
					got = append(got, string(k))
					require.LessOrEqual(t, len(got), 16, "cursor does not terminate")
				}
				assert.Equal(t, tt.want, got)

				k, v := c.Next()
				assert.Nil(t, k, "Next() after the end")
				assert.Nil(t, v, "Next() after the end")
			})
		}
	}
}
