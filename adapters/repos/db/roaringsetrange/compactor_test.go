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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func Test_Compactor(t *testing.T) {
	type test struct {
		name         string
		left         []byte
		right        []byte
		expected     []segmentEntry
		expectedRoot []segmentEntry
	}

	tests := []test{
		{
			name: "segments with nothing deleted",
			left: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33},
					deletions: []uint64{111},
				},
				{
					key:       uint8(1),
					additions: []uint64{22},
					deletions: []uint64{222}, // ignored
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: []uint64{333}, // ignored
				},
			}),
			right: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{55, 66},
					deletions: []uint64{444},
				},
				{
					key:       uint8(1),
					additions: []uint64{55},
					deletions: []uint64{555}, // ignored
				},
				{
					key:       uint8(3),
					additions: []uint64{66},
					deletions: []uint64{666}, // ignored
				},
			}),
			expected: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33, 55, 66},
					deletions: []uint64{111, 444},
				},
				{
					key:       uint8(1),
					additions: []uint64{22, 55},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{66},
					deletions: []uint64{},
				},
			},
			expectedRoot: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33, 55, 66},
					deletions: []uint64{},
				},
				{
					key:       uint8(1),
					additions: []uint64{22, 55},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{66},
					deletions: []uint64{},
				},
			},
		},
		{
			name: "segments with everything overwritten",
			left: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33, 44},
					deletions: []uint64{111},
				},
				{
					key:       uint8(1),
					additions: []uint64{22},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{44},
					deletions: []uint64{},
				},
			}),
			right: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{22, 33, 44, 55},
					deletions: []uint64{11, 22, 33, 44, 666},
				},
				{
					key:       uint8(1),
					additions: []uint64{55},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{22},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{33},
					deletions: []uint64{},
				},
				{
					key:       uint8(4),
					additions: []uint64{44},
					deletions: []uint64{},
				},
			}),
			expected: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{22, 33, 44, 55},
					deletions: []uint64{11, 22, 33, 44, 111, 666},
				},
				{
					key:       uint8(1),
					additions: []uint64{55},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{22},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{33},
					deletions: []uint64{},
				},
				{
					key:       uint8(4),
					additions: []uint64{44},
					deletions: []uint64{},
				},
			},
			expectedRoot: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{22, 33, 44, 55},
					deletions: []uint64{},
				},
				{
					key:       uint8(1),
					additions: []uint64{55},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{22},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{33},
					deletions: []uint64{},
				},
				{
					key:       uint8(4),
					additions: []uint64{44},
					deletions: []uint64{},
				},
			},
		},
		{
			name: "segments with everything deleted",
			left: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33, 44},
					deletions: []uint64{111},
				},
				{
					key:       uint8(1),
					additions: []uint64{22},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{44},
					deletions: []uint64{},
				},
			}),
			right: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{},
					deletions: []uint64{11, 22, 33, 44},
				},
			}),
			expected: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{},
					deletions: []uint64{11, 22, 33, 44, 111},
				},
				{
					key:       uint8(1),
					additions: []uint64{},
					deletions: []uint64{},
				},
				{
					key:       uint8(2),
					additions: []uint64{},
					deletions: []uint64{},
				},
				{
					key:       uint8(3),
					additions: []uint64{},
					deletions: []uint64{},
				},
			},
			expectedRoot: []segmentEntry{},
		},
	}

	for _, test := range tests {
		t.Run("[keep] "+test.name, func(t *testing.T) {
			dir := t.TempDir()

			leftCursor := NewSegmentCursor(test.left)
			rightCursor := NewSegmentCursor(test.right)

			segmentFile := filepath.Join(dir, "result.db")
			f, err := os.Create(segmentFile)
			require.NoError(t, err)

			c := NewCompactor(f, leftCursor, rightCursor, 5, false)
			require.NoError(t, c.Do())

			require.NoError(t, f.Close())

			f, err = os.Open(segmentFile)
			require.NoError(t, err)

			header, err := segmentindex.ParseHeader(f)
			require.NoError(t, err)

			segmentBytes, err := io.ReadAll(f)
			require.NoError(t, err)

			require.NoError(t, f.Close())

			cu := NewSegmentCursor(segmentBytes[:header.IndexStart-segmentindex.HeaderSize])

			i := 0
			for k, l, ok := cu.First(); ok; k, l, ok = cu.Next() {
				fmt.Printf("  ==> i %d\n", i)
				fmt.Printf("  ==> k %d, add %v del %v\n\n", k, l.Additions.ToArray(), l.Deletions.ToArray())

				// assert.Equal(t, test.expected[i].key, k)
				// assert.Equal(t, test.expected[i].additions, l.Additions.ToArray())
				// assert.Equal(t, test.expected[i].deletions, l.Deletions.ToArray())
				i++
			}

			assert.Equal(t, len(test.expected), i, "all expected keys must have been hit")
		})
	}

	for _, test := range tests {
		t.Run("[cleanup] "+test.name, func(t *testing.T) {
			dir := t.TempDir()

			leftCursor := NewSegmentCursor(test.left)
			rightCursor := NewSegmentCursor(test.right)

			segmentFile := filepath.Join(dir, "result.db")
			f, err := os.Create(segmentFile)
			require.NoError(t, err)

			c := NewCompactor(f, leftCursor, rightCursor, 5, true)
			require.NoError(t, c.Do())

			require.NoError(t, f.Close())

			f, err = os.Open(segmentFile)
			require.NoError(t, err)

			header, err := segmentindex.ParseHeader(f)
			require.NoError(t, err)

			segmentBytes, err := io.ReadAll(f)
			require.NoError(t, err)

			require.NoError(t, f.Close())

			cu := NewSegmentCursor(segmentBytes[:header.IndexStart-segmentindex.HeaderSize])

			i := 0
			for k, l, ok := cu.First(); ok; k, l, ok = cu.Next() {
				assert.Equal(t, test.expectedRoot[i].key, k)
				assert.Equal(t, test.expectedRoot[i].additions, l.Additions.ToArray())
				assert.Empty(t, l.Deletions.ToArray())
				i++
			}

			assert.Equal(t, len(test.expectedRoot), i, "all expected keys must have been hit")
		})
	}
}

type segmentEntry struct {
	key       uint8
	additions []uint64
	deletions []uint64
}

func createSegmentsFromEntries(t *testing.T, entries []segmentEntry) []byte {
	out := []byte{}

	for _, entry := range entries {
		add := roaringset.NewBitmap(entry.additions...)
		del := roaringset.NewBitmap(entry.deletions...)
		sn, err := NewSegmentNode(entry.key, add, del)
		require.Nil(t, err)
		out = append(out, sn.ToBuffer()...)
	}

	return out
}
