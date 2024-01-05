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

package roaringset

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func Test_Compactor(t *testing.T) {
	type test struct {
		name         string
		left         []byte
		right        []byte
		expected     []keyWithBML
		expectedRoot []keyWithBML
	}

	tests := []test{
		{
			name: "independent segments without overlap",
			left: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
				{
					key:       []byte("ccc"),
					additions: []uint64{4},
					deletions: []uint64{5},
				},
			}),
			right: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
				{
					key:       []byte("ddd"),
					additions: []uint64{6},
					deletions: []uint64{7},
				},
			}),
			expected: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
				{
					key:       []byte("ccc"),
					additions: []uint64{4},
					deletions: []uint64{5},
				},
				{
					key:       []byte("ddd"),
					additions: []uint64{6},
					deletions: []uint64{7},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
				},
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
				},
				{
					key:       []byte("ccc"),
					additions: []uint64{4},
				},
				{
					key:       []byte("ddd"),
					additions: []uint64{6},
				},
			},
		},
		{
			name: "some segments overlap",
			// note: there is no need to test every possible edge case for the
			// overlapping segments in this place, as this logic is outsourced to
			// BitmapLayer.Merge() which already has tests for edge cases
			left: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
				{
					key:       []byte("overlap"),
					additions: []uint64{4, 5, 6},
					deletions: []uint64{1, 3, 7},
				},
			}),
			right: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("overlap"),
					additions: []uint64{3, 8},
					deletions: []uint64{5},
				},
				{
					key:       []byte("zzz"),
					additions: []uint64{6},
					deletions: []uint64{7},
				},
			}),
			expected: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
				{
					key:       []byte("overlap"),
					additions: []uint64{3, 4, 6, 8},
					deletions: []uint64{1, 5, 7},
				},
				{
					key:       []byte("zzz"),
					additions: []uint64{6},
					deletions: []uint64{7},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
				},
				{
					key:       []byte("overlap"),
					additions: []uint64{3, 4, 6, 8},
				},
				{
					key:       []byte("zzz"),
					additions: []uint64{6},
				},
			},
		},
		{
			name: "everything but one is deleted",
			left: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{},
				},
				{
					key:       []byte("bbb"),
					additions: []uint64{4, 5, 6},
					deletions: []uint64{},
				},
				{
					key:       []byte("ddd"),
					additions: []uint64{11, 12, 111},
					deletions: []uint64{},
				},
			}),
			right: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{},
					deletions: []uint64{0},
				},
				{
					key:       []byte("bbb"),
					additions: []uint64{},
					deletions: []uint64{4, 5, 6},
				},
				{
					key:       []byte("ccc"),
					additions: []uint64{},
					deletions: []uint64{7, 8},
				},
				{
					key:       []byte("ddd"),
					additions: []uint64{222},
					deletions: []uint64{11, 12, 13, 14},
				},
			}),
			expected: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{},
					deletions: []uint64{0},
				},
				{
					key:       []byte("bbb"),
					additions: []uint64{},
					deletions: []uint64{4, 5, 6},
				},
				{
					key:       []byte("ccc"),
					additions: []uint64{},
					deletions: []uint64{7, 8},
				},
				{
					key:       []byte("ddd"),
					additions: []uint64{111, 222},
					deletions: []uint64{11, 12, 13, 14},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("ddd"),
					additions: []uint64{111, 222},
				},
			},
		},

		// the key loop is essentially a state machine. The next tests try to cover
		// all possible states:
		//
		// 1. only the left key is set -> take left key
		// 2. both left key and right key are set, but left is smaller -> take left
		//    key
		// 3. only the right key is set -> take right key
		// 4. both right and left keys are set, but right key is smaller -> take
		//    the right key
		// 5. both keys are identical -> merge them
		//
		// Note: There is also an implicit 6th case: both keys are not set, this is
		// the exit condition which is part of every test.
		{
			name: "state 1 - only left key is set",
			left: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
			}),
			right: createSegmentsFromKeys(t, []keyWithBML{}),
			expected: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
				},
			},
		},
		{
			name: "state 2 - left+right, left is smaller",
			left: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
			}),
			right: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
			}),
			expected: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
				},
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
				},
			},
		},
		{
			name: "state 3 - only the right key is set",
			left: createSegmentsFromKeys(t, []keyWithBML{}),
			right: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
			}),
			expected: []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
				},
			},
		},
		{
			name: "state 4 - left+right, right is smaller",
			left: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("ccc"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
			}),
			right: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
			}),
			expected: []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
				{
					key:       []byte("ccc"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("bbb"),
					additions: []uint64{2},
				},
				{
					key:       []byte("ccc"),
					additions: []uint64{0},
				},
			},
		},
		{
			name: "state 5 - left+right are identical",
			left: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0},
					deletions: []uint64{1},
				},
			}),
			right: createSegmentsFromKeys(t, []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{2},
					deletions: []uint64{3},
				},
			}),
			expected: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0, 2},
					deletions: []uint64{1, 3},
				},
			},
			expectedRoot: []keyWithBML{
				{
					key:       []byte("aaa"),
					additions: []uint64{0, 2},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run("[keep]"+test.name, func(t *testing.T) {
			dir := t.TempDir()

			leftCursor := NewSegmentCursor(test.left, nil)
			rightCursor := NewSegmentCursor(test.right, nil)

			segmentFile := filepath.Join(dir, "result.db")
			f, err := os.Create(segmentFile)
			require.NoError(t, err)

			c := NewCompactor(f, leftCursor, rightCursor, 5, dir+"/scratch", false)
			require.NoError(t, c.Do())

			require.NoError(t, f.Close())

			f, err = os.Open(segmentFile)
			require.NoError(t, err)

			header, err := segmentindex.ParseHeader(f)
			require.NoError(t, err)

			segmentBytes, err := io.ReadAll(f)
			require.NoError(t, err)

			require.NoError(t, f.Close())

			cu := NewSegmentCursor(segmentBytes[:header.IndexStart-segmentindex.HeaderSize], nil)

			i := 0
			for k, v, _ := cu.First(); k != nil; k, v, _ = cu.Next() {
				assert.Equal(t, test.expected[i].key, k)
				assert.Equal(t, test.expected[i].additions, v.Additions.ToArray())
				assert.Equal(t, test.expected[i].deletions, v.Deletions.ToArray())
				i++
			}

			assert.Equal(t, len(test.expected), i, "all expected keys must have been hit")
		})
	}

	for _, test := range tests {
		t.Run("[cleanup] "+test.name, func(t *testing.T) {
			dir := t.TempDir()

			leftCursor := NewSegmentCursor(test.left, nil)
			rightCursor := NewSegmentCursor(test.right, nil)

			segmentFile := filepath.Join(dir, "result.db")
			f, err := os.Create(segmentFile)
			require.NoError(t, err)

			c := NewCompactor(f, leftCursor, rightCursor, 5, dir+"/scratch", true)
			require.NoError(t, c.Do())

			require.NoError(t, f.Close())

			f, err = os.Open(segmentFile)
			require.NoError(t, err)

			header, err := segmentindex.ParseHeader(f)
			require.NoError(t, err)

			segmentBytes, err := io.ReadAll(f)
			require.NoError(t, err)

			require.NoError(t, f.Close())

			cu := NewSegmentCursor(segmentBytes[:header.IndexStart-segmentindex.HeaderSize], nil)

			i := 0
			for k, v, _ := cu.First(); k != nil; k, v, _ = cu.Next() {
				assert.Equal(t, test.expectedRoot[i].key, k)
				assert.Equal(t, test.expectedRoot[i].additions, v.Additions.ToArray())
				assert.Empty(t, v.Deletions.ToArray())
				i++
			}

			assert.Equal(t, len(test.expectedRoot), i, "all expected keys must have been hit")
		})
	}
}

type keyWithBML struct {
	key       []byte
	additions []uint64
	deletions []uint64
}

func createSegmentsFromKeys(t *testing.T, keys []keyWithBML) []byte {
	out := []byte{}

	for _, k := range keys {
		add := NewBitmap(k.additions...)
		del := NewBitmap(k.deletions...)
		sn, err := NewSegmentNode(k.key, add, del)
		require.Nil(t, err)
		out = append(out, sn.ToBuffer()...)
	}

	return out
}
