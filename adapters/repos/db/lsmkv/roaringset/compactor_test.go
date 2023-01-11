package roaringset

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/sroar"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Compactor(t *testing.T) {
	type test struct {
		name     string
		left     []byte
		right    []byte
		expected []keyWithBML
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
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leftCursor := NewSegmentCursor(test.left, nil)
			rightCursor := NewSegmentCursor(test.right, nil)

			segmentFile := filepath.Join(os.TempDir(), "result.db")
			f, err := os.Create(segmentFile)
			require.Nil(t, err)

			c := NewCompactor(f, leftCursor, rightCursor, 5, t.TempDir())
			require.Nil(t, c.Do())

			require.Nil(t, f.Close())

			f, err = os.Open(segmentFile)
			require.Nil(t, err)

			header, err := segmentindex.ParseHeader(f)
			require.Nil(t, err)

			segmentBytes, err := io.ReadAll(f)
			require.Nil(t, err)

			require.Nil(t, f.Close())

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
}

type keyWithBML struct {
	key       []byte
	additions []uint64
	deletions []uint64
}

func createSegmentsFromKeys(t *testing.T, keys []keyWithBML) []byte {
	out := []byte{}

	for _, k := range keys {
		add := sroar.NewBitmap()
		add.SetMany(k.additions)
		del := sroar.NewBitmap()
		del.SetMany(k.deletions)
		sn, err := NewSegmentNode(k.key, add, del)
		require.Nil(t, err)
		out = append(out, sn.ToBuffer()...)
	}

	return out
}
