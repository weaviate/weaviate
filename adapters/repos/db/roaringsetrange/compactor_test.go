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

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func Test_Compactor(t *testing.T) {
	type test struct {
		name            string
		left            []byte
		right           []byte
		expectedKeep    []segmentEntry
		expectedCleanup []segmentEntry
		expectedErr     string
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
			expectedKeep: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33, 55, 66},
					deletions: []uint64{111, 444},
				},
				{
					key:       uint8(1),
					additions: []uint64{22, 55},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: nil,
				},
				{
					key:       uint8(3),
					additions: []uint64{66},
					deletions: nil,
				},
			},
			expectedCleanup: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33, 55, 66},
					deletions: []uint64{},
				},
				{
					key:       uint8(1),
					additions: []uint64{22, 55},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: nil,
				},
				{
					key:       uint8(3),
					additions: []uint64{66},
					deletions: nil,
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
			expectedKeep: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{22, 33, 44, 55},
					deletions: []uint64{11, 22, 33, 44, 111, 666},
				},
				{
					key:       uint8(1),
					additions: []uint64{55},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{22},
					deletions: nil,
				},
				{
					key:       uint8(3),
					additions: []uint64{33},
					deletions: nil,
				},
				{
					key:       uint8(4),
					additions: []uint64{44},
					deletions: nil,
				},
			},
			expectedCleanup: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{22, 33, 44, 55},
					deletions: []uint64{},
				},
				{
					key:       uint8(1),
					additions: []uint64{55},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{22},
					deletions: nil,
				},
				{
					key:       uint8(3),
					additions: []uint64{33},
					deletions: nil,
				},
				{
					key:       uint8(4),
					additions: []uint64{44},
					deletions: nil,
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
			expectedKeep: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{},
					deletions: []uint64{11, 22, 33, 44, 111},
				},
			},
			expectedCleanup: []segmentEntry{},
		},
		{
			name:            "empty both segments",
			left:            []byte{},
			right:           []byte{},
			expectedKeep:    []segmentEntry{},
			expectedCleanup: []segmentEntry{},
		},
		{
			name: "empty right segment",
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
			right: []byte{},
			expectedKeep: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33},
					deletions: []uint64{111},
				},
				{
					key:       uint8(1),
					additions: []uint64{22},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: nil,
				},
			},
			expectedCleanup: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33},
					deletions: []uint64{},
				},
				{
					key:       uint8(1),
					additions: []uint64{22},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: nil,
				},
			},
		},
		{
			name: "empty left segment",
			left: []byte{},
			right: createSegmentsFromEntries(t, []segmentEntry{
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
			expectedKeep: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33},
					deletions: []uint64{111},
				},
				{
					key:       uint8(1),
					additions: []uint64{22},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: nil,
				},
			},
			expectedCleanup: []segmentEntry{
				{
					key:       uint8(0),
					additions: []uint64{11, 22, 33},
					deletions: []uint64{},
				},
				{
					key:       uint8(1),
					additions: []uint64{22},
					deletions: nil,
				},
				{
					key:       uint8(2),
					additions: []uint64{33},
					deletions: nil,
				},
			},
		},
		{
			name: "invalid left segment",
			left: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(1),
					additions: []uint64{12345},
					deletions: []uint64{},
				},
			}),
			right: createSegmentsFromEntries(t, []segmentEntry{
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
			expectedErr: "left segment: missing key 0 (non-null bitmap)",
		},
		{
			name: "invalid right segment",
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
					key:       uint8(1),
					additions: []uint64{12345},
					deletions: []uint64{},
				},
			}),
			expectedErr: "right segment: missing key 0 (non-null bitmap)",
		},
	}

	for _, test := range tests {
		for _, checkSum := range []bool{true, false} {
			maxNewFileSize := int64(len(test.left)+len(test.right)) + segmentindex.HeaderSize
			if checkSum {
				maxNewFileSize += 8 // for checksum
			}

			t.Run("[keep] "+test.name, func(t *testing.T) {
				leftCursor := NewSegmentCursorMmap(test.left)
				rightCursor := NewSegmentCursorMmap(test.right)

				bytesInMemory, _ := cursorCompactor(t, leftCursor, rightCursor, maxNewFileSize, false, checkSum)
				bytesWriter, err := cursorCompactor(t, leftCursor, rightCursor, compactor.SegmentWriterBufferSize+1, false, checkSum)

				if test.expectedErr == "" {
					require.NoError(t, err)
					require.Equal(t, bytesInMemory, bytesWriter)

					header, err := segmentindex.ParseHeader(bytesInMemory[:segmentindex.HeaderSize])
					require.NoError(t, err)

					cu := NewSegmentCursorMmap(bytesInMemory[segmentindex.HeaderSize:header.IndexStart])

					i := 0
					for k, l, ok := cu.First(); ok; k, l, ok = cu.Next() {
						assert.Equal(t, test.expectedKeep[i].key, k)
						assert.Equal(t, test.expectedKeep[i].additions, l.Additions.ToArray())
						assert.Equal(t, test.expectedKeep[i].deletions, l.Deletions.ToArray())
						i++
					}

					assert.Equal(t, len(test.expectedKeep), i, "all expected keys must have been hit")
				} else {
					assert.ErrorContains(t, err, test.expectedErr)
				}
			})
		}
	}

	for _, test := range tests {
		for _, checkSum := range []bool{true, false} {
			maxNewFileSize := int64(len(test.left)+len(test.right)) + segmentindex.HeaderSize
			if checkSum {
				maxNewFileSize += 8 // for checksum
			}

			t.Run("[cleanup] "+test.name, func(t *testing.T) {
				leftCursor := NewSegmentCursorMmap(test.left)
				rightCursor := NewSegmentCursorMmap(test.right)

				bytesInMemory, _ := cursorCompactor(t, leftCursor, rightCursor, maxNewFileSize, true, checkSum)
				bytesWriter, err := cursorCompactor(t, leftCursor, rightCursor, compactor.SegmentWriterBufferSize+1, true, checkSum)

				if test.expectedErr == "" {
					require.NoError(t, err)
					require.Equal(t, bytesInMemory, bytesWriter)

					header, err := segmentindex.ParseHeader(bytesInMemory[:segmentindex.HeaderSize])
					require.NoError(t, err)

					cu := NewSegmentCursorMmap(bytesInMemory[segmentindex.HeaderSize:header.IndexStart])

					i := 0
					for k, l, ok := cu.First(); ok; k, l, ok = cu.Next() {
						assert.Equal(t, test.expectedCleanup[i].key, k)
						assert.Equal(t, test.expectedCleanup[i].additions, l.Additions.ToArray())
						assert.Equal(t, test.expectedCleanup[i].deletions, l.Deletions.ToArray())
						i++
					}

					assert.Equal(t, len(test.expectedCleanup), i, "all expected keys must have been hit")
				} else {
					assert.ErrorContains(t, err, test.expectedErr)
				}
			})
		}
	}
}

func cursorCompactor(t *testing.T, leftCursor, rightCursor SegmentCursor, maxNewFileSize int64, cleanup, checkSum bool) ([]byte, error) {
	t.Helper()
	dir := t.TempDir()

	segmentFile := filepath.Join(dir, fmt.Sprintf("result-%v-%v-%v.db", cleanup, checkSum, maxNewFileSize))
	f, err := os.Create(segmentFile)
	require.NoError(t, err)

	c := NewCompactor(f, leftCursor, rightCursor, 5, cleanup, checkSum, maxNewFileSize)
	if err := c.Do(); err != nil {
		require.NoError(t, f.Close())
		return nil, err
	}

	require.NoError(t, f.Close())

	f, err = os.Open(segmentFile)
	require.NoError(t, err)

	segmentBytes, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return segmentBytes, nil
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
