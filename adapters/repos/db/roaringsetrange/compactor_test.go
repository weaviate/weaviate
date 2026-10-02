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
	"context"
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
	"github.com/weaviate/weaviate/usecases/byteops"
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
			// The merge leaves a bitmap container holding ten values, which
			// Compacted rewrites as an array and a plain copy would not.
			name: "a key whose additions are almost all deleted",
			left: createSegmentsFromEntries(t, []segmentEntry{
				{
					key: uint8(0),
					// This side holds more values than an array container does,
					// so the merge leaves a bitmap container behind whatever
					// survives it.
					additions: valuesFrom(0, 5000),
				},
				{
					key:       uint8(1),
					additions: valuesFrom(0, 5000),
				},
			}),
			right: createSegmentsFromEntries(t, []segmentEntry{
				{
					key:       uint8(0),
					deletions: valuesFrom(0, 4990),
				},
			}),
			expectedKeep: []segmentEntry{
				{
					key:       uint8(0),
					additions: valuesFrom(4990, 5000),
					deletions: valuesFrom(0, 4990),
				},
				{
					key:       uint8(1),
					additions: valuesFrom(4990, 5000),
					deletions: nil,
				},
			},
			expectedCleanup: []segmentEntry{
				{
					key:       uint8(0),
					additions: valuesFrom(4990, 5000),
					deletions: []uint64{},
				},
				{
					key:       uint8(1),
					additions: valuesFrom(4990, 5000),
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
					assertNodesCompacted(t, bytesInMemory[segmentindex.HeaderSize:header.IndexStart],
						len(test.expectedKeep))
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
					assertNodesCompacted(t, bytesInMemory[segmentindex.HeaderSize:header.IndexStart],
						len(test.expectedCleanup))
				} else {
					assert.ErrorContains(t, err, test.expectedErr)
				}
			})
		}
	}
}

// assertNodesCompacted checks that every node's payload is in the form Compacted
// writes, that the nodes tile the body, and that there are expectedNodes of them.
// A plain copy and Compacted decode alike, so only a row whose merge leaves few
// enough values to become an array container can fail the first check.
func assertNodesCompacted(t *testing.T, body []byte, expectedNodes int) {
	t.Helper()

	offset, nodes := 0, 0
	for offset+8 <= len(body) {
		node := NewSegmentNodeFromBuffer(body[offset:])

		// The reads below are bounded by the node's own length indicators, which
		// nothing checks against the buffer, so the node must be known to fit the
		// body first.
		require.GreaterOrEqual(t, node.Len(), uint64(AdditionsStart),
			"node %d declares less than its own fixed-width header", nodes)
		require.LessOrEqual(t, node.Len(), uint64(len(body)-offset),
			"node %d declares %d bytes with %d left in the body", nodes, node.Len(), len(body)-offset)

		// A node over-declaring its length still tiles the body, because
		// writeLayer writes exactly the byte count the node declares.
		require.Equal(t, lenFromIndicators(t, node), node.Len(),
			"node %d must declare the length its own indicators add up to", nodes)

		additions := node.Additions()
		require.Equal(t, additions.Compacted().ToBuffer(), additions.ToBuffer(),
			"additions of node %d", nodes)
		if deletions := node.Deletions(); deletions != nil {
			require.Equal(t, deletions.Compacted().ToBuffer(), deletions.ToBuffer(),
				"deletions of node %d", nodes)
		}
		offset += int(node.Len())
		nodes++
	}

	require.Equal(t, expectedNodes, nodes)
	require.Equal(t, len(body), offset,
		"the nodes must tile the body exactly, leaving no bytes that belong to none of them")
}

// lenFromIndicators adds up what the node's own length indicators say it holds, so a
// node whose total length exceeds its payloads does not agree with itself.
func lenFromIndicators(t *testing.T, node *SegmentNode) uint64 {
	t.Helper()

	rw := byteops.NewReadWriter(node.ToBuffer())
	rw.MoveBufferToAbsolutePosition(NodeLengthSize + KeySize)
	total := uint64(AdditionsStart) + rw.ReadUint64()
	if node.Key() == 0 {
		// ReadUint64 does not check the buffer holds the bytes, and the node keeps
		// the capacity of the body behind it, so a read past the declared length
		// takes the next node's bytes rather than failing.
		require.GreaterOrEqual(t, node.Len(), total+BitmapLengthSize,
			"node declares no room for its deletions length indicator")
		rw.MoveBufferToAbsolutePosition(total)
		total += BitmapLengthSize + rw.ReadUint64()
	}
	return total
}

func cursorCompactor(t *testing.T, leftCursor, rightCursor SegmentCursor, maxNewFileSize int64, cleanup, checkSum bool) ([]byte, error) {
	t.Helper()
	dir := t.TempDir()

	segmentFile := filepath.Join(dir, fmt.Sprintf("result-%v-%v-%v.db", cleanup, checkSum, maxNewFileSize))
	f, err := os.Create(segmentFile)
	require.NoError(t, err)

	c := NewCompactor(f, leftCursor, rightCursor, 5, cleanup, checkSum, maxNewFileSize)
	if err := c.Do(context.Background()); err != nil {
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
