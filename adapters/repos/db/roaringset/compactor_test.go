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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"

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
		leftCursor := NewSegmentCursor(test.left, nil)
		rightCursor := NewSegmentCursor(test.right, nil)
		for _, checkSum := range []bool{true, false} {
			maxNewFileSize := int64(len(test.left)+len(test.right)) + segmentindex.HeaderSize
			if checkSum {
				maxNewFileSize += 8 // for checksum
			}
			t.Run("[keep]"+test.name+fmt.Sprintf("checksum: %v", checkSum), func(t *testing.T) {
				segmentBytesInMem := cursorCompactor(t, leftCursor, rightCursor, maxNewFileSize, false, checkSum)
				segmentBytesWriter := cursorCompactor(t, leftCursor, rightCursor, compactor.SegmentWriterBufferSize+1, false, checkSum)

				require.Equal(t, segmentBytesInMem, segmentBytesWriter)

				header, err := segmentindex.ParseHeader(segmentBytesInMem[:segmentindex.HeaderSize])
				require.NoError(t, err)

				cu := NewSegmentCursor(segmentBytesInMem[segmentindex.HeaderSize:header.IndexStart], nil)

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

	for _, test := range tests {
		for _, checkSum := range []bool{true, false} {
			t.Run("[cleanup] "+test.name, func(t *testing.T) {
				leftCursor := NewSegmentCursor(test.left, nil)
				rightCursor := NewSegmentCursor(test.right, nil)

				maxNewFileSize := int64(len(test.left)+len(test.right)) + segmentindex.HeaderSize
				if checkSum {
					maxNewFileSize += 8 // for checksum
				}

				segmentBytesInMem := cursorCompactor(t, leftCursor, rightCursor, maxNewFileSize, true, checkSum)
				segmentBytesWriter := cursorCompactor(t, leftCursor, rightCursor, compactor.SegmentWriterBufferSize+1, true, checkSum)
				require.Equal(t, segmentBytesInMem, segmentBytesWriter)

				header, err := segmentindex.ParseHeader(segmentBytesInMem[:segmentindex.HeaderSize])
				require.NoError(t, err)

				cu := NewSegmentCursor(segmentBytesInMem[segmentindex.HeaderSize:header.IndexStart], nil)

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
}

func cursorCompactor(t *testing.T, leftCursor, rightCursor SegmentCursor, maxNewFileSize int64, cleanup, checkSum bool) []byte {
	t.Helper()
	dir := t.TempDir()

	segmentFile := filepath.Join(dir, fmt.Sprintf("result-%v-%v-%v.db", cleanup, checkSum, maxNewFileSize))
	f, err := os.Create(segmentFile)
	require.NoError(t, err)

	c := NewCompactor(f, leftCursor, rightCursor, 5, cleanup, checkSum, maxNewFileSize)
	require.NoError(t, c.Do(context.Background()))

	require.NoError(t, f.Close())

	f, err = os.Open(segmentFile)
	require.NoError(t, err)

	segmentBytes, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return segmentBytes
}

func TestCompactor_InMemoryWritesEfficency(t *testing.T) {
	// The point of the in-memory write path is to prevent using up too many
	// Write syscalls for tiny segments/tiny compactions. This test proves that
	// this is actually the case.
	compactionSetup := func(inMem bool) (int, int, []byte) {
		leftCursor := NewSegmentCursor(createSegmentsFromKeys(t, []keyWithBML{
			{
				additions: []uint64{0},
			},
		}), nil)
		rightCursor := NewSegmentCursor(createSegmentsFromKeys(t, []keyWithBML{
			{
				additions: []uint64{1},
			},
		}), nil)

		ws, err := NewCountingWriteSeeker()
		require.Nil(t, err)
		defer ws.Close()

		maxNewFileSize := int64(len(leftCursor.data)+len(rightCursor.data)) + segmentindex.HeaderSize + 156 // for checksum
		// if the maxNewFileSize is already larger than our
		// SegmentWriterBufferSize there is no point in this test, both paths
		// would use the regular buffer.
		require.Less(t, maxNewFileSize, int64(compactor.SegmentWriterBufferSize))

		if !inMem {
			// we can force the "regular" write path by setting the maxNewFileSize
			// to a value larger than the threshold
			maxNewFileSize = compactor.SegmentWriterBufferSize + 1
		}

		c := NewCompactor(ws, leftCursor, rightCursor, 0, true, true, maxNewFileSize)

		err = c.Do(context.Background())
		require.Nil(t, err)

		b, err := ws.Bytes()
		require.Nil(t, err)

		return ws.WriteCalls, ws.SeekCalls, b
	}

	writeCallsRegular, seekCallsRegular, bytesRegular := compactionSetup(false)
	writeCallsInMem, seekCallsInMem, bytesInMem := compactionSetup(true)

	avgWriteSizeRegular := float64(len(bytesRegular)) / float64(writeCallsRegular)
	avgWriteSizeInMem := float64(len(bytesInMem)) / float64(writeCallsInMem)

	t.Logf("Regular write calls: %d (%d seek calls), avg write size: %fB", writeCallsRegular, seekCallsRegular, avgWriteSizeRegular)
	t.Logf("In memory write calls: %d (%d seek calls), avg write size: %fB", writeCallsInMem, seekCallsInMem, avgWriteSizeInMem)

	assert.Equal(t, 1, writeCallsInMem)
	assert.Less(t, writeCallsInMem, writeCallsRegular)
	assert.Equal(t, bytesRegular, bytesInMem)
}

type keyWithBML struct {
	key       []byte
	additions []uint64
	deletions []uint64
}

func createSegmentsFromKeys(t testing.TB, keys []keyWithBML) []byte {
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

// CountingWriteSeeker wraps an *os.File and records how it is used.
// It is intended for single‑goroutine unit tests.  Guard with a sync.Mutex
// if your code writes concurrently.
type CountingWriteSeeker struct {
	f *os.File

	// Statistics
	WriteCalls   int // how many times Write was invoked
	BytesWritten int // total bytes written
	SeekCalls    int // how many times Seek was invoked
}

// NewCountingWriteSeeker creates a temporary on‑disk file, unlinks it
// immediately (so nothing is left behind), and returns the wrapper.
// The file is removed automatically when it is closed or the process exits.
func NewCountingWriteSeeker() (*CountingWriteSeeker, error) {
	tmp, err := os.CreateTemp("", "counting-ws-*")
	if err != nil {
		return nil, err
	}
	// Remove from directory tree right away; the fd keeps it alive.
	_ = os.Remove(tmp.Name())

	return &CountingWriteSeeker{f: tmp}, nil
}

// Write records the call and forwards to the underlying *os.File.
func (c *CountingWriteSeeker) Write(p []byte) (int, error) {
	c.WriteCalls++
	n, err := c.f.Write(p)
	c.BytesWritten += n
	return n, err
}

// Seek records the call and forwards to the underlying *os.File.
func (c *CountingWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	c.SeekCalls++
	return c.f.Seek(offset, whence)
}

// Bytes returns the full contents written so far.
// It leaves the file offset unchanged.
func (c *CountingWriteSeeker) Bytes() ([]byte, error) {
	// Save current position
	pos, err := c.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// Read everything
	_, _ = c.f.Seek(0, io.SeekStart)
	b, err := io.ReadAll(c.f)
	// Restore previous position (ignore error)
	_, _ = c.f.Seek(pos, io.SeekStart)
	return b, err
}

// Close closes the underlying file.
func (c *CountingWriteSeeker) Close() error { return c.f.Close() }

// TestCompactorStoresNoEmptyBitmap is the compaction-side pair of
// TestFlushRoaringSetStoresNoEmptyBitmap: whatever a compaction re-encodes, an
// empty side must still cost a zero length indicator and no payload. Both
// writers share one encoder, so what this adds over the flush's pair is
// cleanupValues: with cleanup on, a node holding only deletions goes, and one
// holding both keeps no deletions payload.
//
// It also walks the body to IndexStart, which is the structural check that a
// recorded byte-for-byte manifest would otherwise be carrying.
func TestCompactorStoresNoEmptyBitmap(t *testing.T) {
	// cleanup=true drops nodes holding only deletions, so both arms of
	// cleanupValues are exercised across the two rows.
	for _, cleanup := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup=%t", cleanup), func(t *testing.T) {
			left := createSegmentsFromKeys(t, []keyWithBML{
				{key: []byte("additions only"), additions: slice(0, 10)},
				{key: []byte("both sides"), additions: slice(20, 30), deletions: slice(40, 50)},
			})
			right := createSegmentsFromKeys(t, []keyWithBML{
				{key: []byte("deletions only"), deletions: slice(100, 110)},
			})

			data := cursorCompactor(t, NewSegmentCursor(left, nil), NewSegmentCursor(right, nil),
				compactor.SegmentWriterBufferSize+1, cleanup, false)

			header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
			require.NoError(t, err)

			nodes := data[segmentindex.HeaderSize:header.IndexStart]
			sides := map[string][2]uint64{}
			at := 0
			for at < len(nodes) {
				nodeLen := binary.LittleEndian.Uint64(nodes[at : at+8])
				require.NotZero(t, nodeLen, "node at %d reports a zero length", at)
				aLen := binary.LittleEndian.Uint64(nodes[at+8 : at+16])
				dLen := binary.LittleEndian.Uint64(nodes[at+16+int(aLen) : at+24+int(aLen)])
				keyOff := at + 24 + int(aLen) + int(dLen)
				keyLen := binary.LittleEndian.Uint32(nodes[keyOff : keyOff+4])
				sides[string(nodes[keyOff+4:keyOff+4+int(keyLen)])] = [2]uint64{aLen, dLen}
				at += int(nodeLen)
			}
			require.Equal(t, len(nodes), at, "the body did not end where the header says the index starts")

			require.NotZero(t, sides["additions only"][0])
			require.Zero(t, sides["additions only"][1], "an empty deletions side must store no payload")

			if cleanup {
				// Deletions are dropped once nothing older can be masked, so a node
				// holding only deletions goes with them.
				require.NotContains(t, sides, "deletions only")
				require.Zero(t, sides["both sides"][1], "cleanup must leave no deletions payload")
			} else {
				require.Zero(t, sides["deletions only"][0], "an empty additions side must store no payload")
				require.NotZero(t, sides["deletions only"][1])
				require.NotZero(t, sides["both sides"][1])
			}
		})
	}
}

// TestCompactorIndexNamesTheNodesItPointsAt pins the half of the output the
// value tests never read: the index. Every entry must carry the key of the node
// its offsets land on.
//
// The compactor builds each node in one reused buffer, so an entry that took
// its key from the node — as SegmentNode.PrimaryKey returns it — would be left
// pointing at whatever the next node overwrote. The body would still be
// correct, and every value test would still pass.
func TestCompactorIndexNamesTheNodesItPointsAt(t *testing.T) {
	// Node sizes repeat, so a later node is built in the buffer an earlier one
	// used and writes over that node's key bytes. Sizes that only grow give
	// every node a fresh allocation, and no stale key is ever disturbed.
	keys := make([]keyWithBML, 0, 40)
	for i := 0; i < 40; i++ {
		keys = append(keys, keyWithBML{
			key:       []byte(fmt.Sprintf("key-%05d", i)),
			additions: slice(uint64(i)*1000, uint64(i)*1000+uint64(10+(i%4)*20)),
		})
	}

	data := cursorCompactor(t,
		NewSegmentCursor(createSegmentsFromKeys(t, keys[:20]), nil),
		NewSegmentCursor(createSegmentsFromKeys(t, keys[20:]), nil),
		compactor.SegmentWriterBufferSize+1, false, false)

	header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
	require.NoError(t, err)

	tree := segmentindex.NewDiskTree(data[header.IndexStart:])
	require.Equal(t, len(keys), tree.KeyCount())

	for _, k := range keys {
		node, err := tree.Get(k.key)
		require.NoError(t, err, "the index does not hold key %q", k.key)

		// Where the index says this key lives, a node carrying it must start.
		require.LessOrEqual(t, node.End, header.IndexStart,
			"index entry for %q points past the body", k.key)
		require.GreaterOrEqual(t, node.Start, uint64(segmentindex.HeaderSize),
			"index entry for %q points into the header", k.key)
		at := NewSegmentNodeFromBuffer(data[node.Start:node.End])
		require.Equal(t, k.key, at.PrimaryKey(),
			"the index entry for %q points at a node holding %q", k.key, at.PrimaryKey())
	}
}

// benchmarkCompactorKeys is the compaction fixture: two segments of equal size
// under disjoint keys, so every node takes the takeLeftKey or takeRightKey arm
// and the count of nodes built is the count of keys.
func benchmarkCompactorKeys(t testing.TB, perSegment int) (left, right []byte) {
	t.Helper()

	keys := make([]keyWithBML, 0, perSegment*2)
	for i := 0; i < perSegment*2; i++ {
		keys = append(keys, keyWithBML{
			key:       []byte(fmt.Sprintf("key-%05d", i)),
			additions: slice(uint64(i)*1000, uint64(i)*1000+500),
		})
	}
	return createSegmentsFromKeys(t, keys[:perSegment]),
		createSegmentsFromKeys(t, keys[perSegment:])
}

// BenchmarkCompactorRoaringSet reports what one compaction allocates. The
// figure a commit message quotes for this path comes from here: the nodes are
// built into one buffer the compaction carries, so allocs/op is what says the
// buffer is still being reused rather than re-made per node.
func BenchmarkCompactorRoaringSet(b *testing.B) {
	const perSegment = 100

	left, right := benchmarkCompactorKeys(b, perSegment)
	dir := b.TempDir()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f, err := os.Create(filepath.Join(dir, "compacted.db"))
		require.NoError(b, err)
		b.StartTimer()

		c := NewCompactor(f, NewSegmentCursor(left, nil), NewSegmentCursor(right, nil),
			5, false, false, int64(compactor.SegmentWriterBufferSize+1))
		require.NoError(b, c.Do(context.Background()))

		b.StopTimer()
		require.NoError(b, f.Close())
		b.StartTimer()
	}
}

// TestCompactorSegmentChecksumValidates reads a compacted segment back the way
// a startup does. The index is marshalled through SegmentFile.BodyWriter, which
// is the writer the CRC is computed over, so a change routing those bytes past
// it would leave a segment that parses and fails validation on the next load.
func TestCompactorSegmentChecksumValidates(t *testing.T) {
	// Deletions on both sides, so the cleanup arm has nodes to drop and the two
	// rows do not compact to the same bytes.
	left := createSegmentsFromKeys(t, []keyWithBML{
		{key: []byte("aaa"), additions: slice(0, 200), deletions: slice(500, 520)},
		{key: []byte("ccc"), deletions: slice(600, 620)},
	})
	right := createSegmentsFromKeys(t, []keyWithBML{
		{key: []byte("bbb"), additions: slice(1000, 1200), deletions: slice(1500, 1520)},
	})

	for _, cleanup := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup=%t", cleanup), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "compacted.db")
			f, err := os.Create(path)
			require.NoError(t, err)

			c := NewCompactor(f, NewSegmentCursor(left, nil), NewSegmentCursor(right, nil),
				5, cleanup, true, int64(compactor.SegmentWriterBufferSize+1))
			require.NoError(t, c.Do(context.Background()))
			require.NoError(t, f.Close())

			rf, err := os.Open(path)
			require.NoError(t, err)
			defer rf.Close()

			info, err := rf.Stat()
			require.NoError(t, err)

			sf := segmentindex.NewSegmentFile(segmentindex.WithReader(rf))
			require.NoError(t, sf.ValidateChecksum(info.Size(), segmentindex.HeaderSize),
				"a compacted segment must validate against the checksum it wrote")
		})
	}
}
