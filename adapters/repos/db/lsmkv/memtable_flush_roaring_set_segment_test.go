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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// fixtureShape is one memtable key: the doc IDs added under it, then the doc
// IDs removed. Removing IDs that were added grows the memtable's additions
// bitmap past its contents; the flush compacts that away as it serializes, so
// it shows in the memtable and never in the recorded bytes.
type fixtureShape struct {
	key    []byte
	add    []uint64
	remove []uint64
}

// newRoaringSetFlushFixture builds a roaring-set memtable holding shapes, ready
// to flush. Both the commit log and the segment take their path from inside the
// temp dir, since flush() derives the segment path from the memtable's own and
// would otherwise write a sibling of the dir that cleanup does not reach.
func newRoaringSetFlushFixture(tb testing.TB, shapes []fixtureShape,
	enableChecksumValidation bool,
) *Memtable {
	tb.Helper()

	logger, _ := test.NewNullLogger()
	path := filepath.Join(tb.TempDir(), "segment")

	cl, err := newCommitLogger(path, StrategyRoaringSet, 0)
	require.NoError(tb, err)

	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     path,
		strategy: StrategyRoaringSet,
		// enableChecksumValidation selects the header version and whether a
		// checksum is appended, so TestFlushRoaringSetSegmentStructure crosses both values.
		enableChecksumValidation: enableChecksumValidation,
		// writeSegmentInfoIntoFileName selects only which path the segment lands
		// on, which callers read back from flush() rather than deriving.
		writeSegmentInfoIntoFileName: false,
	})
	require.NoError(tb, err)

	seen := make(map[string]struct{}, len(shapes))
	for _, shape := range shapes {
		require.NotContains(tb, seen, string(shape.key),
			"fixture keys must be distinct — a duplicate merges into one node")
		seen[string(shape.key)] = struct{}{}
		require.False(tb, len(shape.add) == 0 && len(shape.remove) == 0,
			"a shape with no doc IDs creates no node, so the node count would not match")
		if len(shape.add) > 0 {
			require.NoError(tb, m.roaringSetAddList(shape.key, shape.add))
		}
		if len(shape.remove) > 0 {
			require.NoError(tb, m.roaringSetRemoveList(shape.key, shape.remove))
		}
	}

	return m
}

// docIDRange returns the half-open range [from, to). Fixtures name a range rather
// than sampling, since the recorded hashes reproduce only if the bytes do.
func docIDRange(from, to uint64) []uint64 {
	out := make([]uint64, 0, to-from)
	for id := from; id < to; id++ {
		out = append(out, id)
	}
	return out
}

// flushFixtureShapes covers additions only, deletions only, both, and
// additions emptied by a later remove — which serializes like deletions only
// and is here for the memtable state, a grown but empty additions bitmap.
// The zero-length key tells a nil-key loop termination apart from a
// length-based one. Keys are distinct, so the node count equals the shape count.
//
// The last two shapes reach container arms the others cannot. A key holding
// more than 2048 values is stored as a bitmap container, and emptying it back
// down is what makes the flush rewrite it as an array; a key whose only values
// sit above 65536 leaves an empty container at a non-zero key once they are
// removed, which is dropped rather than written.
func flushFixtureShapes() []fixtureShape {
	return []fixtureShape{
		{key: []byte{}, add: docIDRange(0, 8)},
		{key: []byte("additions"), add: docIDRange(100, 1100)},
		{key: []byte("both"), add: docIDRange(1200, 2200), remove: docIDRange(1220, 2200)},
		{key: []byte("deletions"), remove: docIDRange(2300, 2364)},
		{key: []byte("emptied"), add: docIDRange(2400, 2464), remove: docIDRange(2400, 2464)},
		{
			key:    []byte("was a bitmap container"),
			add:    docIDRange(3000, 8000),
			remove: docIDRange(3100, 8000),
		},
		{
			key:    []byte("emptied a high container"),
			add:    append(docIDRange(10, 20), docIDRange(65536, 65636)...),
			remove: docIDRange(65536, 65636),
		},
	}
}

// flushFixtures are the fixtures whose bytes are recorded. The round trip
// walks the same list, so a regeneration that keeps every cardinality cannot
// hide a value that moved.
func flushFixtures() []struct {
	name   string
	shapes []fixtureShape
} {
	return []struct {
		name   string
		shapes []fixtureShape
	}{
		{name: "five shapes", shapes: flushFixtureShapes()},
		{name: "single node", shapes: []fixtureShape{{key: []byte("only"), add: docIDRange(0, 512)}}},
		{name: "empty", shapes: nil},
	}
}

func TestFlushRoaringSetSegmentStructure(t *testing.T) {
	for _, tt := range flushFixtures() {
		for _, checksums := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/checksums=%t", tt.name, checksums), func(t *testing.T) {
				m := newRoaringSetFlushFixture(t, tt.shapes, checksums)

				segmentPath, err := m.flush()
				require.NoError(t, err)

				if len(tt.shapes) == 0 {
					require.Empty(t, segmentPath)
					left, err := os.ReadDir(filepath.Dir(m.path))
					require.NoError(t, err)
					require.Empty(t, left,
						"an empty memtable must write no segment and delete its commit log")
					return
				}

				data, err := os.ReadFile(segmentPath)
				require.NoError(t, err)

				header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
				require.NoError(t, err)

				// Walking the body node by node has to land exactly on IndexStart.
				// Reading the index through IndexStart only proves it points at
				// something parseable; this pins the value itself.
				require.GreaterOrEqual(t, len(data), int(header.IndexStart),
					"the segment is shorter than the header's IndexStart")
				offset := segmentindex.HeaderSize
				for i := range tt.shapes {
					require.Less(t, offset, int(header.IndexStart),
						"the body ran out after %d of %d nodes", i, len(tt.shapes))
					offset += int(roaringset.NewSegmentNodeFromBuffer(data[offset:]).Len())
				}
				require.Equal(t, int(header.IndexStart), offset,
					"the body did not end where the header says the index starts")

				require.Equal(t, len(tt.shapes),
					segmentindex.NewDiskTree(data[header.IndexStart:]).KeyCount(),
					"the index must carry one entry per key the memtable held")
			})
		}
	}
}

// TestFlushRoaringSetRoundTrip reads every doc ID back out of a flushed
// segment. TestFlushRoaringSetSegmentStructure beside it checks only offsets
// and counts, which hold for a segment whose values are wrong, so this is what
// says the flush wrote the doc IDs it was given.
func TestFlushRoaringSetRoundTrip(t *testing.T) {
	for _, tt := range flushFixtures() {
		if len(tt.shapes) == 0 {
			continue
		}
		for _, checksums := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/checksums=%t", tt.name, checksums), func(t *testing.T) {
				shapes := tt.shapes
				m := newRoaringSetFlushFixture(t, shapes, checksums)

				segmentPath, err := m.flush()
				require.NoError(t, err)

				data, err := os.ReadFile(segmentPath)
				require.NoError(t, err)

				header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
				require.NoError(t, err)

				offset := segmentindex.HeaderSize
				for i := range shapes {
					require.Less(t, offset, int(header.IndexStart),
						"the body ran out after %d of %d nodes", i, len(shapes))
					node := roaringset.NewSegmentNodeFromBuffer(data[offset:])
					assertNodeMatchesShape(t, shapes, node)
					offset += int(node.Len())
				}
				require.Equal(t, int(header.IndexStart), offset)
			})
		}
	}
}

// assertNodeMatchesShape compares one serialized node against the fixture shape
// that produced it, found by key because the segment is in key order and the
// fixture is not required to be.
func assertNodeMatchesShape(t *testing.T, shapes []fixtureShape, node *roaringset.SegmentNode) {
	t.Helper()

	for _, shape := range shapes {
		if !bytes.Equal(shape.key, node.PrimaryKey()) {
			continue
		}
		var additions []uint64
		for _, id := range shape.add {
			if !slices.Contains(shape.remove, id) {
				additions = append(additions, id)
			}
		}
		require.Equal(t, additions, node.Additions().ToArray(), "additions of key %q", shape.key)
		require.Equal(t, shape.remove, node.Deletions().ToArray(), "deletions of key %q", shape.key)
		return
	}
	require.Fail(t, "the segment holds a key no fixture shape produced", "%q", node.PrimaryKey())
}

// segmentSideLengths reads the two bitmap length indicators straight out of a
// node's bytes, ahead of any accessor. Additions() and Deletions() substitute
// nil for a zero indicator, so reading through them cannot tell a stored empty
// bitmap from an absent one — which is the distinction under test.
//
// Layout: [nodeLen u64][aLen u64][additions][dLen u64][deletions][keyLen u32][key]
func segmentSideLengths(t *testing.T, nodes []byte) map[string][2]uint64 {
	t.Helper()

	out := map[string][2]uint64{}
	for at := 0; at < len(nodes); {
		nodeLen := binary.LittleEndian.Uint64(nodes[at : at+8])
		require.NotZero(t, nodeLen, "node at %d reports a zero length", at)
		aLen := binary.LittleEndian.Uint64(nodes[at+8 : at+16])
		dLen := binary.LittleEndian.Uint64(nodes[at+16+int(aLen) : at+24+int(aLen)])
		keyOff := at + 24 + int(aLen) + int(dLen)
		keyLen := binary.LittleEndian.Uint32(nodes[keyOff : keyOff+4])
		out[string(nodes[keyOff+4:keyOff+4+int(keyLen)])] = [2]uint64{aLen, dLen}
		at += int(nodeLen)
	}
	return out
}

// TestFlushRoaringSetStoresNoEmptyBitmap pins that an empty side costs a zero
// length indicator and no payload. An empty sroar bitmap still owns its key-0
// container and occupies bytes in memory, and CompactedToBuf hands a caller
// that size rather than zero, so storing one is an easy mistake to make.
//
// TestFlushRoaringSetRoundTrip does catch it, but only through Additions()
// returning nil for a zero indicator and testify separating a nil slice from an
// empty one. This asserts the stored length directly, so the reason a failure
// names is the size on disk rather than a slice comparison.
func TestFlushRoaringSetStoresNoEmptyBitmap(t *testing.T) {
	shapes := []fixtureShape{
		{key: []byte("additions only"), add: docIDRange(0, 10)},
		{key: []byte("deletions only"), remove: docIDRange(100, 110)},
		// Allocated then emptied, so the source bitmap holds containers while
		// holding no values — the case a length-based test would miss.
		{key: []byte("grown then emptied"), add: docIDRange(200, 5000), remove: docIDRange(200, 5000)},
	}

	m := newRoaringSetFlushFixture(t, shapes, false)
	segmentPath, err := m.flush()
	require.NoError(t, err)

	data, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
	require.NoError(t, err)

	sides := segmentSideLengths(t, data[segmentindex.HeaderSize:header.IndexStart])

	require.NotZero(t, sides["additions only"][0])
	require.Zero(t, sides["additions only"][1], "an empty deletions side must store no payload")

	require.Zero(t, sides["deletions only"][0], "an empty additions side must store no payload")
	require.NotZero(t, sides["deletions only"][1])

	require.Zero(t, sides["grown then emptied"][0],
		"a bitmap emptied after growing must store no payload, however much it holds in memory")
	require.NotZero(t, sides["grown then emptied"][1])
}
