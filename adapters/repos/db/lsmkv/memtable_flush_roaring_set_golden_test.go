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
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

var updateRoaringSetGolden = flag.Bool("update-roaringset-golden", false,
	"rewrite the recorded roaring-set segments instead of asserting against them")

// fixtureShape is one memtable key: the doc IDs added under it, then the doc
// IDs removed. Removing IDs that were added grows the memtable's additions
// bitmap past its contents; Condense reclaims that before the flush serializes
// it, so it shows in the memtable and never in the recorded bytes.
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
		// checksum is appended, so TestFlushRoaringSetGolden crosses both values.
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

// goldenFixtureShapes covers additions only, deletions only, both, and
// additions emptied by a later remove — which serializes like deletions only
// and is here for the memtable state, a grown but empty additions bitmap.
// The zero-length key tells a nil-key loop termination apart from a
// length-based one. Keys are distinct, so the node count equals the shape count.
func goldenFixtureShapes() []fixtureShape {
	return []fixtureShape{
		{key: []byte{}, add: docIDRange(0, 8)},
		{key: []byte("additions"), add: docIDRange(100, 1100)},
		{key: []byte("both"), add: docIDRange(1200, 2200), remove: docIDRange(1220, 2200)},
		{key: []byte("deletions"), remove: docIDRange(2300, 2364)},
		{key: []byte("emptied"), add: docIDRange(2400, 2464), remove: docIDRange(2400, 2464)},
	}
}

func TestFlushRoaringSetGolden(t *testing.T) {
	tests := []struct {
		name   string
		shapes []fixtureShape
	}{
		{name: "five shapes", shapes: goldenFixtureShapes()},
		{name: "single node", shapes: []fixtureShape{{key: []byte("only"), add: docIDRange(0, 512)}}},
		{name: "empty", shapes: nil},
	}

	for _, tt := range tests {
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

				assertGoldenSegment(t, goldenName(tt.name, checksums), data)
			})
		}
	}
}

// TestFlushRoaringSetRoundTrip reads every doc ID back out of a flushed
// segment. TestFlushRoaringSetGolden's assertions are all structural — offsets,
// counts, a hash — so -update-roaringset-golden would happily record a segment
// whose values are wrong. This is the assertion that regenerating cannot
// launder, which is why it has to exist before any step regenerates.
func TestFlushRoaringSetRoundTrip(t *testing.T) {
	for _, checksums := range []bool{false, true} {
		t.Run(fmt.Sprintf("checksums=%t", checksums), func(t *testing.T) {
			shapes := goldenFixtureShapes()
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

func goldenName(fixture string, checksums bool) string {
	return fmt.Sprintf("%s_checksums-%t", strings.ReplaceAll(fixture, " ", "-"), checksums)
}

// segmentManifest itemises a flushed segment: the header's numbers, one line
// per node, the index entry count, and the hash of the whole file. The hash
// alone would report a changed byte as two hex strings; the itemisation says
// which node moved and by how much, which is the quantity the compacting
// rewrite is judged on.
func segmentManifest(tb testing.TB, data []byte) string {
	tb.Helper()

	header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
	require.NoError(tb, err)
	require.GreaterOrEqual(tb, len(data), int(header.IndexStart),
		"the segment is shorter than the header's IndexStart")

	var out strings.Builder
	fmt.Fprintf(&out, "size %d\n", len(data))
	fmt.Fprintf(&out, "header version %d\n", header.Version)
	fmt.Fprintf(&out, "index start %d\n", header.IndexStart)

	offset := segmentindex.HeaderSize
	for i := 0; offset < int(header.IndexStart); i++ {
		node := roaringset.NewSegmentNodeFromBuffer(data[offset:])
		// A zero length would make this walk run forever on a corrupt segment.
		require.NotZero(tb, node.Len(), "node %d reports a zero length", i)
		fmt.Fprintf(&out, "node %d key %q len %d additions %d deletions %d\n",
			i, node.PrimaryKey(), node.Len(),
			len(node.Additions().ToArray()), len(node.Deletions().ToArray()))
		offset += int(node.Len())
	}

	fmt.Fprintf(&out, "index entries %d\n",
		segmentindex.NewDiskTree(data[header.IndexStart:]).KeyCount())
	fmt.Fprintf(&out, "sha256 %x\n", sha256.Sum256(data))

	return out.String()
}

// assertGoldenSegment compares the flushed segment against its recorded
// manifest, or rewrites the record under -update-roaringset-golden, so that
// nothing recorded is ever hand-typed.
func assertGoldenSegment(tb testing.TB, name string, data []byte) {
	tb.Helper()

	got := segmentManifest(tb, data)
	path := filepath.Join("testdata", "roaringset-golden", name+".golden")

	if *updateRoaringSetGolden {
		require.NoError(tb, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(tb, os.WriteFile(path, []byte(got), 0o644))
		return
	}

	recorded, err := os.ReadFile(path)
	require.NoError(tb, err,
		"no recorded segment; re-run with: go test ./adapters/repos/db/lsmkv/ -run TestFlushRoaringSetGolden -update-roaringset-golden")
	require.Equal(tb, string(recorded), got,
		"the flushed segment changed; only if the change was intended, re-run with: go test ./adapters/repos/db/lsmkv/ -run TestFlushRoaringSetGolden -update-roaringset-golden")
}
