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
	"bufio"
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/testinghelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

func newRoaringSetRangeFlushFixture(tb testing.TB) *Memtable {
	tb.Helper()

	logger, _ := test.NewNullLogger()
	path := filepath.Join(tb.TempDir(), "segment")

	cl, err := newCommitLogger(path, StrategyRoaringSetRange, 0)
	require.NoError(tb, err)

	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     path,
		strategy: StrategyRoaringSetRange,
	})
	require.NoError(tb, err)

	for key := uint64(0); key < 8; key++ {
		require.NoError(tb, m.roaringSetRangeAdd(key, key+100, key+200))
	}
	return m
}

func TestFlushRoaringSetRangeBlocksOnMemtableWriteLock(t *testing.T) {
	m := newRoaringSetRangeFlushFixture(t)

	assertFlushBlocksOnMemtableWriteLock(t, m, func() error {
		_, err := m.flushDataRoaringSetRange(discardingSegmentFile())
		return err
	})
}

func TestFlushRoaringSetRangeConcurrentWrite(t *testing.T) {
	m := newRoaringSetRangeFlushFixture(t)

	assertFlushSurvivesConcurrentWrite(t, m,
		func(i int) error { return m.roaringSetRangeAdd(uint64(i), uint64(i)+1000) },
		func() error {
			_, err := m.flushDataRoaringSetRange(discardingSegmentFile())
			return err
		})
}

type roaringSetRangeNodeShape struct {
	name string
	node *roaringsetrange.MemtableNode
}

// roaringSetRangeNodeShapes covers the shapes the writer must emit.
// roaringSetRangeNodes cannot produce three of them: its key 0 node always
// carries deletions, and a keyed node exists only once its bitmap holds a value.
var roaringSetRangeNodeShapes = []roaringSetRangeNodeShape{
	{"key 0, both sides filled", &roaringsetrange.MemtableNode{
		Key:       0,
		Additions: roaringset.NewBitmap(1, 2, 3, 4, 6),
		Deletions: roaringset.NewBitmap(5, 7),
	}},
	{"key 0, empty additions", &roaringsetrange.MemtableNode{
		Key:       0,
		Additions: roaringset.NewBitmap(),
		Deletions: roaringset.NewBitmap(5, 7),
	}},
	{"key 0, empty deletions", &roaringsetrange.MemtableNode{
		Key:       0,
		Additions: roaringset.NewBitmap(1, 2, 3, 4, 6),
		Deletions: roaringset.NewBitmap(),
	}},
	{"key 0, both sides empty", &roaringsetrange.MemtableNode{
		Key:       0,
		Additions: roaringset.NewBitmap(),
		Deletions: roaringset.NewBitmap(),
	}},
	{"lowest non-zero key", &roaringsetrange.MemtableNode{
		Key:       1,
		Additions: roaringset.NewBitmap(1, 2, 3, 4, 6),
		Deletions: nil,
	}},
	{"highest key, empty additions", &roaringsetrange.MemtableNode{
		Key:       64,
		Additions: roaringset.NewBitmap(),
		Deletions: nil,
	}},
}

// TestWriteRoaringSetRangeNode pins the writer against NewSegmentNode, an
// independent route to the same layout. A slipped offset or length indicator
// then shows as a byte difference, not as a segment that reads back wrong.
func TestWriteRoaringSetRangeNode(t *testing.T) {
	all := make([]*roaringsetrange.MemtableNode, 0, len(roaringSetRangeNodeShapes))
	for _, shape := range roaringSetRangeNodeShapes {
		all = append(all, shape.node)
	}

	tests := []roaringSetRangeNodeShape{{name: "no nodes"}}
	tests = append(tests, roaringSetRangeNodeShapes...)

	for _, tt := range tests {
		nodes := []*roaringsetrange.MemtableNode{tt.node}
		if tt.node == nil {
			nodes = nil
		}
		t.Run(tt.name, func(t *testing.T) {
			requireRoaringSetRangeNodesWritten(t, nodes)
		})
	}

	t.Run("every shape back to back", func(t *testing.T) {
		requireRoaringSetRangeNodesWritten(t, all)
	})
}

// requireRoaringSetRangeNodesWritten writes nodes through one scratch array, the
// way the flush loop does, and requires the bytes to match NewSegmentNode's.
func requireRoaringSetRangeNodesWritten(t *testing.T, nodes []*roaringsetrange.MemtableNode) {
	t.Helper()

	var expected bytes.Buffer
	for _, node := range nodes {
		sn, err := roaringsetrange.NewSegmentNode(node.Key, node.Additions, node.Deletions)
		require.NoError(t, err)
		expected.Write(sn.ToBuffer())
	}

	var scratch [roaringsetrange.AdditionsStart]byte
	var written bytes.Buffer
	for _, node := range nodes {
		require.NoError(t, writeRoaringSetRangeNode(&written, node, &scratch))
	}

	require.Equal(t, expected.Bytes(), written.Bytes(),
		"the writer must emit the byte-for-byte layout NewSegmentNode produces")
	require.Equal(t, expected.Len(), totalPayloadSizeRoaringSetRange(nodes),
		"the header reserves totalPayloadSizeRoaringSetRange bytes for the body")
}

// TestWriteRoaringSetRangeNodeWriteError fails the stream at every byte a node
// occupies, so a change to the layout or the bitmap encoding moves the sweep.
func TestWriteRoaringSetRangeNodeWriteError(t *testing.T) {
	for _, shape := range roaringSetRangeNodeShapes {
		t.Run(shape.name, func(t *testing.T) {
			var scratch [roaringsetrange.AdditionsStart]byte

			var full bytes.Buffer
			require.NoError(t, writeRoaringSetRangeNode(&full, shape.node, &scratch))

			for b := 1; b <= full.Len(); b++ {
				w := &testinghelpers.FailingWriteSeeker{FailAtByte: b}
				require.ErrorIsf(t, writeRoaringSetRangeNode(w, shape.node, &scratch),
					testinghelpers.ErrDiskFull,
					"failing at byte %d of %d must reach the caller", b, full.Len())
			}
		})
	}
}

// TestFlushDataRoaringSetRangeReportsWriteFailures pins the node index in the
// wrap, an operator's only clue how far a failed flush got.
func TestFlushDataRoaringSetRangeReportsWriteFailures(t *testing.T) {
	starts := roaringSetRangeFixtureNodeStarts(t)

	for _, node := range []int{0, 1} {
		t.Run(fmt.Sprintf("node %d fails", node), func(t *testing.T) {
			m := newRoaringSetRangeFlushFixture(t)

			ws := &testinghelpers.FailingWriteSeeker{FailAtByte: starts[node] + 1}
			// The flush returns without flushing, so a larger buffer would hold
			// every byte back and no write would reach ws at all.
			bufw := bufio.NewWriterSize(ws, 1)
			f := segmentindex.NewSegmentFile(segmentindex.WithBufferedWriter(bufw))

			_, err := m.flushDataRoaringSetRange(f)
			require.ErrorContains(t, err, fmt.Sprintf("write segment node %d", node))
			require.ErrorIs(t, err, testinghelpers.ErrDiskFull,
				"the underlying failure must survive the wrap")
		})
	}
}

// roaringSetRangeFixtureNodeStarts returns the byte offset each fixture node
// begins at, counting the header the flush writes first.
func roaringSetRangeFixtureNodeStarts(t *testing.T) []int {
	t.Helper()

	nodes := newRoaringSetRangeFlushFixture(t).roaringSetRangeNodes()
	require.Greater(t, len(nodes), 1, "the fixture must produce a node after the key 0 one")

	var scratch [roaringsetrange.AdditionsStart]byte
	var body bytes.Buffer
	starts := make([]int, 0, len(nodes))
	for _, node := range nodes {
		starts = append(starts, segmentindex.HeaderSize+body.Len())
		require.NoError(t, writeRoaringSetRangeNode(&body, node, &scratch))
	}
	return starts
}
