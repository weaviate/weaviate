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
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// TestFlushRoaringSetKeysDoNotAliasNodeBuffers pins the backing array, not just
// the bytes: a key copied out of the node's serialization would release the
// segment body and still pass a bytes-only assertion, and copying every key per
// flush is the cost this avoids.
func TestFlushRoaringSetKeysDoNotAliasNodeBuffers(t *testing.T) {
	m := newRoaringSetFlushFixture(t, flushFixtureShapes(), false)

	keys, err := m.writeRoaringSetNodes(discardingSegmentFile())
	require.NoError(t, err)

	// Pairing by index assumes the flush walks in FlattenInOrder order, which is
	// what makes a walk-order change report as a mismatch rather than as a
	// pointer surprise.
	flat := m.roaringSet.FlattenInOrder()
	require.Len(t, keys, len(flat))
	require.Equal(t, m.roaringSet.Count(), len(keys),
		"the tree's count is what presizes keys, so a wrong one silently regrows it")

	for i, node := range flat {
		require.Equal(t, node.Key, keys[i].Key,
			"key %d does not hold its node's key bytes", i)
		require.Same(t, unsafe.SliceData(node.Key), unsafe.SliceData(keys[i].Key),
			"key %d points into the node's serialization, holding the segment body", i)
	}
}

// TestFlushRoaringSetRejectsSecondaryIndexes drives a whole flush, since the
// refusal guards a segment the flush would otherwise finish writing. It also
// pins what flush() leaves behind: the partial .db.tmp is removed, so a failed
// flush cannot be read as a segment by the next startup scan.
func TestFlushRoaringSetRejectsSecondaryIndexes(t *testing.T) {
	m := newRoaringSetFlushFixture(t, flushFixtureShapes(), false)
	m.secondaryIndices = 1

	segmentPath, err := m.flush()
	require.ErrorContains(t, err, "secondary indexes")
	require.Empty(t, segmentPath)

	left, err := os.ReadDir(filepath.Dir(m.path))
	require.NoError(t, err)
	for _, entry := range left {
		require.NotContains(t, entry.Name(), ".db",
			"a failed flush left %q behind", entry.Name())
	}
}

var errDiskFull = errors.New("no space left on device")

// failingWriteSeeker fails the failOnWrite'th Write, counting from 1, or every
// Seek when failSeek is set. It stands in for the ENOSPC or EIO a real segment
// file returns; nothing else in the package can reach those paths.
type failingWriteSeeker struct {
	failOnWrite int
	failSeek    bool
	writes      int
	offset      int64
}

func (w *failingWriteSeeker) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.failOnWrite {
		return 0, errDiskFull
	}
	w.offset += int64(len(p))
	return len(p), nil
}

func (w *failingWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	if w.failSeek {
		return 0, errDiskFull
	}
	switch whence {
	case io.SeekStart:
		w.offset = offset
	case io.SeekEnd:
		w.offset += offset
	}
	return w.offset, nil
}

// TestFlushDataRoaringSetReportsWriteFailures walks the error paths an ENOSPC
// or EIO reaches during an ordinary flush. The header patch is the one worth
// most: it is the only failure that lands after a complete body and index are
// already written, so what it leaves behind looks finished.
//
// bufSize is what selects the path. A buffer larger than the segment holds
// every node until the explicit Flush, so that is the first write the segment
// file makes; a buffer the size of the reserved header pushes each node
// straight through instead.
func TestFlushDataRoaringSetReportsWriteFailures(t *testing.T) {
	tests := []struct {
		name        string
		bufSize     int
		failOnWrite int
		failSeek    bool
		wantErr     string
	}{
		{
			name:        "a node write fails",
			bufSize:     segmentindex.HeaderSize,
			failOnWrite: 1,
			wantErr:     "write node 0",
		},
		{
			name:        "the body and index flush fails",
			bufSize:     1 << 16,
			failOnWrite: 1,
			wantErr:     "flush buffered",
		},
		{
			name:        "the header patch fails after the body is written",
			bufSize:     1 << 16,
			failOnWrite: 2,
			wantErr:     "write header",
		},
		{
			name:     "seeking back to patch the header fails",
			bufSize:  1 << 16,
			failSeek: true,
			wantErr:  "write header",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newRoaringSetFlushFixture(t, flushFixtureShapes(), false)

			ws := new(failingWriteSeeker)
			ws.failOnWrite = tt.failOnWrite
			ws.failSeek = tt.failSeek
			bufw := bufio.NewWriterSize(ws, tt.bufSize)
			f := segmentindex.NewSegmentFile(segmentindex.WithBufferedWriter(bufw))

			err := m.flushDataRoaringSet(f, ws, bufw)
			require.ErrorContains(t, err, tt.wantErr)
			require.ErrorIs(t, err, errDiskFull,
				"the underlying failure must survive the wrap")
		})
	}
}
