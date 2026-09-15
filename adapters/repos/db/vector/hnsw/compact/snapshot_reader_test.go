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

package compact

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"runtime"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestValidateSnapshotBlockRanges pins the tolerance policy: interior gaps
// (the signature of the legacy classic-writer block-boundary bug, see
// weaviate/0-weaviate-issues#268) are tolerated and reported as missing nodes,
// while overlap, trailing shortfall (truncation), and beyond-count still fail
// closed.
func TestValidateSnapshotBlockRanges(t *testing.T) {
	tests := []struct {
		name        string
		ranges      []snapshotBlockRange
		nodeCount   int
		wantMissing int
		wantErr     bool
	}{
		{"zero nodes", nil, 0, 0, false},
		{"missing body for nodes", nil, 10, 0, true},
		{"clean contiguous", []snapshotBlockRange{{0, 5}, {5, 10}}, 10, 0, false},
		{"interior gap tolerated", []snapshotBlockRange{{0, 5}, {6, 10}}, 10, 1, false},
		{"multiple interior gaps tolerated", []snapshotBlockRange{{0, 5}, {6, 10}, {11, 15}}, 15, 2, false},
		{"leading gap tolerated", []snapshotBlockRange{{2, 10}}, 10, 2, false},
		{"overlap fails", []snapshotBlockRange{{0, 6}, {5, 10}}, 10, 0, true},
		{"trailing shortfall fails", []snapshotBlockRange{{0, 5}}, 10, 0, true},
		{"beyond node count fails", []snapshotBlockRange{{0, 12}}, 10, 0, true},
		{"invalid range fails", []snapshotBlockRange{{5, 3}}, 10, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logtest.NewNullLogger()
			err := validateSnapshotBlockRanges(tt.ranges, tt.nodeCount, logger)
			if tt.wantErr {
				require.Error(t, err)
				require.Empty(t, hook.AllEntries(), "no warning expected on error")
				return
			}
			require.NoError(t, err)
			if tt.wantMissing > 0 {
				entries := hook.AllEntries()
				require.Len(t, entries, 1)
				require.Equal(t, logrus.WarnLevel, entries[0].Level)
				require.Equal(t, tt.wantMissing, entries[0].Data["missing"])
			} else {
				require.Empty(t, hook.AllEntries(), "no warning when nothing is missing")
			}
		})
	}
}

// TestSnapshotReader_ToleratesLegacyInteriorGap reads a snapshot whose body has
// an interior gap (one node dropped at a block boundary, as the legacy classic
// writer produced — weaviate/0-weaviate-issues#268). The reader must load it
// without error, leave the dropped slot nil, keep its neighbours, and log a
// warning. The compact writer cannot emit such a gap, so the body is crafted by
// hand and paired with a real (writer-produced) metadata header.
func TestSnapshotReader_ToleratesLegacyInteriorGap(t *testing.T) {
	const (
		blockSize = 128
		nodeCount = 6
		gapID     = 3
	)

	header := snapshotMetadataHeader(t, blockSize, nodeCount)
	body := gappedSnapshotBody(t, blockSize, nodeCount, gapID)
	full := append(append([]byte{}, header...), body...)

	logger, hook := logtest.NewNullLogger()
	result, err := NewSnapshotReaderWithBlockSize(logger, blockSize).Read(bytes.NewReader(full))
	require.NoError(t, err)

	require.Equal(t, nodeCount, len(result.Graph.Nodes))
	require.Nil(t, result.Graph.Nodes[gapID], "dropped slot must load as nil")
	for id := 0; id < nodeCount; id++ {
		if id == gapID {
			continue
		}
		require.NotNilf(t, result.Graph.Nodes[id], "node %d must survive", id)
	}

	var warned bool
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "missing") {
			warned = true
		}
	}
	require.True(t, warned, "expected a WARN about missing nodes")
}

// TestSnapshotReader_BlockBuffersCappedByBlockCount pins that Read allocates one
// block buffer per block, up to snapshotConcurrency, so loading a small
// snapshot does not pay for snapshotConcurrency full-size buffers.
func TestSnapshotReader_BlockBuffersCappedByBlockCount(t *testing.T) {
	const blockSize = 4 * 1024 * 1024

	tests := []struct {
		name       string
		blocks     int
		maxBuffers int
	}{
		{name: "one block", blocks: 1, maxBuffers: 1},
		{name: "two blocks", blocks: 2, maxBuffers: 2},
		{name: "as many blocks as workers", blocks: snapshotConcurrency, maxBuffers: snapshotConcurrency},
		{name: "more blocks than workers", blocks: snapshotConcurrency + 1, maxBuffers: snapshotConcurrency},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := snapshotMetadataHeader(t, blockSize, tt.blocks)
			full := make([]byte, 0, len(header)+tt.blocks*blockSize)
			full = append(full, header...)
			for id := 0; id < tt.blocks; id++ {
				full = append(full, snapshotBlock(t, blockSize, uint64(id), 1)...)
			}
			reader := NewSnapshotReaderWithBlockSize(logrus.New(), blockSize)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			result, err := reader.Read(bytes.NewReader(full))
			runtime.ReadMemStats(&after)
			require.NoError(t, err)
			require.Len(t, result.Graph.Nodes, tt.blocks)

			// One extra blockSize covers the nodes and bookkeeping Read allocates
			// besides the block buffers.
			allocated := after.TotalAlloc - before.TotalAlloc
			require.Lessf(t, allocated, uint64((tt.maxBuffers+1)*blockSize),
				"Read allocated %d bytes for %d block(s)", allocated, tt.blocks)
		})
	}
}

// snapshotMetadataHeader returns the version+checksum+metadata prefix of a real
// V3 snapshot with the given node count, so tests can pair valid metadata with a
// hand-crafted body.
func snapshotMetadataHeader(t *testing.T, blockSize, nodeCount int) []byte {
	t.Helper()
	var buf bytes.Buffer
	sw := NewSnapshotWriterWithBlockSize(&buf, int64(blockSize))
	sw.SetEntrypoint(0, 0)
	for i := 0; i < nodeCount; i++ {
		sw.AddNode(uint64(i), 0, [][]uint64{{0}}, false)
	}
	require.NoError(t, sw.Flush())
	b := buf.Bytes()
	metaSize := binary.LittleEndian.Uint32(b[5:9])
	return append([]byte{}, b[:9+int(metaSize)]...)
}

// gappedSnapshotBody builds a body of two fixed-size blocks that together cover
// every slot except gapID: block one is [0,gapID), block two is [gapID+1,
// nodeCount). Each present slot is a minimal live entry (existence 2, level 0,
// no connections).
func gappedSnapshotBody(t *testing.T, blockSize, nodeCount, gapID int) []byte {
	t.Helper()
	b1 := snapshotBlock(t, blockSize, 0, gapID)                             // nodes [0, gapID)
	b2 := snapshotBlock(t, blockSize, uint64(gapID+1), nodeCount-(gapID+1)) // nodes [gapID+1, nodeCount)
	return append(append([]byte{}, b1...), b2...)
}

// snapshotBlock builds one fixed-size body block holding count minimal live
// entries (existence 2, level 0, no connections) starting at startID.
func snapshotBlock(t *testing.T, blockSize int, startID uint64, count int) []byte {
	t.Helper()
	maxBlockSize := blockSize - 8

	var block bytes.Buffer
	var u64 [8]byte
	binary.LittleEndian.PutUint64(u64[:], startID)
	block.Write(u64[:])
	for i := 0; i < count; i++ {
		var e [9]byte
		e[0] = 2 // alive, no tombstone
		// level (uint32) and connSize (uint32) both zero
		block.Write(e[:])
	}
	blockLen := block.Len()
	require.LessOrEqualf(t, blockLen, maxBlockSize, "block starting at %d does not fit", startID)
	block.Write(make([]byte, maxBlockSize-blockLen))
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], uint32(blockLen))
	block.Write(u32[:])

	cs := crc32.ChecksumIEEE(block.Bytes())
	out := make([]byte, 0, blockSize)
	binary.LittleEndian.PutUint32(u32[:], cs)
	out = append(out, u32[:]...)
	out = append(out, block.Bytes()...)
	require.Equal(t, blockSize, len(out))
	return out
}
