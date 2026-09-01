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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryReader_CorruptEntrypoint(t *testing.T) {
	tests := []struct {
		name       string
		entrypoint uint64
		level      uint16
		wantSkip   bool
	}{
		{name: "valid", entrypoint: 5, level: 2, wantSkip: false},
		{name: "entrypoint exceeds maxNodeID", entrypoint: maxNodeID + 1, level: 2, wantSkip: true},
		{name: "level exceeds MaxLayerCount", entrypoint: 5, level: 500, wantSkip: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWALWriter(&buf)
			require.NoError(t, w.WriteSetEntryPointMaxLevel(tt.entrypoint, tt.level))

			logger := logrus.New()
			logger.SetLevel(logrus.FatalLevel)
			reader := NewWALCommitReader(&buf, logger)
			memReader := NewInMemoryReader(reader, logger)
			result, err := memReader.Do(nil, false)
			require.NoError(t, err)

			if tt.wantSkip {
				assert.Equal(t, uint64(0), result.Graph.Entrypoint)
				assert.Equal(t, uint16(0), result.Graph.Level)
				assert.False(t, result.Graph.EntrypointChanged)
			} else {
				assert.Equal(t, tt.entrypoint, result.Graph.Entrypoint)
				assert.Equal(t, tt.level, result.Graph.Level)
				assert.True(t, result.Graph.EntrypointChanged)
			}
		})
	}
}

func TestInMemoryReader_GarbageTargets(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	t.Run("AddLinkAtLevel skips garbage target", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWALWriter(&buf)
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinkAtLevel(1, 0, maxNodeID+1))

		reader := NewWALCommitReader(&buf, logger)
		memReader := NewInMemoryReader(reader, logger)
		result, err := memReader.Do(nil, false)
		require.NoError(t, err)
		require.NotNil(t, result.Graph.Nodes[1])
		assert.Equal(t, 0, result.Graph.Nodes[1].Connections.LenAtLayer(0))
	})

	t.Run("AddLinksAtLevel filters garbage targets", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWALWriter(&buf)
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteAddLinksAtLevel(1, 0, []uint64{2, maxNodeID + 1, 3}))

		reader := NewWALCommitReader(&buf, logger)
		memReader := NewInMemoryReader(reader, logger)
		result, err := memReader.Do(nil, false)
		require.NoError(t, err)
		require.NotNil(t, result.Graph.Nodes[1])
		conns := result.Graph.Nodes[1].Connections.GetLayer(0)
		assert.ElementsMatch(t, []uint64{2, 3}, conns)
	})

	t.Run("ReplaceLinksAtLevel filters garbage targets", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWALWriter(&buf)
		require.NoError(t, w.WriteAddNode(1, 0))
		require.NoError(t, w.WriteReplaceLinksAtLevel(1, 0, []uint64{maxNodeID + 1, 4, 5}))

		reader := NewWALCommitReader(&buf, logger)
		memReader := NewInMemoryReader(reader, logger)
		result, err := memReader.Do(nil, false)
		require.NoError(t, err)
		require.NotNil(t, result.Graph.Nodes[1])
		conns := result.Graph.Nodes[1].Connections.GetLayer(0)
		assert.ElementsMatch(t, []uint64{4, 5}, conns)
	})
}

func TestInMemoryReader_HasResetReflectsLastDoCall(t *testing.T) {
	var buf bytes.Buffer
	w := NewWALWriter(&buf)
	require.NoError(t, w.WriteResetIndex())
	require.NoError(t, w.WriteAddNode(1, 1))

	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	reader := NewWALCommitReader(&buf, logger)
	memReader := NewInMemoryReader(reader, logger)

	_, err := memReader.Do(nil, false)
	require.NoError(t, err)
	require.True(t, memReader.HasReset(), "first Do should report the reset it applied")

	_, err = memReader.Do(nil, false)
	require.NoError(t, err)
	require.False(t, memReader.HasReset(), "second Do should clear the previous reset state")
}

// TestInMemoryReader_DocIDReuse_ReAddClearsStaleState verifies that when a
// docID is re-added after a delete (docID reuse), the reader drops all stale
// per-id delete/tombstone state. Without this reconciliation, downstream
// consumers (SortedWriter) would classify the live re-added node as deleted
// and drop it from the condensed output.
func TestInMemoryReader_DocIDReuse_ReAddClearsStaleState(t *testing.T) {
	const id = uint64(5)

	tests := []struct {
		name      string
		write     func(t *testing.T, w *WALWriter)
		wantLevel int
	}{
		{
			name: "full lifecycle then re-add",
			write: func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddNode(id, 2))
				require.NoError(t, w.WriteAddTombstone(id))
				require.NoError(t, w.WriteDeleteNode(id))
				require.NoError(t, w.WriteRemoveTombstone(id))
				require.NoError(t, w.WriteAddNode(id, 1))
			},
			wantLevel: 1,
		},
		{
			// AddTombstone happened in an older log, so RemoveTombstone records
			// the asymmetric TombstonesDeleted bookkeeping. It must not survive
			// the re-add.
			name: "cleanup tail then re-add leaves no TombstonesDeleted",
			write: func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteDeleteNode(id))
				require.NoError(t, w.WriteRemoveTombstone(id))
				require.NoError(t, w.WriteAddNode(id, 1))
			},
			wantLevel: 1,
		},
		{
			// Tombstone still pending when the id is re-added. The stale
			// tombstone belongs to the previous life and must not survive.
			name: "re-add while old-life tombstone still present",
			write: func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddNode(id, 2))
				require.NoError(t, w.WriteAddTombstone(id))
				require.NoError(t, w.WriteDeleteNode(id))
				require.NoError(t, w.WriteAddNode(id, 1))
			},
			wantLevel: 1,
		},
		{
			name: "re-add followed by new-life links",
			write: func(t *testing.T, w *WALWriter) {
				require.NoError(t, w.WriteAddNode(1, 0))
				require.NoError(t, w.WriteAddNode(id, 1))
				require.NoError(t, w.WriteAddTombstone(id))
				require.NoError(t, w.WriteDeleteNode(id))
				require.NoError(t, w.WriteRemoveTombstone(id))
				require.NoError(t, w.WriteAddNode(id, 2))
				require.NoError(t, w.WriteReplaceLinksAtLevel(id, 0, []uint64{1}))
			},
			wantLevel: 2,
		},
	}

	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWALWriter(&buf)
			tt.write(t, w)

			reader := NewWALCommitReader(&buf, logger)
			memReader := NewInMemoryReader(reader, logger)
			result, err := memReader.Do(nil, true)
			require.NoError(t, err)

			require.NotNil(t, result.Graph.Nodes[id], "re-added node must be live")
			assert.Equal(t, tt.wantLevel, result.Graph.Nodes[id].Level, "level must come from the re-add")
			assert.NotContains(t, result.Graph.NodesDeleted, id, "stale NodesDeleted entry must be cleared on re-add")
			assert.NotContains(t, result.Graph.Tombstones, id, "stale Tombstones entry must be cleared on re-add")
			assert.NotContains(t, result.Graph.TombstonesDeleted, id, "stale TombstonesDeleted entry must be cleared on re-add")
		})
	}
}

// TestInMemoryReader_DeleteWithoutReAdd_KeepsDeleteState pins the counterpart:
// without a re-add, delete/tombstone bookkeeping must be preserved so that the
// SortedWriter still emits DeleteNode for the id.
func TestInMemoryReader_DeleteWithoutReAdd_KeepsDeleteState(t *testing.T) {
	const id = uint64(5)

	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	var buf bytes.Buffer
	w := NewWALWriter(&buf)
	require.NoError(t, w.WriteAddNode(id, 1))
	require.NoError(t, w.WriteAddTombstone(id))
	require.NoError(t, w.WriteDeleteNode(id))

	reader := NewWALCommitReader(&buf, logger)
	memReader := NewInMemoryReader(reader, logger)
	result, err := memReader.Do(nil, true)
	require.NoError(t, err)

	assert.Nil(t, result.Graph.Nodes[id])
	assert.Contains(t, result.Graph.NodesDeleted, id)
	assert.Contains(t, result.Graph.Tombstones, id)
}

func TestFilterValidTargets(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	tests := []struct {
		name   string
		input  []uint64
		expect []uint64
	}{
		{name: "all valid", input: []uint64{1, 2, 3}, expect: []uint64{1, 2, 3}},
		{name: "all garbage", input: []uint64{maxNodeID + 1, maxNodeID + 2}, expect: []uint64{}},
		{name: "mixed", input: []uint64{1, maxNodeID + 1, 2}, expect: []uint64{1, 2}},
		{name: "empty", input: []uint64{}, expect: []uint64{}},
		{name: "at boundary", input: []uint64{maxNodeID, maxNodeID + 1}, expect: []uint64{maxNodeID}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy since filterValidTargets modifies in place
			input := make([]uint64, len(tt.input))
			copy(input, tt.input)
			result := filterValidTargets(input, logger, 1, 0)
			assert.Equal(t, tt.expect, result)
		})
	}
}
