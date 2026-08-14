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
