//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compactv2

import (
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

func testLogger() logrus.FieldLogger {
	return logrus.New()
}

func TestWALRoundTrip_AddNode(t *testing.T) {
	testCases := []struct {
		name  string
		id    uint64
		level uint16
	}{
		{"basic", 42, 3},
		{"zero", 0, 0},
		{"max_uint64", math.MaxUint64, math.MaxUint16},
		{"large_id_small_level", 1<<48 - 1, 1},
		{"small_id_large_level", 1, math.MaxUint16 - 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddNode(tc.id, tc.level)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addNode, ok := commit.(*AddNodeCommit)
			require.True(t, ok, "expected AddNodeCommit, got %T", commit)
			assert.Equal(t, tc.id, addNode.ID)
			assert.Equal(t, tc.level, addNode.Level)

			// Verify EOF after single commit
			_, err = reader.ReadNextCommit()
			assert.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestWALRoundTrip_DeleteNode(t *testing.T) {
	testCases := []struct {
		name string
		id   uint64
	}{
		{"basic", 42},
		{"zero", 0},
		{"max", math.MaxUint64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteDeleteNode(tc.id)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			deleteNode, ok := commit.(*DeleteNodeCommit)
			require.True(t, ok, "expected DeleteNodeCommit, got %T", commit)
			assert.Equal(t, tc.id, deleteNode.ID)
		})
	}
}

func TestWALRoundTrip_SetEntryPointMaxLevel(t *testing.T) {
	testCases := []struct {
		name       string
		entrypoint uint64
		level      uint16
	}{
		{"basic", 100, 5},
		{"zero", 0, 0},
		{"max", math.MaxUint64, math.MaxUint16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteSetEntryPointMaxLevel(tc.entrypoint, tc.level)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			setEP, ok := commit.(*SetEntryPointMaxLevelCommit)
			require.True(t, ok, "expected SetEntryPointMaxLevelCommit, got %T", commit)
			assert.Equal(t, tc.entrypoint, setEP.Entrypoint)
			assert.Equal(t, tc.level, setEP.Level)
		})
	}
}

func TestWALRoundTrip_AddLinkAtLevel(t *testing.T) {
	testCases := []struct {
		name   string
		source uint64
		level  uint16
		target uint64
	}{
		{"basic", 10, 2, 20},
		{"zeros", 0, 0, 0},
		{"max_values", math.MaxUint64, math.MaxUint16, math.MaxUint64},
		{"same_source_target", 42, 1, 42},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddLinkAtLevel(tc.source, tc.level, tc.target)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addLink, ok := commit.(*AddLinkAtLevelCommit)
			require.True(t, ok, "expected AddLinkAtLevelCommit, got %T", commit)
			assert.Equal(t, tc.source, addLink.Source)
			assert.Equal(t, tc.level, addLink.Level)
			assert.Equal(t, tc.target, addLink.Target)
		})
	}
}

func TestWALRoundTrip_AddLinksAtLevel(t *testing.T) {
	testCases := []struct {
		name    string
		source  uint64
		level   uint16
		targets []uint64
	}{
		{"single_target", 10, 1, []uint64{20}},
		{"multiple_targets", 10, 2, []uint64{20, 30, 40, 50}},
		{"empty_targets", 10, 0, []uint64{}},
		{"max_values", math.MaxUint64, math.MaxUint16, []uint64{math.MaxUint64, math.MaxUint64 - 1}},
		{"many_targets", 5, 0, func() []uint64 {
			targets := make([]uint64, 100)
			for i := range targets {
				targets[i] = uint64(i * 10)
			}
			return targets
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddLinksAtLevel(tc.source, tc.level, tc.targets)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addLinks, ok := commit.(*AddLinksAtLevelCommit)
			require.True(t, ok, "expected AddLinksAtLevelCommit, got %T", commit)
			assert.Equal(t, tc.source, addLinks.Source)
			assert.Equal(t, tc.level, addLinks.Level)
			assert.Equal(t, tc.targets, addLinks.Targets)
		})
	}
}

func TestWALRoundTrip_ReplaceLinksAtLevel(t *testing.T) {
	testCases := []struct {
		name    string
		source  uint64
		level   uint16
		targets []uint64
	}{
		{"single_target", 10, 1, []uint64{20}},
		{"multiple_targets", 10, 2, []uint64{20, 30, 40}},
		{"empty_targets", 10, 0, []uint64{}},
		{"many_targets", 5, 0, func() []uint64 {
			targets := make([]uint64, 50)
			for i := range targets {
				targets[i] = uint64(i)
			}
			return targets
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteReplaceLinksAtLevel(tc.source, tc.level, tc.targets)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			replaceLinks, ok := commit.(*ReplaceLinksAtLevelCommit)
			require.True(t, ok, "expected ReplaceLinksAtLevelCommit, got %T", commit)
			assert.Equal(t, tc.source, replaceLinks.Source)
			assert.Equal(t, tc.level, replaceLinks.Level)
			assert.Equal(t, tc.targets, replaceLinks.Targets)
		})
	}
}

func TestWALRoundTrip_ClearLinks(t *testing.T) {
	testCases := []struct {
		name string
		id   uint64
	}{
		{"basic", 42},
		{"zero", 0},
		{"max", math.MaxUint64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteClearLinks(tc.id)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			clearLinks, ok := commit.(*ClearLinksCommit)
			require.True(t, ok, "expected ClearLinksCommit, got %T", commit)
			assert.Equal(t, tc.id, clearLinks.ID)
		})
	}
}

func TestWALRoundTrip_ClearLinksAtLevel(t *testing.T) {
	testCases := []struct {
		name  string
		id    uint64
		level uint16
	}{
		{"basic", 42, 3},
		{"zero", 0, 0},
		{"max", math.MaxUint64, math.MaxUint16},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteClearLinksAtLevel(tc.id, tc.level)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			clearLinksAtLevel, ok := commit.(*ClearLinksAtLevelCommit)
			require.True(t, ok, "expected ClearLinksAtLevelCommit, got %T", commit)
			assert.Equal(t, tc.id, clearLinksAtLevel.ID)
			assert.Equal(t, tc.level, clearLinksAtLevel.Level)
		})
	}
}

func TestWALRoundTrip_AddTombstone(t *testing.T) {
	testCases := []struct {
		name string
		id   uint64
	}{
		{"basic", 42},
		{"zero", 0},
		{"max", math.MaxUint64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddTombstone(tc.id)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addTombstone, ok := commit.(*AddTombstoneCommit)
			require.True(t, ok, "expected AddTombstoneCommit, got %T", commit)
			assert.Equal(t, tc.id, addTombstone.ID)
		})
	}
}

func TestWALRoundTrip_RemoveTombstone(t *testing.T) {
	testCases := []struct {
		name string
		id   uint64
	}{
		{"basic", 42},
		{"zero", 0},
		{"max", math.MaxUint64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteRemoveTombstone(tc.id)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			removeTombstone, ok := commit.(*RemoveTombstoneCommit)
			require.True(t, ok, "expected RemoveTombstoneCommit, got %T", commit)
			assert.Equal(t, tc.id, removeTombstone.ID)
		})
	}
}

func TestWALRoundTrip_ResetIndex(t *testing.T) {
	var buf bytes.Buffer
	writer := NewWALWriter(&buf)
	err := writer.WriteResetIndex()
	require.NoError(t, err)

	reader := NewWALCommitReader(&buf, testLogger())
	commit, err := reader.ReadNextCommit()
	require.NoError(t, err)

	_, ok := commit.(*ResetIndexCommit)
	require.True(t, ok, "expected ResetIndexCommit, got %T", commit)
}

func TestWALRoundTrip_AddPQ_TileEncoder(t *testing.T) {
	// Create PQ data with TileEncoder
	pqData := &compression.PQData{
		Dimensions:          128,
		EncoderType:         compression.UseTileEncoder,
		Ks:                  256,
		M:                   4,
		EncoderDistribution: 1,
		UseBitsEncoding:     true,
		Encoders:            make([]compression.PQSegmentEncoder, 4),
	}

	// Create tile encoders for each segment
	for i := uint16(0); i < pqData.M; i++ {
		pqData.Encoders[i] = compressionhelpers.RestoreTileEncoder(
			256.0,                      // bins
			0.5,                        // mean
			1.0,                        // stdDev
			1000.0,                     // size
			0.1,                        // s1
			0.9,                        // s2
			i,                          // segment
			pqData.EncoderDistribution, // encDistribution
		)
	}

	var buf bytes.Buffer
	writer := NewWALWriter(&buf)
	err := writer.WriteAddPQ(pqData)
	require.NoError(t, err)

	reader := NewWALCommitReader(&buf, testLogger())
	commit, err := reader.ReadNextCommit()
	require.NoError(t, err)

	addPQ, ok := commit.(*AddPQCommit)
	require.True(t, ok, "expected AddPQCommit, got %T", commit)
	require.NotNil(t, addPQ.Data)

	assert.Equal(t, pqData.Dimensions, addPQ.Data.Dimensions)
	assert.Equal(t, pqData.EncoderType, addPQ.Data.EncoderType)
	assert.Equal(t, pqData.Ks, addPQ.Data.Ks)
	assert.Equal(t, pqData.M, addPQ.Data.M)
	assert.Equal(t, pqData.EncoderDistribution, addPQ.Data.EncoderDistribution)
	assert.Equal(t, pqData.UseBitsEncoding, addPQ.Data.UseBitsEncoding)
	assert.Equal(t, len(pqData.Encoders), len(addPQ.Data.Encoders))
}

func TestWALRoundTrip_AddPQ_KMeansEncoder(t *testing.T) {
	dimensions := uint16(128)
	m := uint16(4)
	ks := uint16(256)
	segmentSize := int(dimensions / m)

	pqData := &compression.PQData{
		Dimensions:          dimensions,
		EncoderType:         compression.UseKMeansEncoder,
		Ks:                  ks,
		M:                   m,
		EncoderDistribution: 0,
		UseBitsEncoding:     false,
		Encoders:            make([]compression.PQSegmentEncoder, m),
	}

	// Create KMeans encoders for each segment
	for i := uint16(0); i < m; i++ {
		centers := make([][]float32, ks)
		for k := uint16(0); k < ks; k++ {
			center := make([]float32, segmentSize)
			for j := 0; j < segmentSize; j++ {
				center[j] = float32(k)*0.01 + float32(j)*0.001
			}
			centers[k] = center
		}
		pqData.Encoders[i] = compressionhelpers.NewKMeansEncoderWithCenters(
			int(ks),
			segmentSize,
			int(i),
			centers,
		)
	}

	var buf bytes.Buffer
	writer := NewWALWriter(&buf)
	err := writer.WriteAddPQ(pqData)
	require.NoError(t, err)

	reader := NewWALCommitReader(&buf, testLogger())
	commit, err := reader.ReadNextCommit()
	require.NoError(t, err)

	addPQ, ok := commit.(*AddPQCommit)
	require.True(t, ok, "expected AddPQCommit, got %T", commit)
	require.NotNil(t, addPQ.Data)

	assert.Equal(t, pqData.Dimensions, addPQ.Data.Dimensions)
	assert.Equal(t, pqData.EncoderType, addPQ.Data.EncoderType)
	assert.Equal(t, pqData.Ks, addPQ.Data.Ks)
	assert.Equal(t, pqData.M, addPQ.Data.M)
	assert.Equal(t, len(pqData.Encoders), len(addPQ.Data.Encoders))
}

func TestWALRoundTrip_AddSQ(t *testing.T) {
	testCases := []struct {
		name       string
		a          float32
		b          float32
		dimensions uint16
	}{
		{"basic", 0.5, 1.5, 128},
		{"zero_values", 0.0, 0.0, 0},
		{"negative", -1.5, -0.5, 256},
		{"large_dimensions", 0.25, 0.75, 4096},
		{"special_floats", math.SmallestNonzeroFloat32, math.MaxFloat32, 64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqData := &compression.SQData{
				A:          tc.a,
				B:          tc.b,
				Dimensions: tc.dimensions,
			}

			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddSQ(sqData)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addSQ, ok := commit.(*AddSQCommit)
			require.True(t, ok, "expected AddSQCommit, got %T", commit)
			require.NotNil(t, addSQ.Data)

			assert.Equal(t, tc.a, addSQ.Data.A)
			assert.Equal(t, tc.b, addSQ.Data.B)
			assert.Equal(t, tc.dimensions, addSQ.Data.Dimensions)
		})
	}
}

func TestWALRoundTrip_AddRQ(t *testing.T) {
	testCases := []struct {
		name      string
		inputDim  uint32
		bits      uint32
		outputDim uint32
		rounds    uint32
	}{
		{"basic", 128, 4, 64, 2},
		{"single_round", 256, 8, 128, 1},
		{"many_rounds", 64, 2, 32, 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rqData := &compression.RQData{
				InputDim: tc.inputDim,
				Bits:     tc.bits,
				Rotation: compression.FastRotation{
					OutputDim: tc.outputDim,
					Rounds:    tc.rounds,
					Swaps:     make([][]compression.Swap, tc.rounds),
					Signs:     make([][]float32, tc.rounds),
				},
			}

			// Populate swaps and signs
			for i := uint32(0); i < tc.rounds; i++ {
				rqData.Rotation.Swaps[i] = make([]compression.Swap, tc.outputDim/2)
				for j := uint32(0); j < tc.outputDim/2; j++ {
					rqData.Rotation.Swaps[i][j] = compression.Swap{
						I: uint16(j * 2),
						J: uint16(j*2 + 1),
					}
				}
				rqData.Rotation.Signs[i] = make([]float32, tc.outputDim)
				for j := uint32(0); j < tc.outputDim; j++ {
					rqData.Rotation.Signs[i][j] = float32(j) * 0.1
				}
			}

			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddRQ(rqData)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addRQ, ok := commit.(*AddRQCommit)
			require.True(t, ok, "expected AddRQCommit, got %T", commit)
			require.NotNil(t, addRQ.Data)

			assert.Equal(t, tc.inputDim, addRQ.Data.InputDim)
			assert.Equal(t, tc.bits, addRQ.Data.Bits)
			assert.Equal(t, tc.outputDim, addRQ.Data.Rotation.OutputDim)
			assert.Equal(t, tc.rounds, addRQ.Data.Rotation.Rounds)

			// Verify swaps
			require.Equal(t, len(rqData.Rotation.Swaps), len(addRQ.Data.Rotation.Swaps))
			for i := range rqData.Rotation.Swaps {
				require.Equal(t, len(rqData.Rotation.Swaps[i]), len(addRQ.Data.Rotation.Swaps[i]))
				for j := range rqData.Rotation.Swaps[i] {
					assert.Equal(t, rqData.Rotation.Swaps[i][j].I, addRQ.Data.Rotation.Swaps[i][j].I)
					assert.Equal(t, rqData.Rotation.Swaps[i][j].J, addRQ.Data.Rotation.Swaps[i][j].J)
				}
			}

			// Verify signs
			require.Equal(t, len(rqData.Rotation.Signs), len(addRQ.Data.Rotation.Signs))
			for i := range rqData.Rotation.Signs {
				assert.Equal(t, rqData.Rotation.Signs[i], addRQ.Data.Rotation.Signs[i])
			}
		})
	}
}

func TestWALRoundTrip_AddBRQ(t *testing.T) {
	testCases := []struct {
		name      string
		inputDim  uint32
		outputDim uint32
		rounds    uint32
	}{
		{"basic", 128, 64, 2},
		{"single_round", 256, 128, 1},
		{"many_rounds", 64, 32, 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			brqData := &compression.BRQData{
				InputDim: tc.inputDim,
				Rotation: compression.FastRotation{
					OutputDim: tc.outputDim,
					Rounds:    tc.rounds,
					Swaps:     make([][]compression.Swap, tc.rounds),
					Signs:     make([][]float32, tc.rounds),
				},
				Rounding: make([]float32, tc.outputDim),
			}

			// Populate swaps and signs
			for i := uint32(0); i < tc.rounds; i++ {
				brqData.Rotation.Swaps[i] = make([]compression.Swap, tc.outputDim/2)
				for j := uint32(0); j < tc.outputDim/2; j++ {
					brqData.Rotation.Swaps[i][j] = compression.Swap{
						I: uint16(j * 2),
						J: uint16(j*2 + 1),
					}
				}
				brqData.Rotation.Signs[i] = make([]float32, tc.outputDim)
				for j := uint32(0); j < tc.outputDim; j++ {
					brqData.Rotation.Signs[i][j] = float32(j) * 0.05
				}
			}

			// Populate rounding
			for i := uint32(0); i < tc.outputDim; i++ {
				brqData.Rounding[i] = float32(i) * 0.01
			}

			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddBRQ(brqData)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addBRQ, ok := commit.(*AddBRQCommit)
			require.True(t, ok, "expected AddBRQCommit, got %T", commit)
			require.NotNil(t, addBRQ.Data)

			assert.Equal(t, tc.inputDim, addBRQ.Data.InputDim)
			assert.Equal(t, tc.outputDim, addBRQ.Data.Rotation.OutputDim)
			assert.Equal(t, tc.rounds, addBRQ.Data.Rotation.Rounds)

			// Verify swaps
			require.Equal(t, len(brqData.Rotation.Swaps), len(addBRQ.Data.Rotation.Swaps))
			for i := range brqData.Rotation.Swaps {
				require.Equal(t, len(brqData.Rotation.Swaps[i]), len(addBRQ.Data.Rotation.Swaps[i]))
				for j := range brqData.Rotation.Swaps[i] {
					assert.Equal(t, brqData.Rotation.Swaps[i][j].I, addBRQ.Data.Rotation.Swaps[i][j].I)
					assert.Equal(t, brqData.Rotation.Swaps[i][j].J, addBRQ.Data.Rotation.Swaps[i][j].J)
				}
			}

			// Verify signs
			require.Equal(t, len(brqData.Rotation.Signs), len(addBRQ.Data.Rotation.Signs))
			for i := range brqData.Rotation.Signs {
				assert.Equal(t, brqData.Rotation.Signs[i], addBRQ.Data.Rotation.Signs[i])
			}

			// Verify rounding
			assert.Equal(t, brqData.Rounding, addBRQ.Data.Rounding)
		})
	}
}

func TestWALRoundTrip_AddMuvera(t *testing.T) {
	testCases := []struct {
		name         string
		kSim         uint32
		numClusters  uint32
		dimensions   uint32
		dProjections uint32
		repetitions  uint32
	}{
		{"basic", 2, 4, 8, 2, 2},
		{"single", 1, 1, 4, 1, 1},
		{"larger", 4, 8, 16, 4, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			muveraData := &multivector.MuveraData{
				KSim:         tc.kSim,
				NumClusters:  tc.numClusters,
				Dimensions:   tc.dimensions,
				DProjections: tc.dProjections,
				Repetitions:  tc.repetitions,
				Gaussians:    make([][][]float32, tc.repetitions),
				S:            make([][][]float32, tc.repetitions),
			}

			// Populate Gaussians
			for i := uint32(0); i < tc.repetitions; i++ {
				muveraData.Gaussians[i] = make([][]float32, tc.kSim)
				for j := uint32(0); j < tc.kSim; j++ {
					muveraData.Gaussians[i][j] = make([]float32, tc.dimensions)
					for k := uint32(0); k < tc.dimensions; k++ {
						muveraData.Gaussians[i][j][k] = float32(i*100+j*10) + float32(k)*0.1
					}
				}
			}

			// Populate S matrices
			for i := uint32(0); i < tc.repetitions; i++ {
				muveraData.S[i] = make([][]float32, tc.dProjections)
				for j := uint32(0); j < tc.dProjections; j++ {
					muveraData.S[i][j] = make([]float32, tc.dimensions)
					for k := uint32(0); k < tc.dimensions; k++ {
						muveraData.S[i][j][k] = float32(i*200+j*20) + float32(k)*0.2
					}
				}
			}

			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			err := writer.WriteAddMuvera(muveraData)
			require.NoError(t, err)

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addMuvera, ok := commit.(*AddMuveraCommit)
			require.True(t, ok, "expected AddMuveraCommit, got %T", commit)
			require.NotNil(t, addMuvera.Data)

			assert.Equal(t, tc.kSim, addMuvera.Data.KSim)
			assert.Equal(t, tc.numClusters, addMuvera.Data.NumClusters)
			assert.Equal(t, tc.dimensions, addMuvera.Data.Dimensions)
			assert.Equal(t, tc.dProjections, addMuvera.Data.DProjections)
			assert.Equal(t, tc.repetitions, addMuvera.Data.Repetitions)

			// Verify Gaussians
			require.Equal(t, len(muveraData.Gaussians), len(addMuvera.Data.Gaussians))
			for i := range muveraData.Gaussians {
				require.Equal(t, len(muveraData.Gaussians[i]), len(addMuvera.Data.Gaussians[i]))
				for j := range muveraData.Gaussians[i] {
					assert.Equal(t, muveraData.Gaussians[i][j], addMuvera.Data.Gaussians[i][j])
				}
			}

			// Verify S matrices
			require.Equal(t, len(muveraData.S), len(addMuvera.Data.S))
			for i := range muveraData.S {
				require.Equal(t, len(muveraData.S[i]), len(addMuvera.Data.S[i]))
				for j := range muveraData.S[i] {
					assert.Equal(t, muveraData.S[i][j], addMuvera.Data.S[i][j])
				}
			}
		})
	}
}

func TestWALRoundTrip_MultipleSequentialCommits(t *testing.T) {
	var buf bytes.Buffer
	writer := NewWALWriter(&buf)

	// Write various commits
	require.NoError(t, writer.WriteAddNode(1, 2))
	require.NoError(t, writer.WriteSetEntryPointMaxLevel(1, 2))
	require.NoError(t, writer.WriteAddLinkAtLevel(1, 0, 2))
	require.NoError(t, writer.WriteAddLinksAtLevel(1, 1, []uint64{3, 4, 5}))
	require.NoError(t, writer.WriteReplaceLinksAtLevel(2, 0, []uint64{1}))
	require.NoError(t, writer.WriteAddTombstone(3))
	require.NoError(t, writer.WriteRemoveTombstone(3))
	require.NoError(t, writer.WriteDeleteNode(3))
	require.NoError(t, writer.WriteClearLinksAtLevel(1, 0))
	require.NoError(t, writer.WriteClearLinks(2))
	require.NoError(t, writer.WriteResetIndex())

	// Read and verify all commits in order
	reader := NewWALCommitReader(&buf, testLogger())

	// AddNode
	commit, err := reader.ReadNextCommit()
	require.NoError(t, err)
	addNode, ok := commit.(*AddNodeCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(1), addNode.ID)
	assert.Equal(t, uint16(2), addNode.Level)

	// SetEntryPointMaxLevel
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	setEP, ok := commit.(*SetEntryPointMaxLevelCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(1), setEP.Entrypoint)
	assert.Equal(t, uint16(2), setEP.Level)

	// AddLinkAtLevel
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	addLink, ok := commit.(*AddLinkAtLevelCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(1), addLink.Source)
	assert.Equal(t, uint16(0), addLink.Level)
	assert.Equal(t, uint64(2), addLink.Target)

	// AddLinksAtLevel
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	addLinks, ok := commit.(*AddLinksAtLevelCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(1), addLinks.Source)
	assert.Equal(t, uint16(1), addLinks.Level)
	assert.Equal(t, []uint64{3, 4, 5}, addLinks.Targets)

	// ReplaceLinksAtLevel
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	replaceLinks, ok := commit.(*ReplaceLinksAtLevelCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(2), replaceLinks.Source)
	assert.Equal(t, uint16(0), replaceLinks.Level)
	assert.Equal(t, []uint64{1}, replaceLinks.Targets)

	// AddTombstone
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	addTombstone, ok := commit.(*AddTombstoneCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(3), addTombstone.ID)

	// RemoveTombstone
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	removeTombstone, ok := commit.(*RemoveTombstoneCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(3), removeTombstone.ID)

	// DeleteNode
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	deleteNode, ok := commit.(*DeleteNodeCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(3), deleteNode.ID)

	// ClearLinksAtLevel
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	clearLinksAtLevel, ok := commit.(*ClearLinksAtLevelCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(1), clearLinksAtLevel.ID)
	assert.Equal(t, uint16(0), clearLinksAtLevel.Level)

	// ClearLinks
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	clearLinks, ok := commit.(*ClearLinksCommit)
	require.True(t, ok)
	assert.Equal(t, uint64(2), clearLinks.ID)

	// ResetIndex
	commit, err = reader.ReadNextCommit()
	require.NoError(t, err)
	_, ok = commit.(*ResetIndexCommit)
	require.True(t, ok)

	// EOF
	_, err = reader.ReadNextCommit()
	assert.ErrorIs(t, err, io.EOF)
}

func TestWALRoundTrip_ReplaceLinksAtLevel_MaxUint16Truncation(t *testing.T) {
	// Test that targets longer than MaxUint16 are truncated by the writer
	targets := make([]uint64, math.MaxUint16+100)
	for i := range targets {
		targets[i] = uint64(i)
	}

	var buf bytes.Buffer
	writer := NewWALWriter(&buf)
	err := writer.WriteReplaceLinksAtLevel(1, 0, targets)
	require.NoError(t, err)

	reader := NewWALCommitReader(&buf, testLogger())
	commit, err := reader.ReadNextCommit()
	require.NoError(t, err)

	replaceLinks, ok := commit.(*ReplaceLinksAtLevelCommit)
	require.True(t, ok)
	// Writer truncates to MaxUint16, reader further truncates to maxConnectionsPerNodeReader (4096)
	// So we check that the length is the reader's max (4096)
	assert.Equal(t, maxConnectionsPerNodeReader, len(replaceLinks.Targets))
}
