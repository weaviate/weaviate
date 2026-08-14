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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// TestWALRoundTrip_AddRQCenteredBits1 pins two facts about the AddRQCentered
// record for the centered 1-bit quantizer:
//
//  1. The record itself is bits-agnostic: InputDim, Bits=1, the rotation and
//     the mean all survive the round trip with no bits=1-specific WAL code.
//  2. The record does NOT carry RQData.Rounding — the field is dropped on
//     write. The centered 1-bit query encoder needs it, which is why
//     centered rq1 persists through the BRQ record family instead (see
//     TestWALRoundTrip_AddBRQCentered below), whose AddBRQCentered variant
//     carries rotation, rounding and mean together. The AddRQ record's
//     rounding drop remains pinned here so nobody re-routes rq1 through it
//     without noticing.
func TestWALRoundTrip_AddRQCenteredBits1(t *testing.T) {
	mean := []float32{0.5, -1.25, 3.75, 0}
	rqData := makeTestRQData(uint32(len(mean)), 1, 64, 2, mean)
	rqData.Rounding = []float32{0.1, 0.2, 0.3, 0.4} // dropped by the record, see above

	var buf bytes.Buffer
	writer := NewWALWriter(&buf)
	require.NoError(t, writer.WriteAddRQ(rqData))
	require.Equal(t, byte(AddRQCentered), buf.Bytes()[0])

	reader := NewWALCommitReader(&buf, testLogger())
	commit, err := reader.ReadNextCommit()
	require.NoError(t, err)

	addRQ, ok := commit.(*AddRQCommit)
	require.True(t, ok)
	assert.Equal(t, uint32(1), addRQ.Data.Bits, "bits=1 must survive the record")
	assert.Equal(t, rqData.InputDim, addRQ.Data.InputDim)
	assert.Equal(t, mean, addRQ.Data.Mean, "the mean must survive the record")
	assert.Equal(t, rqData.Rotation.OutputDim, addRQ.Data.Rotation.OutputDim)
	assert.Equal(t, rqData.Rotation.Swaps, addRQ.Data.Rotation.Swaps)
	assert.Equal(t, rqData.Rotation.Signs, addRQ.Data.Rotation.Signs)

	// the AddRQ family still drops rounding — which is fine for rq4/rq8
	// and precisely why centered rq1 persists via AddBRQCentered instead
	assert.Empty(t, addRQ.Data.Rounding,
		"AddRQCentered does not persist rounding; rq1 must keep using the BRQ record family")
}

// TestWALRoundTrip_AddBRQCentered pins the record that centered rq1
// actually persists through: the BRQ family (which already carries the
// query-encoder Rounding) extended with the mean and the centered layout
// flags byte, mirroring the AddRQ→AddRQCentered split. Uncentered BRQData
// keeps the legacy AddBRQ format byte-for-byte.
func TestWALRoundTrip_AddBRQCentered(t *testing.T) {
	mkBRQ := func(inputDim, outputDim, rounds uint32, mean []float32) *compression.BRQData {
		d := &compression.BRQData{
			InputDim: inputDim,
			Rotation: compression.FastRotation{
				OutputDim: outputDim,
				Rounds:    rounds,
				Swaps:     make([][]compression.Swap, rounds),
				Signs:     make([][]float32, rounds),
			},
			Rounding: make([]float32, outputDim),
			Mean:     mean,
		}
		for i := uint32(0); i < rounds; i++ {
			d.Rotation.Swaps[i] = make([]compression.Swap, outputDim/2)
			for j := uint32(0); j < outputDim/2; j++ {
				d.Rotation.Swaps[i][j] = compression.Swap{I: uint16(j * 2), J: uint16(j*2 + 1)}
			}
			d.Rotation.Signs[i] = make([]float32, outputDim)
		}
		for i := range d.Rounding {
			d.Rounding[i] = float32(i) * 0.01
		}
		return d
	}

	t.Run("centered round trip", func(t *testing.T) {
		mean := []float32{0.25, -0.5, 1.5, 0}
		data := mkBRQ(4, 64, 2, mean)
		var buf bytes.Buffer
		require.NoError(t, NewWALWriter(&buf).WriteAddBRQ(data))
		require.Equal(t, byte(AddBRQCentered), buf.Bytes()[0])
		commit, err := NewWALCommitReader(&buf, testLogger()).ReadNextCommit()
		require.NoError(t, err)
		got, ok := commit.(*AddBRQCommit)
		require.True(t, ok)
		assert.Equal(t, data.InputDim, got.Data.InputDim)
		assert.Equal(t, data.Rounding, got.Data.Rounding, "rounding must survive — it is why rq1 uses this record family")
		assert.Equal(t, mean, got.Data.Mean)
		assert.Equal(t, data.Rotation.Swaps, got.Data.Rotation.Swaps)
		assert.Equal(t, data.Rotation.Signs, got.Data.Rotation.Signs)
	})

	t.Run("uncentered stays legacy AddBRQ", func(t *testing.T) {
		data := mkBRQ(4, 64, 2, nil)
		var buf bytes.Buffer
		require.NoError(t, NewWALWriter(&buf).WriteAddBRQ(data))
		require.Equal(t, byte(AddBRQ), buf.Bytes()[0])
		commit, err := NewWALCommitReader(&buf, testLogger()).ReadNextCommit()
		require.NoError(t, err)
		got := commit.(*AddBRQCommit)
		assert.Empty(t, got.Data.Mean)
		assert.Equal(t, data.Rounding, got.Data.Rounding)
	})

	t.Run("unknown flags rejected", func(t *testing.T) {
		mean := []float32{1}
		data := mkBRQ(1, 64, 1, mean)
		var buf bytes.Buffer
		require.NoError(t, NewWALWriter(&buf).WriteAddBRQ(data))
		raw := buf.Bytes()
		raw[1] |= 0x40 // corrupt the flags byte with an undefined bit
		reader := NewWALCommitReader(bytes.NewBuffer(raw), testLogger())
		_, err := reader.ReadNextCommit()
		require.Error(t, err, "an unknown layout flag must refuse the record, not misparse codes")
	})
}
