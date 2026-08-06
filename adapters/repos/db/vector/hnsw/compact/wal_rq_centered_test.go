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
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func makeTestRQData(inputDim, bits, outputDim, rounds uint32, mean []float32) *compression.RQData {
	data := &compression.RQData{
		InputDim: inputDim,
		Bits:     bits,
		Rotation: compression.FastRotation{
			OutputDim: outputDim,
			Rounds:    rounds,
			Swaps:     make([][]compression.Swap, rounds),
			Signs:     make([][]float32, rounds),
		},
		Mean: mean,
	}
	for i := uint32(0); i < rounds; i++ {
		data.Rotation.Swaps[i] = make([]compression.Swap, outputDim/2)
		for j := uint32(0); j < outputDim/2; j++ {
			data.Rotation.Swaps[i][j] = compression.Swap{I: uint16(j * 2), J: uint16(j*2 + 1)}
		}
		data.Rotation.Signs[i] = make([]float32, outputDim)
		for j := uint32(0); j < outputDim; j++ {
			data.Rotation.Signs[i][j] = float32(j) * 0.1
		}
	}
	return data
}

// A WriteAddRQ carrying a mean must round-trip through the AddRQCentered
// commit type, while nil/empty means keep the legacy AddRQ format.
func TestWALRoundTrip_AddRQCentered(t *testing.T) {
	testCases := []struct {
		name string
		mean []float32
	}{
		{"nil_mean_stays_legacy", nil},
		{"empty_mean_stays_legacy", []float32{}},
		{"centered", []float32{0.5, -1.25, 3.75, 0}},
		{"centered_single", []float32{42}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rqData := makeTestRQData(uint32(max(len(tc.mean), 1)), 4, 64, 2, tc.mean)

			var buf bytes.Buffer
			writer := NewWALWriter(&buf)
			require.NoError(t, writer.WriteAddRQ(rqData))

			// The first byte is the commit type: centered payloads must use the
			// new type so legacy WALs keep their exact format.
			wantType := AddRQ
			if len(tc.mean) > 0 {
				wantType = AddRQCentered
			}
			require.Equal(t, byte(wantType), buf.Bytes()[0])

			reader := NewWALCommitReader(&buf, testLogger())
			commit, err := reader.ReadNextCommit()
			require.NoError(t, err)

			addRQ, ok := commit.(*AddRQCommit)
			require.True(t, ok, "expected AddRQCommit, got %T", commit)
			require.NotNil(t, addRQ.Data)
			assert.Equal(t, rqData.InputDim, addRQ.Data.InputDim)
			assert.Equal(t, rqData.Bits, addRQ.Data.Bits)
			assert.Equal(t, rqData.Rotation.Signs, addRQ.Data.Rotation.Signs)
			if len(tc.mean) > 0 {
				assert.Equal(t, tc.mean, addRQ.Data.Mean)
			} else {
				assert.Nil(t, addRQ.Data.Mean)
			}
		})
	}
}

// A corrupted mean length must be rejected before allocation: the mean
// always has exactly InputDim entries, and a damaged record must not be able
// to request an arbitrarily large slice.
func TestWALRoundTrip_AddRQCenteredCorruptMeanLength(t *testing.T) {
	mean := []float32{1, 2, 3, 4}
	rqData := makeTestRQData(uint32(len(mean)), 4, 64, 2, mean)

	var buf bytes.Buffer
	require.NoError(t, NewWALWriter(&buf).WriteAddRQ(rqData))

	// meanLen sits after the header (17B) + swaps + signs.
	swapSize := int(rqData.Rotation.Rounds * (rqData.Rotation.OutputDim / 2) * 4)
	signSize := int(rqData.Rotation.Rounds * rqData.Rotation.OutputDim * 4)
	off := 17 + swapSize + signSize
	raw := buf.Bytes()
	raw[off], raw[off+1], raw[off+2], raw[off+3] = 0xff, 0xff, 0xff, 0x7f

	_, err := NewWALCommitReader(bytes.NewReader(raw), testLogger()).ReadNextCommit()
	require.ErrorContains(t, err, "mean length")
}

// Same guarantee for the snapshot encoding.
func TestSnapshotRoundTrip_RQCenteredCorruptMeanLength(t *testing.T) {
	mean := []float32{1, 2, 3, 4}
	rqData := makeTestRQData(uint32(len(mean)), 4, 64, 2, mean)

	var buf bytes.Buffer
	w := &SnapshotWriter{rqData: rqData}
	require.NoError(t, w.writeRQData(&buf))

	// meanLen sits after the type byte (1B) + four uint32 header fields
	// (16B) + swaps + signs.
	swapSize := int(rqData.Rotation.Rounds * (rqData.Rotation.OutputDim / 2) * 4)
	signSize := int(rqData.Rotation.Rounds * rqData.Rotation.OutputDim * 4)
	off := 1 + 16 + swapSize + signSize
	raw := buf.Bytes()
	raw[off], raw[off+1], raw[off+2], raw[off+3] = 0xff, 0xff, 0xff, 0x7f

	res := &ent.DeserializationResult{}
	err := (&SnapshotReader{}).readCompressionData(bytes.NewReader(raw), res)
	require.ErrorContains(t, err, "mean length")
}

// The snapshot format must carry the mean through its own (separate) binary
// encoding, again without touching the uncentered layout.
func TestSnapshotRoundTrip_RQCentered(t *testing.T) {
	for _, tc := range []struct {
		name string
		mean []float32
	}{
		{"uncentered", nil},
		{"centered", []float32{1.5, -0.5, 0.25, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rqData := makeTestRQData(uint32(max(len(tc.mean), 1)), 4, 64, 2, tc.mean)

			var buf bytes.Buffer
			w := &SnapshotWriter{rqData: rqData}
			require.NoError(t, w.writeRQData(&buf))

			wantType := byte(SnapshotCompressionTypeRQ)
			if len(tc.mean) > 0 {
				wantType = byte(SnapshotCompressionTypeRQCentered)
			}
			require.Equal(t, wantType, buf.Bytes()[0])

			res := &ent.DeserializationResult{}
			r := &SnapshotReader{}
			require.NoError(t, r.readCompressionData(bytes.NewReader(buf.Bytes()), res))
			restored := res.CompressionRQData()
			require.NotNil(t, restored)
			assert.Equal(t, rqData.InputDim, restored.InputDim)
			assert.Equal(t, rqData.Rotation.Signs, restored.Rotation.Signs)
			if len(tc.mean) > 0 {
				assert.Equal(t, tc.mean, restored.Mean)
			} else {
				assert.Nil(t, restored.Mean)
			}
		})
	}
}
