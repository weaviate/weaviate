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

// The centering mean decides how every stored code is decoded, so it must
// round-trip through both persistence formats. Centered records carry a
// layout flags byte after the record type — currently always zero, reserved
// for the next lever that changes the code layout.
func TestRQCenteringRoundTrip(t *testing.T) {
	cases := []struct {
		name         string
		mean         []float32
		wantWALType  HnswCommitType
		wantSnapType byte
		wantFlags    []byte // empty for uncentered, which carries no flags byte
	}{
		{
			name:         "uncentered",
			wantWALType:  AddRQ,
			wantSnapType: byte(SnapshotCompressionTypeRQ),
		},
		{
			name:         "centered",
			mean:         []float32{0.5, -1.25, 3.75, 0},
			wantWALType:  AddRQCentered,
			wantSnapType: byte(SnapshotCompressionTypeRQCentered),
			wantFlags:    []byte{0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rqData := makeTestRQData(uint32(max(len(tc.mean), 1)), 4, 64, 2, tc.mean)

			t.Run("wal", func(t *testing.T) {
				var buf bytes.Buffer
				require.NoError(t, NewWALWriter(&buf).WriteAddRQ(rqData))
				require.Equal(t, byte(tc.wantWALType), buf.Bytes()[0])
				if len(tc.wantFlags) > 0 {
					require.Equal(t, tc.wantFlags[0], buf.Bytes()[1])
				}

				commit, err := NewWALCommitReader(&buf, testLogger()).ReadNextCommit()
				require.NoError(t, err)
				addRQ, ok := commit.(*AddRQCommit)
				require.True(t, ok, "expected AddRQCommit, got %T", commit)
				require.NotNil(t, addRQ.Data)
				assert.Equal(t, rqData.Rotation.Signs, addRQ.Data.Rotation.Signs)
				if len(tc.mean) > 0 {
					assert.Equal(t, tc.mean, addRQ.Data.Mean)
				} else {
					assert.Nil(t, addRQ.Data.Mean)
				}
			})

			t.Run("snapshot", func(t *testing.T) {
				var buf bytes.Buffer
				w := &SnapshotWriter{rqData: rqData}
				require.NoError(t, w.writeRQData(&buf))
				require.Equal(t, tc.wantSnapType, buf.Bytes()[0])
				if len(tc.wantFlags) > 0 {
					require.Equal(t, tc.wantFlags[0], buf.Bytes()[1])
				}

				res := &ent.DeserializationResult{}
				require.NoError(t, (&SnapshotReader{}).readCompressionData(bytes.NewReader(buf.Bytes()), res))
				restored := res.CompressionRQData()
				require.NotNil(t, restored)
				assert.Equal(t, rqData.Rotation.Signs, restored.Rotation.Signs)
				if len(tc.mean) > 0 {
					assert.Equal(t, tc.mean, restored.Mean)
				} else {
					assert.Nil(t, restored.Mean)
				}
			})
		})
	}
}

// The centering mean must survive a WAL -> snapshot conversion, the path a
// compaction takes: a snapshot that silently dropped it would restore a
// quantizer decoding every code against the uncentered layout, at the same
// length and without an error.
func TestRQCenteringSurvivesWALToSnapshot(t *testing.T) {
	mean := []float32{1, 2, 3, 4}
	rqData := makeTestRQData(uint32(len(mean)), 4, 64, 2, mean)

	var wal bytes.Buffer
	require.NoError(t, NewWALWriter(&wal).WriteAddRQ(rqData))
	commit, err := NewWALCommitReader(&wal, testLogger()).ReadNextCommit()
	require.NoError(t, err)
	fromWAL := commit.(*AddRQCommit).Data
	require.Equal(t, mean, fromWAL.Mean)

	var snap bytes.Buffer
	require.NoError(t, (&SnapshotWriter{rqData: fromWAL}).writeRQData(&snap))
	res := &ent.DeserializationResult{}
	require.NoError(t, (&SnapshotReader{}).readCompressionData(bytes.NewReader(snap.Bytes()), res))
	assert.Equal(t, mean, res.CompressionRQData().Mean)

	// And back out to a WAL, the shape compaction rewrites take.
	var rewritten bytes.Buffer
	require.NoError(t, NewWALWriter(&rewritten).WriteAddRQ(res.CompressionRQData()))
	assert.Equal(t, byte(AddRQCentered), rewritten.Bytes()[0])
	assert.Equal(t, byte(0), rewritten.Bytes()[1])
}

// A flag this binary does not implement changes the code length in a way it
// cannot reproduce, so both readers must refuse the record rather than
// restore a quantizer that misparses every vector. This is the loudness the
// centered record type used to provide by existing at all: released binaries
// stop at the unknown type, newer flags stop here.
func TestRQCenteredUnknownFlagsRejected(t *testing.T) {
	mean := []float32{1, 2, 3, 4}
	rqData := makeTestRQData(uint32(len(mean)), 4, 64, 2, mean)

	t.Run("wal", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, NewWALWriter(&buf).WriteAddRQ(rqData))
		record := buf.Bytes()
		record[1] = 0x80 // a flag from a future version

		_, err := NewWALCommitReader(bytes.NewReader(record), testLogger()).ReadNextCommit()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown bits")
	})

	t.Run("snapshot", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, (&SnapshotWriter{rqData: rqData}).writeRQData(&buf))
		record := buf.Bytes()
		record[1] = 0x80

		err := (&SnapshotReader{}).readCompressionData(bytes.NewReader(record), &ent.DeserializationResult{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown bits")
	})
}
