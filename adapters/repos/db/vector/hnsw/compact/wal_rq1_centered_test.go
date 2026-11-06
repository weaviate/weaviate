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
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestWALRoundTrip_AddRQCenteredBits1 pins two facts about the AddRQCentered
// record for the centered 1-bit quantizer:
//
//  1. The record itself is bits-agnostic: InputDim, Bits=1, the rotation and
//     the mean all survive the round trip with no bits=1-specific WAL code.
//  2. The record does NOT carry RQData.Rounding — the field is dropped on
//     write, by design: the centered 1-bit restore path regenerates the
//     identical rounding from DefaultFastRotationSeed (see
//     compressionhelpers.RestoreCenteredBinaryRotationalQuantizer and its
//     identity test TestRestoreCenteredRQ1Identity), so the record never
//     needs the field.
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

	// rounding deliberately does not survive: restore regenerates it from
	// DefaultFastRotationSeed instead
	assert.Empty(t, addRQ.Data.Rounding,
		"AddRQCentered must not persist rounding; the restore path regenerates it deterministically")
}

// TestSnapshotRoundTrip_RQCenteredBits1 is the snapshot counterpart of the WAL
// test above: the RQCentered snapshot record carries InputDim, Bits=1, the
// rotation and the mean, and no rounding (restore regenerates it).
func TestSnapshotRoundTrip_RQCenteredBits1(t *testing.T) {
	mean := []float32{0.5, -1.25, 3.75, 0}
	rqData := makeTestRQData(uint32(len(mean)), 1, 64, 2, mean)
	rqData.Rounding = []float32{0.1, 0.2, 0.3, 0.4} // not part of the record

	var buf bytes.Buffer
	w := &SnapshotWriter{rqData: rqData}
	require.NoError(t, w.writeRQData(&buf))
	require.Equal(t, byte(SnapshotCompressionTypeRQCentered), buf.Bytes()[0])

	res := &ent.DeserializationResult{}
	require.NoError(t, (&SnapshotReader{}).readCompressionData(bytes.NewReader(buf.Bytes()), res))
	restored := res.CompressionRQData()
	require.NotNil(t, restored)
	assert.Equal(t, uint32(1), restored.Bits, "bits=1 must survive the snapshot")
	assert.Equal(t, rqData.InputDim, restored.InputDim)
	assert.Equal(t, mean, restored.Mean, "the mean must survive the snapshot")
	assert.Equal(t, rqData.Rotation.OutputDim, restored.Rotation.OutputDim)
	assert.Equal(t, rqData.Rotation.Rounds, restored.Rotation.Rounds)
	assert.Equal(t, rqData.Rotation.Swaps, restored.Rotation.Swaps)
	assert.Equal(t, rqData.Rotation.Signs, restored.Rotation.Signs)
	assert.Empty(t, restored.Rounding, "the snapshot must not persist rounding")
}
