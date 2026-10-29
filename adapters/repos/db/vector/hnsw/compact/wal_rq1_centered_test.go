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
)

// TestWALRoundTrip_AddRQCenteredBits1 pins two facts about the AddRQCentered
// record for the centered 1-bit quantizer:
//
//  1. The record itself is bits-agnostic: InputDim, Bits=1, the rotation and
//     the mean all survive the round trip with no bits=1-specific WAL code.
//  2. The record does NOT carry RQData.Rounding — the field is dropped on
//     write. The centered 1-bit query encoder needs it, which is why
//     RestoreRQCompressor rejects centered bits=1 (see
//     TestRestoreRQCompressorRejectsCenteredBits1) instead of restoring a
//     quantizer that would encode queries differently from the one that
//     built the graph. If rounding persistence is ever added, assertion 2
//     flips and the restore rejection can be lifted.
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

	// the load-bearing limitation: rounding does not survive
	assert.Empty(t, addRQ.Data.Rounding,
		"AddRQCentered does not persist rounding; if this starts passing rounding through, "+
			"lift the centered bits=1 restore rejection and delete this assertion")
}
