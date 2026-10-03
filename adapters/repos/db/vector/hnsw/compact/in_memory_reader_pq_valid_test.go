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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

func kmeansPQData(dims, m, ks uint16) *compression.PQData {
	segmentSize := int(dims / m)
	encoders := make([]compression.PQSegmentEncoder, m)
	for i := uint16(0); i < m; i++ {
		centers := make([][]float32, ks)
		for k := uint16(0); k < ks; k++ {
			center := make([]float32, segmentSize)
			for j := 0; j < segmentSize; j++ {
				center[j] = float32(k)*0.01 + float32(j)*0.001
			}
			centers[k] = center
		}
		encoders[i] = compressionhelpers.NewKMeansEncoderWithCenters(int(ks), segmentSize, int(i), centers)
	}
	return &compression.PQData{
		Dimensions:  dims,
		EncoderType: compression.UseKMeansEncoder,
		Ks:          ks,
		M:           m,
		Encoders:    encoders,
	}
}

func Test_InMemoryReader_SkipsIncompletePQCodebook(t *testing.T) {
	tests := []struct {
		name           string
		pqData         *compression.PQData
		wantCompressed bool
	}{
		{
			name:           "complete codebook enables compression",
			pqData:         kmeansPQData(128, 4, 256),
			wantCompressed: true,
		},
		{
			name:           "zero segments is skipped",
			pqData:         &compression.PQData{Dimensions: 8, EncoderType: compression.UseKMeansEncoder, Ks: 16, M: 0},
			wantCompressed: false,
		},
		{
			// dims not divisible by segments: the restored codebook would be
			// internally inconsistent, so compression must be skipped.
			name:           "dimensions not divisible by segments is skipped",
			pqData:         kmeansPQData(10, 4, 16),
			wantCompressed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Sanity-check the fixture actually models the intended (in)validity.
			if tt.wantCompressed {
				require.NoError(t, tt.pqData.Valid())
			} else {
				require.Error(t, tt.pqData.Valid())
			}

			var buf bytes.Buffer
			require.NoError(t, NewWALWriter(&buf).WriteAddPQ(tt.pqData))

			reader := NewWALCommitReader(&buf, testLogger())
			res, err := NewInMemoryReader(reader, testLogger()).Do(nil, false)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCompressed, res.Compressed(),
				"compressed state must reflect codebook validity")
			if tt.wantCompressed {
				require.NotNil(t, res.CompressionPQData())
			}
		})
	}
}
