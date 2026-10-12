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

package compressionhelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

func kmeansEncoder(ks, subDim, nilIdx int) compression.PQSegmentEncoder {
	centers := make([][]float32, ks)
	for i := range centers {
		if i == nilIdx {
			continue // leave nil
		}
		centers[i] = make([]float32, subDim)
	}
	return NewKMeansEncoderWithCenters(ks, subDim, 0, centers)
}

func segmentEncoders(m, ks, subDim int) []compression.PQSegmentEncoder {
	encs := make([]compression.PQSegmentEncoder, m)
	for i := range encs {
		encs[i] = kmeansEncoder(ks, subDim, -1)
	}
	return encs
}

func TestPQDataValid(t *testing.T) {
	// A well-formed codebook: 4 segments, 16 centroids each, 8 dims total
	// (subDim = 8/4 = 2).
	valid := func() *compression.PQData {
		return &compression.PQData{
			Ks:         16,
			M:          4,
			Dimensions: 8,
			Encoders:   segmentEncoders(4, 16, 2),
		}
	}

	tests := []struct {
		name    string
		data    *compression.PQData
		wantErr string
	}{
		{
			name: "valid",
			data: valid(),
		},
		{
			// Auto-segment (config Segments=0) resolves via
			// CalculateOptimalSegments, which for a dims divisible only by 2
			// returns dims/2. subDim = 12/6 = 2.
			name: "auto-segment M = dimensions/2",
			data: &compression.PQData{
				Ks: 16, M: 6, Dimensions: 12, Encoders: segmentEncoders(6, 16, 2),
			},
		},
		{
			// Auto-segment fallback for a dims with no small divisor returns
			// dims itself: M == Dimensions, subDim = 1. This must stay valid.
			name: "auto-segment fallback M == dimensions (subDim 1)",
			data: &compression.PQData{
				Ks: 16, M: 7, Dimensions: 7, Encoders: segmentEncoders(7, 16, 1),
			},
		},
		{
			name:    "nil pq data",
			data:    nil,
			wantErr: "pq data is nil",
		},
		{
			name: "zero segments",
			data: &compression.PQData{
				Ks: 16, M: 0, Dimensions: 8, Encoders: nil,
			},
			wantErr: "0 segments",
		},
		{
			name: "zero centroids",
			data: &compression.PQData{
				Ks: 0, M: 4, Dimensions: 8, Encoders: segmentEncoders(4, 16, 2),
			},
			wantErr: "0 centroids",
		},
		{
			name: "zero dimensions",
			data: &compression.PQData{
				Ks: 16, M: 4, Dimensions: 0, Encoders: segmentEncoders(4, 16, 2),
			},
			wantErr: "0 dimensions",
		},
		{
			name: "dimensions not divisible by segments",
			data: &compression.PQData{
				Ks: 16, M: 3, Dimensions: 8, Encoders: segmentEncoders(3, 16, 2),
			},
			wantErr: "not divisible",
		},
		{
			name: "too few encoders",
			data: &compression.PQData{
				Ks: 16, M: 4, Dimensions: 8, Encoders: segmentEncoders(3, 16, 2),
			},
			wantErr: "expected 4 (M)",
		},
		{
			name: "too many encoders",
			data: &compression.PQData{
				Ks: 16, M: 4, Dimensions: 8, Encoders: segmentEncoders(5, 16, 2),
			},
			wantErr: "expected 4 (M)",
		},
		{
			name: "nil encoder in slice",
			data: &compression.PQData{
				Ks: 16, M: 4, Dimensions: 8,
				Encoders: []compression.PQSegmentEncoder{
					kmeansEncoder(16, 2, -1),
					nil,
					kmeansEncoder(16, 2, -1),
					kmeansEncoder(16, 2, -1),
				},
			},
			wantErr: "segment encoder 1 is nil",
		},
		{
			name: "encoder with nil centroid",
			data: &compression.PQData{
				Ks: 16, M: 4, Dimensions: 8,
				Encoders: []compression.PQSegmentEncoder{
					kmeansEncoder(16, 2, -1),
					kmeansEncoder(16, 2, -1),
					kmeansEncoder(16, 2, 7), // centroid 7 is nil
					kmeansEncoder(16, 2, -1),
				},
			},
			wantErr: "segment encoder 2: centroid 7 has length 0, expected 2",
		},
		{
			name: "encoder with too few centroids",
			data: &compression.PQData{
				Ks: 16, M: 4, Dimensions: 8,
				Encoders: []compression.PQSegmentEncoder{
					kmeansEncoder(16, 2, -1),
					kmeansEncoder(8, 2, -1), // only 8 of the expected 16
					kmeansEncoder(16, 2, -1),
					kmeansEncoder(16, 2, -1),
				},
			},
			wantErr: "segment encoder 1: has 8 centroids, expected 16",
		},
		{
			name: "encoder with wrong centroid dimension",
			data: &compression.PQData{
				Ks: 16, M: 4, Dimensions: 8,
				Encoders: []compression.PQSegmentEncoder{
					kmeansEncoder(16, 3, -1), // subDim should be 2
					kmeansEncoder(16, 2, -1),
					kmeansEncoder(16, 2, -1),
					kmeansEncoder(16, 2, -1),
				},
			},
			wantErr: "segment encoder 0: centroid 0 has length 3, expected 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.data.Valid()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
