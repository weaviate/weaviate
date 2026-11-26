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

package compressionhelpers_test

import (
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestPQDistancerQueryDimensionMismatch(t *testing.T) {
	dimensions := 12
	vectors, _ := testinghelpers.RandomVecs(100, 1, dimensions)
	nullLogger, _ := logrustest.NewNullLogger()

	cfg := ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		Centroids: 16,
		Segments:  2,
	}
	pq, err := compressionhelpers.NewProductQuantizer(cfg, distancer.NewCosineDistanceProvider(), dimensions, nullLogger)
	require.NoError(t, err)
	require.NoError(t, pq.Fit(vectors))

	encoded := pq.Encode(vectors[0])

	tests := []struct {
		name     string
		queryLen int
		wantErr  bool
	}{
		{name: "query matches trained dimensions", queryLen: dimensions},
		{name: "query longer than trained dimensions", queryLen: dimensions + 4, wantErr: true},
		{name: "query shorter than trained dimensions", queryLen: dimensions - 4, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := make([]float32, tt.queryLen)
			for i := range query {
				query[i] = float32(i)
			}

			d := pq.NewDistancer(query)

			_, err := d.Distance(encoded)
			if tt.wantErr {
				assert.ErrorIs(t, err, distancer.ErrVectorLength)
			} else {
				assert.NoError(t, err)
			}

			_, err = d.DistanceToFloat(vectors[0])
			if tt.wantErr {
				assert.ErrorIs(t, err, distancer.ErrVectorLength)
			} else {
				assert.NoError(t, err)
			}

			pq.ReturnDistancer(d)
		})
	}
}

// TestPQRestoreWithShortKMeansEncoders pins that restoring PQ from k-means
// encoders with fewer centers than the configured centroids fails instead of
// panicking while building the distance table. Corrupt commit log data can
// decode as such encoders.
func TestPQRestoreWithShortKMeansEncoders(t *testing.T) {
	nullLogger, _ := logrustest.NewNullLogger()
	encoders := func(centers int) []compression.PQSegmentEncoder {
		out := make([]compression.PQSegmentEncoder, 0, 2)
		for s := 0; s < 2; s++ {
			c := make([][]float32, centers)
			for i := range c {
				c[i] = []float32{float32(i), float32(s)}
			}
			out = append(out, compressionhelpers.NewKMeansEncoderWithCenters(centers, 2, s, c))
		}
		return out
	}

	tests := []struct {
		name    string
		centers int
		wantErr bool
	}{
		{name: "no centers", centers: 0, wantErr: true},
		{name: "fewer centers than configured", centers: 1, wantErr: true},
		{name: "as many centers as configured", centers: 4},
		{name: "more centers than configured", centers: 8},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ent.PQConfig{
				Enabled:   true,
				Encoder:   ent.PQEncoder{Type: ent.PQEncoderTypeKMeans, Distribution: ent.DefaultPQEncoderDistribution},
				Centroids: 4,
			}
			require.NotPanics(t, func() {
				_, err := compressionhelpers.NewProductQuantizerWithEncoders(cfg, distancer.NewL2SquaredProvider(), 4, encoders(tc.centers), nullLogger)
				if tc.wantErr {
					require.ErrorContains(t, err, "centers")
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}
