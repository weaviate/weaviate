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
