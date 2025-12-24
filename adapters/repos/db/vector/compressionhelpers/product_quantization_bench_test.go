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

package compressionhelpers_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func BenchmarkDistanceBetweenCompressedVectors(b *testing.B) {
	dimensions := 512
	centroids := 256
	segments := dimensions / 4

	distanceProvider := distancer.NewCosineDistanceProvider()

	cfg := ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		Centroids: centroids,
		Segments:  segments,
	}

	pq, err := compressionhelpers.NewProductQuantizer(
		cfg,
		distanceProvider,
		dimensions,
		logger,
	)
	if err != nil {
		b.Fatalf("Failed to create ProductQuantizer: %v", err)
	}

	// Generate training data and fit the quantizer
	vectors, _ := testinghelpers.RandomVecs(1000, 0, dimensions)
	if err := pq.Fit(vectors); err != nil {
		b.Fatalf("Failed to fit quantizer: %v", err)
	}

	// Generate test vectors
	x := pq.Encode(vectors[0])
	y := pq.Encode(vectors[1])

	// Warm up
	for i := 0; i < 10; i++ {
		_, err1 := pq.DistanceBetweenCompressedVectors(x, y)
		require.NoError(b, err1)
	}

	b.ResetTimer()

	b.Run("DistanceBetweenCompressedVectors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = pq.DistanceBetweenCompressedVectors(x, y)
		}
	})
}
