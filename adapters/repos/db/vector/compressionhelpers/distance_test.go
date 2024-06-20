//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race

package compressionhelpers_test

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func BenchmarkDistancer(b *testing.B) {
	dims := 1536
	vecs, _ := testinghelpers.RandomVecs(2, 0, dims)
	distancer := distancer.NewDotProductProvider().New(vecs[0])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		distancer.Distance(vecs[1])
	}
}

func BenchmarkPQDistancer(b *testing.B) {
	dims := 1536
	vecs, _ := testinghelpers.RandomVecs(1000, 0, dims)
	pq, _ := compressionhelpers.NewProductQuantizer(hnsw.PQConfig{
		Enabled:        true,
		BitCompression: hnsw.DefaultPQBitCompression,
		Segments:       384,
		Centroids:      hnsw.DefaultPQCentroids,
		TrainingLimit:  1000,
		Encoder: hnsw.PQEncoder{
			Type:         hnsw.DefaultPQEncoderType,
			Distribution: hnsw.DefaultPQEncoderDistribution,
		},
	}, distancer.NewDotProductProvider(), dims, logrus.New())
	pq.Fit(vecs)
	distancer := pq.NewDistancer(vecs[0])
	compressed := pq.Encode(vecs[1])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		distancer.Distance(compressed)
	}
}
