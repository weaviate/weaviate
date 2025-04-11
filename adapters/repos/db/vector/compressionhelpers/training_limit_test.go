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

package compressionhelpers

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func getRandomSeed() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func genVector(r *rand.Rand, dimensions int) []float32 {
	vector := make([]float32, 0, dimensions)
	for i := 0; i < dimensions; i++ {
		vector = append(vector, r.Float32()*2-1)
	}
	return vector
}

func RandomVecs(size int, queriesSize int, dimensions int) ([][]float32, [][]float32) {
	fmt.Printf("generating %d vectors...\n", size+queriesSize)
	r := getRandomSeed()
	vectors := make([][]float32, 0, size)
	queries := make([][]float32, 0, queriesSize)
	for i := 0; i < size; i++ {
		vectors = append(vectors, genVector(r, dimensions))
	}
	for i := 0; i < queriesSize; i++ {
		queries = append(queries, genVector(r, dimensions))
	}
	return vectors, queries
}

func Test_NoRacePQInvalidConfig(t *testing.T) {
	logger, _ := test.NewNullLogger()
	t.Run("validate training limit applied", func(t *testing.T) {
		amount := 64
		centroids := 256
		vectors_size := 400
		vectors, _ := RandomVecs(vectors_size, vectors_size, amount)
		distanceProvider := distancer.NewL2SquaredProvider()

		cfg := hnsw.PQConfig{
			Enabled: true,
			Encoder: hnsw.PQEncoder{
				Type:         hnsw.PQEncoderTypeKMeans,
				Distribution: hnsw.PQEncoderDistributionLogNormal,
			},
			Centroids:     centroids,
			TrainingLimit: 260,
			Segments:      amount,
		}
		pq, err := NewProductQuantizer(
			cfg,
			distanceProvider,
			amount,
			logger,
		)
		assert.NoError(t, err)
		pq.Fit(vectors)
		assert.Equal(t, pq.trainingLimit, 260)
	})
}
