//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race
// +build !race

package ssdhelpers_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

type IndexAndDistance struct {
	index    uint64
	distance float32
}

func distance(dp distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := dp.SingleDist(x, y)
		return dist
	}
}

func Test_NoRacePQKMeans(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 1000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, int(dimensions))
	distanceProvider := distancer.NewL2SquaredProvider()

	pq, _ := ssdhelpers.NewProductQuantizer(
		dimensions,
		512,
		false,
		distanceProvider,
		dimensions,
		ssdhelpers.UseKMeansEncoder,
		ssdhelpers.LogNormalEncoderDistribution,
	)
	pq.Fit(vectors)
	encoded := make([][]byte, vectors_size)
	for i := 0; i < vectors_size; i++ {
		encoded[i] = pq.Encode(vectors[i])
	}

	var relevant uint64
	queries_size = 100
	for _, query := range queries {
		truth := testinghelpers.BruteForce(vectors, query, k, distance(distanceProvider))
		distances := make([]IndexAndDistance, len(vectors))

		lut := pq.CenterAt(query)
		for v := range vectors {
			distances[v] = IndexAndDistance{index: uint64(v), distance: pq.Distance(encoded[v], lut)}
		}
		sort.Slice(distances, func(a, b int) bool {
			return distances[a].distance < distances[b].distance
		})

		results := make([]uint64, 0, k)
		for i := 0; i < k; i++ {
			results = append(results, distances[i].index)
		}
		relevant += testinghelpers.MatchesInLists(truth, results)
	}
	recall := float32(relevant) / float32(k*queries_size)
	fmt.Println(recall)
	assert.True(t, recall > 0.99)
}

func Test_NoRacePQDecodeBits(t *testing.T) {
	t.Run("extracts correctly on one code per byte", func(t *testing.T) {
		amount := 100
		centroids := 256
		values := make([]byte, 0, amount)
		for i := byte(0); i < byte(amount); i++ {
			values = append(values, i)
		}
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			true,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("extracts correctly on 6 bits", func(t *testing.T) {
		amount := 8
		centroids := 64
		values := []byte{0, 16, 131, 16, 81, 135, 0}

		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			true,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("extracts correctly on 12 bits", func(t *testing.T) {
		amount := 4
		centroids := 4096
		values := []byte{0, 0, 1, 0, 32, 3, 0, 0}

		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			true,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("extracts correctly on one code per two bytes", func(t *testing.T) {
		amount := 100
		centroids := 65536
		values := make([]byte, 2*amount)
		for i := 0; i < amount; i++ {
			binary.BigEndian.PutUint16(values[2*i:2*i+2], uint16(i))
		}
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			true,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})
}

func Test_NoRacePQEncodeBits(t *testing.T) {
	t.Run("encodes correctly on one code per byte", func(t *testing.T) {
		amount := 100
		centroids := 256
		values := make([]byte, amount)
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			true,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			pq.PutCode(uint64(i), values, i)
		}
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("encodes correctly on one code per two bytes", func(t *testing.T) {
		amount := 100
		centroids := 65536
		values := make([]byte, 2*amount)
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			true,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			pq.PutCode(uint64(i), values, i)
		}
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("encodes correctly on 10 bits", func(t *testing.T) {
		amount := 100
		centroids := 1024
		values := make([]byte, 2*amount)
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			true,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			pq.PutCode(uint64(i), values, i)
		}
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})
}

func Test_NoRacePQDecodeBytes(t *testing.T) {
	t.Run("extracts correctly on one code per byte", func(t *testing.T) {
		amount := 100
		centroids := 256
		values := make([]byte, 0, amount)
		for i := byte(0); i < byte(amount); i++ {
			values = append(values, i)
		}
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			false,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("extracts correctly on 6 bits", func(t *testing.T) {
		amount := 100
		centroids := 64
		values := make([]byte, 0, amount)
		for i := byte(0); i < byte(amount); i++ {
			values = append(values, i)
		}

		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			false,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("extracts correctly on 12 bits", func(t *testing.T) {
		amount := 100
		centroids := 4096
		values := make([]byte, 2*amount)
		for i := byte(0); i < byte(amount); i++ {
			binary.BigEndian.PutUint16(values[2*i:], uint16(i))
		}

		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			false,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("extracts correctly on one code per two bytes", func(t *testing.T) {
		amount := 100
		centroids := 65536
		values := make([]byte, 2*amount)
		for i := 0; i < amount; i++ {
			binary.BigEndian.PutUint16(values[2*i:], uint16(i))
		}
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			false,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})
}

func Test_NoRacePQEncodeBytes(t *testing.T) {
	t.Run("encodes correctly on one code per byte", func(t *testing.T) {
		amount := 100
		centroids := 256
		values := make([]byte, amount)
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			false,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			pq.PutCode(uint64(i), values, i)
		}
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("encodes correctly on one code per two bytes", func(t *testing.T) {
		amount := 100
		centroids := 65536
		values := make([]byte, 2*amount)
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			false,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			pq.PutCode(uint64(i), values, i)
		}
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})

	t.Run("encodes correctly on 10 bits", func(t *testing.T) {
		amount := 100
		centroids := 1024
		values := make([]byte, 2*amount)
		pq, _ := ssdhelpers.NewProductQuantizer(
			amount,
			centroids,
			false,
			nil,
			amount,
			ssdhelpers.UseKMeansEncoder,
			ssdhelpers.LogNormalEncoderDistribution,
		)
		for i := 0; i < amount; i++ {
			pq.PutCode(uint64(i), values, i)
		}
		for i := 0; i < amount; i++ {
			code := pq.ExtractCode(values, i)
			assert.Equal(t, code, uint64(i))
		}
	})
}
