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

package ivfpq

import (
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type invertedIndex struct {
	nextDim []*invertedIndex
	vectors [][]float32
	ids     []uint64
	dim     int
}

func newInvertedIndex(dimension int) *invertedIndex {
	return &invertedIndex{
		dim: dimension,
	}
}

func (i *invertedIndex) add(code []byte, vector []float32, id uint64) {
	if i.dim == len(vector) {
		i.ids = append(i.ids, id)
		i.vectors = append(i.vectors, vector)
	} else {
		if i.nextDim[code[i.dim]] == nil {
			i.nextDim[code[i.dim]] = newInvertedIndex(i.dim + 1)
		}
		i.nextDim[code[i.dim]].add(code, vector, id)
	}
}

func (i *invertedIndex) get(code []byte) ([][]float32, []uint64) {
	if i.nextDim == nil {
		return i.vectors, i.ids
	}
	pos := int(code[i.dim])
	if len(i.nextDim) <= pos || i.nextDim[pos] == nil {
		return nil, nil
	}
	return i.nextDim[pos].get(code)
}

type IvfPQ struct {
	sync.RWMutex
	pq            *compressionhelpers.ProductQuantizer
	invertedIndex *invertedIndex
}

func NewIvf(vectors [][]float32, distancer distancer.Provider) *IvfPQ {
	cfg := hnsw.PQConfig{
		Enabled:       true,
		Segments:      256,
		Centroids:     256,
		TrainingLimit: 100_000,
		Encoder: hnsw.PQEncoder{
			Type:         hnsw.PQEncoderTypeKMeans,
			Distribution: hnsw.PQEncoderDistributionNormal,
		},
	}
	pq, err := compressionhelpers.NewProductQuantizer(cfg, distancer, len(vectors[0]), logrus.New())
	if err != nil {
		panic(err)
	}
	pq.Fit(vectors)
	ivf := &IvfPQ{
		pq:            pq,
		invertedIndex: newInvertedIndex(0),
	}
	return ivf
}

func (ivf *IvfPQ) Add(id uint64, vector []float32) {
	code := ivf.pq.Encode(vector)
	ivf.Lock()
	defer ivf.Unlock()
	ivf.invertedIndex.add(code, vector, id)
}
