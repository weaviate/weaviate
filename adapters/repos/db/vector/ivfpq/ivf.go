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
	"context"
	"math"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type invertedIndexNode interface {
	add(code []byte, id uint64)
	search(codes [][]byte, dists [][]float32, k int, heap *priorityqueue.Queue[byte], accDist float32)
}

type invertedIndex struct {
	nextDim []invertedIndexNode
	perCode []int
	dim     int
	maxDim  int
	locks   *common.ShardedLocks
}

type leave struct {
	ids   []uint64
	locks *common.ShardedLocks
}

func newInvertedIndex(dimension, maxDimension int, locks *common.ShardedLocks) invertedIndexNode {
	if dimension == maxDimension {
		return &leave{
			locks: locks,
		}
	}

	return &invertedIndex{
		dim:     dimension,
		maxDim:  maxDimension,
		locks:   locks,
		nextDim: make([]invertedIndexNode, 256),
		perCode: make([]int, 256),
	}
}

func (i *invertedIndex) add(code []byte, id uint64) {
	pos := uint64(code[i.dim])
	lpos := uint64(1+i.dim%2) * pos
	i.locks.Lock(lpos)
	if i.nextDim[pos] == nil {
		i.nextDim[pos] = newInvertedIndex(i.dim+1, i.maxDim, i.locks)
	}
	i.perCode[pos]++
	i.locks.Unlock(lpos)
	i.nextDim[pos].add(code, id)
}

func (i *leave) add(code []byte, id uint64) {
	i.ids = append(i.ids, id)
}

func (i *invertedIndex) search(codes [][]byte, dists [][]float32, k int, heap *priorityqueue.Queue[byte], accDist float32) {
	if i.nextDim == nil {
		return
	}

	var maxDist float32
	if heap.Len() < k {
		maxDist = math.MaxFloat32
	} else {
		maxDist = heap.Top().Dist
	}

	currentCodes := codes[i.dim]
	currentDists := dists[i.dim]
	for codePos, codeList := range currentCodes {
		if i.nextDim[codeList] != nil && accDist+currentDists[codePos] < maxDist {
			i.nextDim[codeList].search(codes, dists, k, heap, accDist+currentDists[codePos])
			if heap.Len() >= k {
				maxDist = heap.Top().Dist
			}
		}
	}
}

func (i *leave) search(codes [][]byte, dists [][]float32, k int, heap *priorityqueue.Queue[byte], accDist float32) {
	for _, id := range i.ids {
		heap.Insert(id, accDist)
	}
	for heap.Len() > k {
		heap.Pop()
	}
}

type IvfPQ struct {
	pq             *compressionhelpers.ProductQuantizer
	bq             compressionhelpers.BinaryQuantizer
	compressedVecs [][]uint64
	invertedIndex  invertedIndexNode
}

func NewIvf(vectors [][]float32, distancer distancer.Provider) *IvfPQ {
	segments := 12
	cfg := hnsw.PQConfig{
		Enabled:       true,
		Segments:      segments,
		Centroids:     256,
		TrainingLimit: 10_000,
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
		pq:             pq,
		bq:             compressionhelpers.NewBinaryQuantizer(distancer),
		invertedIndex:  newInvertedIndex(0, segments, common.NewDefaultShardedLocks()),
		compressedVecs: make([][]uint64, 1_000_000),
	}
	return ivf
}

func (ivf *IvfPQ) Add(id uint64, vector []float32) {
	code := ivf.pq.Encode(vector)
	ivf.compressedVecs[id] = ivf.bq.Encode(vector)
	ivf.invertedIndex.add(code, id)
}

func (ivf *IvfPQ) SearchByVector(ctx context.Context, searchVec []float32, k int) ([]uint64, []float32, error) {
	codes, dists := ivf.pq.SortCodes(searchVec)
	factor := 500
	bqFactor := 100
	heap := priorityqueue.NewMax[byte](k * factor)
	ivf.invertedIndex.search(codes, dists, k*factor, heap, 0)
	bqheap := priorityqueue.NewMax[byte](k * bqFactor)
	bqdistancer := ivf.bq.NewDistancer(searchVec)
	for bqheap.Len() < k*bqFactor {
		element := heap.Pop()
		d, _ := bqdistancer.Distance(ivf.compressedVecs[element.ID])
		bqheap.Insert(element.ID, d)
	}
	for heap.Len() > 0 {
		element := heap.Pop()
		d, _ := bqdistancer.Distance(ivf.compressedVecs[element.ID])
		if d < bqheap.Top().Dist {
			bqheap.Pop()
			bqheap.Insert(element.ID, d)
		}
	}

	ids := make([]uint64, k*bqFactor)
	dist := make([]float32, k*bqFactor)

	j := k * bqFactor
	for bqheap.Len() > 0 {
		j--
		elem := bqheap.Pop()
		ids[j] = elem.ID
		dist[j] = elem.Dist
	}
	return ids, dist, nil
}
