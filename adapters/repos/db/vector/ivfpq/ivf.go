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
	"fmt"
	"math/bits"
	"sync"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type invertedIndexNode interface {
	add(code []byte, id uint64, dim int) invertedIndexNode
	search(codes []byte, k int, heap, aux *priorityqueue.Queue[byte], dim, accDist int)
}

type invertedIndex struct {
	sync.RWMutex
	nextDim []invertedIndexNode
	codes   []byte
}

type leave struct {
	sync.RWMutex
	ids   []uint64
	codes []byte
}

func newInvertedIndex(dimension, maxDimension int) invertedIndexNode {
	return &leave{}
}

func (i *invertedIndex) add(code []byte, id uint64, dim int) invertedIndexNode {
	i.Lock()
	offset := 0
	for offset < len(i.codes) && i.codes[offset] == code[dim+offset] {
		offset++
	}
	if offset < len(i.codes) {
		//split
		common := make([]byte, offset)
		copy(common, i.codes[:offset])
		nextDim := make([]invertedIndexNode, 256)
		nextDim[i.codes[offset]] = i
		i.codes = i.codes[offset+1:]
		i.Unlock()
		remaining := make([]byte, len(code)-dim-1-offset)
		copy(remaining, code[dim+1+offset:])
		nextDim[code[dim+offset]] = &leave{
			codes: remaining,
			ids:   []uint64{id},
		}
		newInvertedIndex := &invertedIndex{
			codes:   common,
			nextDim: nextDim,
		}
		return newInvertedIndex
	}
	if len(i.codes) == 0 {
		if i.nextDim[code[dim]] == nil {
			remaining := make([]byte, len(code)-dim-1-offset)
			copy(remaining, code[dim+offset+1:])
			i.nextDim[code[dim]] = &leave{
				codes: remaining,
				ids:   []uint64{id},
			}
			i.Unlock()
			return i
		}
		i.nextDim[code[dim+offset]] = i.nextDim[code[dim+offset]].add(code, id, dim+offset+1)
		i.Unlock()
		return i
	}
	if i.nextDim[code[dim+offset]] == nil {
		remaining := make([]byte, len(code)-dim-offset-1)
		copy(remaining, code[dim+offset+1:])
		i.nextDim[code[dim+offset]] = &leave{
			ids:   []uint64{id},
			codes: remaining,
		}
		i.Unlock()
		return i
	}
	i.nextDim[code[dim+offset]] = i.nextDim[code[dim+offset]].add(code, id, dim+offset+1)
	i.Unlock()
	return i
}

func (i *leave) add(code []byte, id uint64, dim int) invertedIndexNode {
	i.Lock()
	if len(i.ids) == 0 {
		i.codes = code
		i.ids = append(i.ids, id)
		i.Unlock()
		return i
	}
	offset := 0
	for offset < len(i.codes) && i.codes[offset] == code[dim+offset] {
		offset++
	}
	if offset < len(i.codes) {
		common := make([]byte, offset)
		copy(common, i.codes[:offset])
		nextDim := make([]invertedIndexNode, 256)
		nextDim[i.codes[offset]] = i
		i.codes = i.codes[offset+1:]
		remaining := make([]byte, len(code)-dim-1-offset)
		copy(remaining, code[dim+1+offset:])
		nextDim[code[dim+offset]] = &leave{
			codes: remaining,
			ids:   []uint64{id},
		}
		newInvertedIndex := &invertedIndex{
			codes:   common,
			nextDim: nextDim,
		}
		if dim+1+len(i.codes)+len(common) != len(code) {
			fmt.Println("here")
		}
		if dim+1+len(remaining)+len(common) != len(code) {
			fmt.Println("here")
		}
		i.Unlock()
		return newInvertedIndex
	}
	i.ids = append(i.ids, id)
	i.Unlock()
	return i
}

func (i *invertedIndex) search(code []byte, k int, heap, aux *priorityqueue.Queue[byte], dim, accDist int) {
	var maxDist int
	if heap.Len() < k {
		maxDist = len(code) * 64
	} else {
		maxDist = int(heap.Top().Dist)
	}

	offset := 0
	i.RLock()
	defer i.RUnlock()
	for offset < len(i.codes) {
		accDist += bits.OnesCount8(i.codes[offset] ^ code[dim+offset])
		offset++
	}

	currentCode := code[dim+offset]
	for idx := range i.nextDim {
		if i.nextDim[idx] != nil {
			currDist := bits.OnesCount8(currentCode ^ byte(idx))
			if accDist+currDist < maxDist {
				aux.Insert(uint64(idx), float32(currDist))
			}
		}
	}

	ids := make([]uint64, aux.Len())
	dist := make([]int, aux.Len())
	j := 0
	for aux.Len() > 0 {
		elem := aux.Pop()
		ids[j] = elem.ID
		dist[j] = int(elem.Dist)
		j++
	}

	for j := range ids {
		currentDist := dist[j]

		if accDist+currentDist < maxDist {
			i.nextDim[ids[j]].search(code, k, heap, aux, dim+offset+1, accDist+currentDist)
			if heap.Len() >= k {
				maxDist = int(heap.Top().Dist)
			}
		}
	}
}

func (i *leave) search(code []byte, k int, heap, aux *priorityqueue.Queue[byte], dim, accDist int) {
	offset := 0
	i.RLock()
	defer i.RUnlock()
	for offset < len(i.codes) {
		accDist += bits.OnesCount8(i.codes[offset] ^ code[dim+offset])
		offset++
	}
	if heap.Len() < k || heap.Top().Dist > float32(accDist) {
		for _, id := range i.ids {
			heap.Insert(id, float32(accDist))
		}
		for heap.Len() > k {
			heap.Pop()
		}
	}
}

type IvfPQ struct {
	sync.Mutex
	lsh            *compressionhelpers.LSHQuantizer
	compressor     *compressionhelpers.ScalarQuantizer
	compressedVecs [][]byte
	invertedIndex  invertedIndexNode
}

func NewIvf(vectors [][]float32, distancer distancer.Provider) *IvfPQ {
	bands := 3
	perBand := 192
	ivf := &IvfPQ{
		lsh:            compressionhelpers.NewLSHQuantizer(perBand, bands, len(vectors[0])),
		compressor:     compressionhelpers.NewScalarQuantizer(vectors, distancer),
		invertedIndex:  newInvertedIndex(0, bands*perBand/64),
		compressedVecs: make([][]byte, 1_000_000),
	}
	return ivf
}

func (ivf *IvfPQ) Add(id uint64, vector []float32) {
	code := ivf.lsh.Encode8(vector)
	ivf.compressedVecs[id] = ivf.compressor.Encode(vector)
	ivf.Lock()
	ivf.invertedIndex = ivf.invertedIndex.add(code, id, 0)
	ivf.Unlock()
}

func (ivf *IvfPQ) SearchByVector(ctx context.Context, searchVec []float32, k int) ([]uint64, []float32, error) {
	code := ivf.lsh.Encode8(searchVec)
	probing := 5_000
	heap := priorityqueue.NewMax[byte](probing)
	aux := priorityqueue.NewMin[byte](probing)
	start := time.Now()
	ivf.invertedIndex.search(code, probing, heap, aux, 0, 0)
	fmt.Println(time.Since(start))

	compressedK := 15
	cheap := priorityqueue.NewMax[byte](compressedK)
	cdistancer := ivf.compressor.NewDistancer(searchVec)

	for heap.Len() > 0 {
		element := heap.Pop()
		d, _ := cdistancer.Distance(ivf.compressedVecs[element.ID])
		if cheap.Len() >= compressedK {
			cheap.Pop()
		}
		cheap.Insert(element.ID, d)
	}

	ids := make([]uint64, compressedK)
	dist := make([]float32, compressedK)

	index := compressedK
	for cheap.Len() > 0 {
		index--
		element := cheap.Pop()
		ids[index] = element.ID
		dist[index] = element.Dist
	}
	return ids, dist, nil
}
