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

package compressionhelpers_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestBinaryQuantizerRecall(t *testing.T) {
	k := 10
	distanceProvider := distancer.NewCosineDistanceProvider()
	vectors, queryVecs := testinghelpers.RandomVecs(10_000, 100, 1536)
	compressionhelpers.Concurrently(uint64(len(vectors)), func(i uint64) {
		vectors[i] = distancer.Normalize(vectors[i])
	})
	compressionhelpers.Concurrently(uint64(len(queryVecs)), func(i uint64) {
		queryVecs[i] = distancer.Normalize(queryVecs[i])
	})
	bq := compressionhelpers.NewBinaryQuantizer(nil)

	codes := make([][]uint64, len(vectors))
	compressionhelpers.Concurrently(uint64(len(vectors)), func(i uint64) {
		codes[i] = bq.Encode(vectors[i])
	})
	neighbors := make([][]uint64, len(queryVecs))
	compressionhelpers.Concurrently(uint64(len(queryVecs)), func(i uint64) {
		neighbors[i], _ = testinghelpers.BruteForce(vectors, queryVecs[i], k, func(f1, f2 []float32) float32 {
			d, _, _ := distanceProvider.SingleDist(f1, f2)
			return d
		})
	})
	correctedK := 200
	hits := uint64(0)
	mutex := sync.Mutex{}
	duration := time.Duration(0)
	compressionhelpers.Concurrently(uint64(len(queryVecs)), func(i uint64) {
		before := time.Now()
		query := bq.Encode(queryVecs[i])
		heap := priorityqueue.NewMax[any](correctedK)
		for j := range codes {
			d, _ := bq.DistanceBetweenCompressedVectors(codes[j], query)
			if heap.Len() < correctedK || heap.Top().Dist > d {
				if heap.Len() == correctedK {
					heap.Pop()
				}
				heap.Insert(uint64(j), d)
			}
		}
		ids := make([]uint64, correctedK)
		for j := range ids {
			ids[j] = heap.Pop().ID
		}
		mutex.Lock()
		duration += time.Since(before)
		hits += testinghelpers.MatchesInLists(neighbors[i][:k], ids)
		mutex.Unlock()
	})
	recall := float32(hits) / float32(k*len(queryVecs))
	latency := float32(duration.Microseconds()) / float32(len(queryVecs))
	fmt.Println(recall, latency)
	assert.True(t, recall > 0.7)
}

func TestBinaryQuantizerChecksSize(t *testing.T) {
	bq := compressionhelpers.NewBinaryQuantizer(nil)
	_, err := bq.DistanceBetweenCompressedVectors(make([]uint64, 3), make([]uint64, 4))
	assert.NotNil(t, err)
}
