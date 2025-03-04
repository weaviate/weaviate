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

func TestLsh(t *testing.T) {
	vectors, _ := testinghelpers.RandomVecs(5, 0, 500)

	lsh := compressionhelpers.NewLSHQuantizer(50, 10, 500)
	compressed := make([][]uint64, 5)
	for i := range vectors {
		compressed[i] = lsh.Encode(vectors[i])
		fmt.Println(compressed[i])
		assert.Equal(t, 8, len(compressed[i]))
	}
}

func TestLSHQuantizerRecall(t *testing.T) {
	k := 10
	distanceProvider := distancer.NewCosineDistanceProvider()
	vectors, queryVecs := testinghelpers.RandomVecsFixedSeed(10_000, 100, 1536)
	compressionhelpers.Concurrently(logger, uint64(len(vectors)), func(i uint64) {
		vectors[i] = distancer.Normalize(vectors[i])
	})
	compressionhelpers.Concurrently(logger, uint64(len(queryVecs)), func(i uint64) {
		queryVecs[i] = distancer.Normalize(queryVecs[i])
	})
	lshq := compressionhelpers.NewLSHQuantizer(64, 192, 1536)

	codes := make([][]uint64, len(vectors))
	compressionhelpers.Concurrently(logger, uint64(len(vectors)), func(i uint64) {
		codes[i] = lshq.Encode(vectors[i])
	})
	neighbors := make([][]uint64, len(queryVecs))
	compressionhelpers.Concurrently(logger, uint64(len(queryVecs)), func(i uint64) {
		neighbors[i], _ = testinghelpers.BruteForce(logger, vectors, queryVecs[i], k, func(f1, f2 []float32) float32 {
			d, _ := distanceProvider.SingleDist(f1, f2)
			return d
		})
	})
	correctedK := 500
	hits := uint64(0)
	mutex := sync.Mutex{}
	duration := time.Duration(0)
	compressionhelpers.Concurrently(logger, uint64(len(queryVecs)), func(i uint64) {
		before := time.Now()
		query := lshq.Encode(queryVecs[i])
		heap := priorityqueue.NewMax[any](correctedK)
		for j := range codes {
			d, _ := lshq.DistanceBetweenCompressedVectors(codes[j], query)
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
	assert.True(t, recall > 0.999)
}
