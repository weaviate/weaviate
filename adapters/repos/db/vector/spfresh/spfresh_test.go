package spfresh

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := provider.SingleDist(x, y)
		return dist
	}
}

func TestSPFreshRecall(t *testing.T) {
	cfg := DefaultConfig()
	l := logrus.New()
	l.SetLevel(logrus.InfoLevel)
	cfg.Logger = l

	logger, _ := test.NewNullLogger()

	vectors_size := 10_000
	queries_size := 10
	dimensions := 64
	k := 100

	before := time.Now()
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)

	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(cfg.Distancer))
	})

	fmt.Printf("generating data took %s\n", time.Since(before))

	index, err := New(cfg, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	defer index.Shutdown(t.Context())

	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(id uint64) {
		if id%1000 == 0 {
			fmt.Printf("indexing vector %d/%d\n", id, vectors_size)
		}
		err := index.Add(t.Context(), id, vectors[id])
		require.NoError(t, err)
	})

	fmt.Println("--------------------------- indexing done, starting queries after 1 minute")
	time.Sleep(2 * time.Minute)

	// do some warmup queries to trigger merge operations
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		_, _, err := index.SearchByVector(t.Context(), queries[i], k, nil)
		require.NoError(t, err)
	})

	fmt.Println("--------------------------- warmup done, starting timed queries after 1 minute")
	time.Sleep(1 * time.Minute)
	var mu sync.Mutex
	var relevant uint64
	var retrieved int

	var querying time.Duration = 0
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		before := time.Now()
		results, _, err := index.SearchByVector(t.Context(), queries[i], k, nil)
		require.NoError(t, err)
		mu.Lock()
		querying += time.Since(before)
		retrieved += k
		relevant += testinghelpers.MatchesInLists(truths[i], results)
		mu.Unlock()
	})

	recall := float32(relevant) / float32(retrieved)
	latency := float32(querying.Microseconds()) / float32(queries_size)
	fmt.Println(recall, latency)
	require.Greater(t, recall, float32(0.9))
}
