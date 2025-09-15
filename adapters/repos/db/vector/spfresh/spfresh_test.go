package spfresh

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestMain(m *testing.M) {
	// Capture every contended mutex event (set >0; 1 is fine)
	runtime.SetMutexProfileFraction(1)

	// Optional: also capture blocking profile
	// runtime.SetBlockProfileRate(1)

	go func() {
		addr := "127.0.0.1:6060"
		log.Printf("pprof listening at http://%s/debug/pprof/\n", addr)
		_ = http.ListenAndServe(addr, nil) // DefaultServeMux has pprof handlers
	}()

	os.Exit(m.Run())
}

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := provider.SingleDist(x, y)
		return dist
	}
}

func TestSPFreshRecall(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RNGFactor = 5.0
	l := logrus.New()
	// l.SetLevel(logrus.DebugLevel)
	cfg.Logger = l

	logger, _ := test.NewNullLogger()

	vectors_size := 100_000
	queries_size := 100
	dimensions := 64
	k := 100

	before := time.Now()
	vectors, queries := testinghelpers.RandomVecsFixedSeed(vectors_size, queries_size, dimensions)

	var mu sync.Mutex

	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		res, _ := testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(cfg.Distancer))
		mu.Lock()
		truths[i] = res
		mu.Unlock()
	})

	fmt.Printf("generating data took %s\n", time.Since(before))

	index, err := New(cfg, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	defer index.Shutdown(t.Context())

	before = time.Now()
	var count atomic.Uint32
	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(id uint64) {
		cur := count.Add(1)
		if cur%1000 == 0 {
			fmt.Printf("indexing vectors %d/%d\n", cur, vectors_size)
		}
		err := index.Add(t.Context(), id, vectors[id])
		require.NoError(t, err)
	})

	fmt.Printf("indexing done, took: %s\n", time.Since(before))

	recall, latency := testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
	fmt.Println(recall, latency)
	require.Greater(t, recall, float32(0.7))
}
