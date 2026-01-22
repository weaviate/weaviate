//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hfresh

import (
	"context"
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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := provider.SingleDist(x, y)
		return dist
	}
}

// Uncomment to enable pprof and prometheus metrics when running tests

func TestMain(m *testing.M) {
	runtime.SetMutexProfileFraction(1)

	go func() {
		addr := "127.0.0.1:6060"
		log.Printf("pprof listening at http://%s/debug/pprof/\n", addr)
		_ = http.ListenAndServe(addr, nil) // DefaultServeMux has pprof handlers
	}()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", mux); err != nil {
			fmt.Printf("metrics server on %s stopped: %v\n", ":2112", err)
		}
	}()

	os.Exit(m.Run())
}

func makeNoopCommitLogger() (hnsw.CommitLogger, error) {
	return &hnsw.NoopCommitLogger{}, nil
}

func makeTestMetrics() *Metrics {
	return NewMetrics(monitoring.GetMetrics(), "n/a", "n/a")
}

func makeHFreshConfig(t *testing.T) (*Config, ent.UserConfig) {
	l := logrus.New()
	tmpDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.RootPath = tmpDir
	cfg.ID = "hfresh"
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath: tmpDir,
		ID:       "centroids",
		MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(tmpDir, "centroids",
				l, cyclemanager.NewCallbackGroupNoop(),
			)
		},
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
	}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()

	cfg.Logger = l
	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: l,
		},
	)
	scheduler.Start()
	cfg.Scheduler = scheduler
	cfg.PrometheusMetrics = monitoring.GetMetrics()
	cfg.PrometheusMetrics.Registerer.MustRegister()

	return cfg, ent.NewDefaultUserConfig()
}

func makeHFreshWithConfig(t *testing.T, store *lsmkv.Store, cfg *Config, uc ent.UserConfig) *HFresh {
	index, err := New(cfg, uc, store)
	require.NoError(t, err)

	index.PostStartup(t.Context())

	t.Cleanup(func() {
		index.Shutdown(t.Context())
	})

	return index
}

func TestHFreshRecall(t *testing.T) {
	logger, _ := test.NewNullLogger()
	store := testinghelpers.NewDummyStore(t)
	cfg, ucfg := makeHFreshConfig(t)

	vectors_size := 10_000
	queries_size := 100
	dimensions := 64
	k := 10

	before := time.Now()
	vectors, queries := testinghelpers.RandomVecsFixedSeed(vectors_size, queries_size, dimensions)
	var mu sync.Mutex
	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		res, _ := testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(distancer.NewL2SquaredProvider()))
		mu.Lock()
		truths[i] = res
		mu.Unlock()
	})

	fmt.Printf("generating data took %s\n", time.Since(before))

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		return vectors[indexID], nil
	})
	index := makeHFreshWithConfig(t, store, cfg, ucfg)

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

	fmt.Printf("indexing done, took: %s, waiting for background tasks...\n", time.Since(before))

	for index.taskQueue.Size() > 0 {
		fmt.Println("background tasks: ", index.taskQueue.Size())
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("all background tasks done, took: ", time.Since(before))

	index.searchProbe = 64
	recall, latency := testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
	fmt.Println(index.searchProbe, recall, latency)

	index.searchProbe = 128
	recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
	fmt.Println(index.searchProbe, recall, latency)

	index.searchProbe = 256
	recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
	fmt.Println(index.searchProbe, recall, latency)

	index.searchProbe = 512
	recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
	fmt.Println(index.searchProbe, recall, latency)

	require.Greater(t, recall, float32(0.7))

	err := index.Flush()
	require.NoError(t, err)

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	t.Run("test disk layout", func(t *testing.T) {
		dirs, err := os.ReadDir(cfg.RootPath)
		require.NoError(t, err)
		require.Len(t, dirs, 5)
		require.Equal(t, "centroids.hnsw.commitlog.d", dirs[0].Name())
		require.Equal(t, "centroids.hnsw.snapshot.d", dirs[1].Name())
		require.Equal(t, "merge.queue.d", dirs[2].Name())
		require.Equal(t, "reassign.queue.d", dirs[3].Name())
		require.Equal(t, "split.queue.d", dirs[4].Name())
	})

	t.Run("restart and re-test recall", func(t *testing.T) {
		index = makeHFreshWithConfig(t, store, cfg, ucfg)

		index.searchProbe = 256
		recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
		require.Greater(t, recall, float32(0.7))
	})
}
