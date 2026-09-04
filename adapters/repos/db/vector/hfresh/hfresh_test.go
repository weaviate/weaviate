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
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type noopBucketView struct{}

func (n *noopBucketView) ReleaseView() {}

func getViewThunk() common.BucketView {
	return &noopBucketView{}
}

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

func makeNoopCommitLogger(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
	return &hnsw.NoopCommitLogger{}, nil
}

func makeHFreshConfig(t *testing.T) (*Config, ent.UserConfig) {
	l := logrus.New()
	tmpDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.VectorForIDThunk = func(context.Context, uint64) ([]float32, error) {
		return nil, fmt.Errorf("no vector store wired in this test")
	}
	cfg.RootPath = tmpDir
	cfg.ID = "hfresh"
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath: tmpDir,
		ID:       "centroids",
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(tmpDir, "centroids",
				l, cyclemanager.NewCallbackGroupNoop(),
				opts...,
			)
		},
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		GetViewThunk:      getViewThunk,
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

	setDelegatingTempThunk(cfg)

	return cfg, ent.NewDefaultUserConfig()
}

// setDelegatingTempThunk satisfies the required TempVectorForIDWithViewThunk
// by delegating to cfg.VectorForIDThunk at call time — tests assign their
// fixture thunk after building the config, and the pooled read path picks it
// up automatically. The vector is copied into the pooled container because
// the caller may normalize the returned slice in place.
func setDelegatingTempThunk(cfg *Config) {
	cfg.TempVectorForIDWithViewThunk = func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		// Mirror the production thunk (Shard.readVectorByIndexIDIntoSliceWithView),
		// which writes the ID into Buff8 unconditionally: a caller passing a
		// container without pool-initialized buffers must fail tests too.
		binary.LittleEndian.PutUint64(container.Buff8, id)
		vec, err := cfg.VectorForIDThunk(ctx, id)
		if err != nil {
			return nil, err
		}
		if cap(container.Slice) < len(vec) {
			container.Slice = make([]float32, len(vec))
		}
		container.Slice = container.Slice[:len(vec)]
		copy(container.Slice, vec)
		return container.Slice, nil
	}
}

func makeHFreshWithConfig(t *testing.T, store *lsmkv.Store, cfg *Config, uc ent.UserConfig) *HFresh {
	createObjectsBucket(t, store)

	index, err := New(cfg, uc, store)
	require.NoError(t, err)

	index.PostStartup(t.Context())

	t.Cleanup(func() {
		index.Shutdown(t.Context())
	})

	return index
}

func countFiles(t *testing.T, dir string) int {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	return len(files)
}

func TestHFreshRecall(t *testing.T) {
	logger, _ := test.NewNullLogger()
	store := testinghelpers.NewDummyStore(t)
	cfg, ucfg := makeHFreshConfig(t)

	// Reduced from 10,000 for faster CI execution — but kept well above the
	// split threshold: at 64 dims maxPostingSize is 1,490, so anything at or
	// below ~1,500 vectors exercises at most one split and recall is measured
	// against an effectively single-posting index. 5,000 forces several
	// splits so recall runs on a real multi-posting layout.
	vectors_size := 5_000
	queries_size := 50
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
		require.Equal(t, "analyze.queue.d", dirs[0].Name())
		require.Equal(t, 0, countFiles(t, filepath.Join(cfg.RootPath, dirs[0].Name())))
		require.Equal(t, "centroids.hnsw.commitlog.d", dirs[1].Name())
		require.Equal(t, "merge.queue.d", dirs[2].Name())
		require.Equal(t, "reassign.queue.d", dirs[3].Name())
		require.Equal(t, 0, countFiles(t, filepath.Join(cfg.RootPath, dirs[3].Name())))
		require.Equal(t, "split.queue.d", dirs[4].Name())
		require.Equal(t, 0, countFiles(t, filepath.Join(cfg.RootPath, dirs[4].Name())))
	})

	t.Run("restart and re-test recall", func(t *testing.T) {
		index = makeHFreshWithConfig(t, store, cfg, ucfg)

		index.searchProbe = 256
		recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, truths)
		require.Greater(t, recall, float32(0.7))
	})
}

// seedUncompressedCentroidState writes hnsw node state, but no compression
// record, into the commit log the centroid graph at (rootPath, centroidID)
// reads back on its next construction. It bypasses hfresh's Insert on
// purpose: hfresh forces RQ on for the centroid graph, and RQ initializes
// inside the first insert, which would persist a compression record and hide
// the "nodes on disk, no compression record" shape a torn commit-log tail or
// a failed RQ initialization leaves behind. That shape is the one in which
// the centroid graph used to prefill from object storage.
func seedUncompressedCentroidState(t *testing.T, rootPath, centroidID string, store *lsmkv.Store, vec []float32) {
	t.Helper()
	logger := logrus.New()
	idx, err := hnsw.New(hnsw.Config{
		RootPath:         rootPath,
		ID:               centroidID,
		Logger:           logger,
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) { return nil, nil },
		GetViewThunk:     getViewThunk,
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(rootPath, centroidID, logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
	}, hnswent.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)

	require.NoError(t, idx.Add(context.Background(), 999, vec))
	require.NoError(t, idx.Flush())
	require.NoError(t, idx.Shutdown(context.Background()))
}

// TestCentroidPrefillNeverReadsObjectStorage builds a real hfresh index over
// a store whose objects bucket holds legacy vectors, with the centroid graph
// pre-seeded to the "nodes but no compression record" shape, and checks the
// centroid cache stays empty of them: the centroid graph carries no
// VectorFromObject, so hnsw's startup prefill never scans the objects bucket
// for it. Before that gate, hnsw derived the legacy vector from the centroid
// graph's ID and loaded real object vectors into the centroid cache.
func TestCentroidPrefillNeverReadsObjectStorage(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	createObjectsBucket(t, store)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	testinghelpers.PutTestObject(t, bucket, 1, []float32{0.1, 0.2, 0.3}, nil)
	testinghelpers.PutTestObject(t, bucket, 2, []float32{0.4, 0.5, 0.6}, nil)

	cfg, uc := makeHFreshConfig(t)
	seedUncompressedCentroidState(t, cfg.Centroids.HNSWConfig.RootPath, cfg.Centroids.HNSWConfig.ID,
		store, []float32{1, 0, 0})

	index, err := New(cfg, uc, store)
	require.NoError(t, err)
	index.PostStartup(t.Context())
	t.Cleanup(func() { index.Shutdown(t.Context()) })

	for _, docID := range []uint64{1, 2} {
		centroid, err := index.Centroids.Get(docID)
		require.NoError(t, err)
		assert.Emptyf(t, centroid.Uncompressed,
			"centroid graph must never prefill from object storage, but doc id %d was found in its cache", docID)
	}
}
