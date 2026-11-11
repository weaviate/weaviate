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

//go:build benchmark

package hfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/emeans"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Compares index build time and recall when posting splits are clustered
// with balanced k-means (the default) or balanced e-means. Run with
//
//	HFRESH_BENCH_DATA=<file.fvecs> HFRESH_BENCH_N=200000 go test -tags benchmark \
//	  -run TestSplitClusteringBuild -v -timeout 0 ./adapters/repos/db/vector/hfresh/
//
// HFRESH_BENCH_VARIANTS filters variants by substring and HFRESH_BENCH_REPS
// sets how often every variant is built (default 1). Vectors are compared
// with squared L2, and the last vectors of the file serve as queries.

func emeansSplitClusterer(epochs int) splitClusterer {
	return func(data [][]float32, dims int) ([][]float32, []uint32, error) {
		em := emeans.New(2, dims, 0)
		em.Epochs = epochs
		assignments, err := em.FitBalanced(data)
		return em.Centers, assignments, err
	}
}

var splitClusterers = []struct {
	name    string
	cluster splitClusterer
}{
	{"kmeans", kmeansSplitClusterer},
	{"emeans-1", emeansSplitClusterer(1)},
	{"emeans-2", emeansSplitClusterer(2)},
	{"emeans-3", emeansSplitClusterer(3)},
	{"emeans-5", emeansSplitClusterer(5)},
}

func readFvecs(t *testing.T, path string) [][]float32 {
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	d := int(binary.LittleEndian.Uint32(raw))
	data := make([][]float32, len(raw)/(4*(d+1)))
	for i := range data {
		row := raw[i*4*(d+1)+4 : (i+1)*4*(d+1)]
		data[i] = make([]float32, d)
		for j := range d {
			data[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(row[4*j:]))
		}
	}
	return data
}

func bruteForceNeighbors(vectors [][]float32, queries [][]float32, k int) [][]uint64 {
	provider := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	truths := make([][]uint64, len(queries))
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(q uint64) {
		type hit struct {
			id   uint64
			dist float32
		}
		hits := make([]hit, len(vectors))
		for i, v := range vectors {
			dist, _ := provider.SingleDist(queries[q], v)
			hits[i] = hit{id: uint64(i), dist: dist}
		}
		slices.SortFunc(hits, func(a, b hit) int {
			if a.dist < b.dist {
				return -1
			}
			if a.dist > b.dist {
				return 1
			}
			return 0
		})
		truths[q] = make([]uint64, k)
		for i := range k {
			truths[q][i] = hits[i].id
		}
	})
	return truths
}

func buildWithSplitClusterer(t *testing.T, name string, cluster splitClusterer, vectors [][]float32, queries [][]float32, truths [][]uint64) {
	store := testinghelpers.NewDummyStore(t)
	cfg := DefaultConfig()
	cfg.RootPath = t.TempDir()
	cfg.ID = "hfresh"
	cfg.DistanceProvider = distancer.NewL2SquaredProvider()
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		GetViewThunk:          getViewThunk,
		DistanceProvider:      cfg.DistanceProvider,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
	}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.ErrorLevel)
	cfg.Logger = logger
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
	scheduler.Start()
	cfg.Scheduler = scheduler
	cfg.PrometheusMetrics = monitoring.GetMetrics()
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk("", func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if indexID < uint64(len(vectors)) {
			return vectors[indexID], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})
	setDelegatingTempThunk(cfg)
	createObjectsBucket(t, store)

	index, err := New(cfg, ent.NewDefaultUserConfig(), store)
	require.NoError(t, err)
	defer index.Shutdown(t.Context())

	var fitNanos, fits, fitPoints atomic.Int64
	index.clusterSplit = func(data [][]float32, dims int) ([][]float32, []uint32, error) {
		start := time.Now()
		centers, assignments, err := cluster(data, dims)
		fitNanos.Add(int64(time.Since(start)))
		fits.Add(1)
		fitPoints.Add(int64(len(data)))
		return centers, assignments, err
	}

	start := time.Now()
	compressionhelpers.Concurrently(logger, uint64(len(vectors)), func(id uint64) {
		require.NoError(t, index.Add(t.Context(), id, vectors[id]))
	})
	insert := time.Since(start)
	for index.taskQueue.Size() > 0 {
		flushBenchBucket(t, index.IndexMetadata.bucket)
		flushBenchBucket(t, index.PostingStore.bucket)
		time.Sleep(100 * time.Millisecond)
	}
	total := time.Since(start)

	recall, latency := testinghelpers.RecallAndLatency(t.Context(), queries, 10, index, truths)
	// The fit time is summed over the workers that split concurrently.
	fmt.Fprintf(os.Stderr, "RESULT\t%s\tn=%d\tinsert=%.1fs\ttotal=%.1fs\tfit=%.2fs\tsplits=%d\tpoints/split=%.0f\tus/split=%.0f\trecall@10=%.4f\tlatency=%.0fus\n",
		name, len(vectors), insert.Seconds(), total.Seconds(), time.Duration(fitNanos.Load()).Seconds(),
		fits.Load(), float64(fitPoints.Load())/float64(max(1, fits.Load())),
		float64(fitNanos.Load())/1e3/float64(max(1, fits.Load())), recall, latency)
}

func flushBenchBucket(t *testing.T, ref bucketRef) {
	t.Helper()

	bucket, release, err := ref.acquire()
	require.NoError(t, err)
	defer release()

	require.NoError(t, bucket.FlushAndSwitch())
}

func TestSplitClusteringBuild(t *testing.T) {
	path := os.Getenv("HFRESH_BENCH_DATA")
	if path == "" {
		t.Skip("HFRESH_BENCH_DATA is not set")
	}
	const queryCount = 500
	data := readFvecs(t, path)
	n := len(data) - queryCount
	if value := os.Getenv("HFRESH_BENCH_N"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		n = min(n, parsed)
	}
	reps := 1
	if value := os.Getenv("HFRESH_BENCH_REPS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		reps = parsed
	}
	vectors, queries := data[:n], data[len(data)-queryCount:]
	truths := bruteForceNeighbors(vectors, queries, 10)

	filter := os.Getenv("HFRESH_BENCH_VARIANTS")
	// Repetitions are interleaved so that drift affects every variant alike.
	for rep := range reps {
		for _, variant := range splitClusterers {
			if !strings.Contains(variant.name, filter) {
				continue
			}
			t.Run(fmt.Sprintf("%s/rep%d", variant.name, rep), func(t *testing.T) {
				buildWithSplitClusterer(t, variant.name, variant.cluster, vectors, queries, truths)
			})
		}
	}
}
