//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build benchmark

package spfresh

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/spfresh"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

var (
	distanceMethod = flag.String("distance", "l2-squared", "distance method: l2-squared, cosine, or dot")
	dataset        = flag.String("dataset", "fiqa-st-minilm-384-dot-12k", "ann dataset i.e. fiqa-st-minilm-384-dot-12k")
)

func getDistanceProvider() distancer.Provider {
	switch *distanceMethod {
	case "cosine":
		return distancer.NewCosineDistanceProvider()
	case "dot":
		return distancer.NewDotProductProvider()
	case "l2-squared":
		fallthrough
	default:
		return distancer.NewL2SquaredProvider()
	}
}

func TestSPFreshRecallParquet(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg := DefaultConfig()
	cfg.Centroids.IndexType = "hnsw"

	distanceProvider := getDistanceProvider()
	cfg.DistanceProvider = distanceProvider
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "spfresh",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distanceProvider,
	}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	l := logrus.New()
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

	logger, _ := test.NewNullLogger()

	// Load dataset using parquet reader
	hf := datasets.NewHubDataset("weaviate/ann-datasets", *dataset)

	t.Logf("Using dataset %s, distance metric %s", *dataset, *distanceMethod)

	before := time.Now()
	t.Log("Loading training data...")
	ids, vectors, err := hf.LoadTrainData()
	require.NoError(t, err)

	t.Log("Loading test data...")
	neighbors, queries, err := hf.LoadTestData()
	require.NoError(t, err)

	t.Logf("Loading data took %s", time.Since(before))

	vectors_size := len(vectors)
	k := 10

	idToIndex := make(map[uint64]int, len(ids))
	for i, id := range ids {
		idToIndex[id] = i
	}

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if idx, ok := idToIndex[indexID]; ok {
			return vectors[idx], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index, err := New(cfg, ent.NewDefaultUserConfig(), store)
	require.NoError(t, err)
	defer index.Shutdown(t.Context())

	before = time.Now()
	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(id uint64) {
		err := index.Add(t.Context(), ids[id], vectors[id])
		require.NoError(t, err)
	})

	t.Logf("Indexing done, took: %s, waiting for background tasks...", time.Since(before))

	for index.taskQueue.Size() > 0 {
		t.Logf("background tasks: %d", index.taskQueue.Size())
		time.Sleep(500 * time.Millisecond)
	}

	t.Logf("All background tasks done, took: %s", time.Since(before))

	index.searchProbe = 64
	recall, latency := testinghelpers.RecallAndLatency(t.Context(), queries, k, index, neighbors)
	t.Logf("searchProbe=%d, recall=%.4f, latency=%.2f", index.searchProbe, recall, latency)

	index.searchProbe = 128
	recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, neighbors)
	t.Logf("searchProbe=%d, recall=%.4f, latency=%.2f", index.searchProbe, recall, latency)

	index.searchProbe = 256
	recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, neighbors)
	t.Logf("searchProbe=%d, recall=%.4f, latency=%.2f", index.searchProbe, recall, latency)

	index.searchProbe = 512
	recall, latency = testinghelpers.RecallAndLatency(t.Context(), queries, k, index, neighbors)
	t.Logf("searchProbe=%d, recall=%.4f, latency=%.2f", index.searchProbe, recall, latency)
}
