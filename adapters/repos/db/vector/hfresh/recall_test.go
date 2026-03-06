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

//go:build integrationTest && !race

package hfresh

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

var (
	distanceMethod = flag.String("distance", "l2-squared", "distance method: l2-squared, cosine, or dot")
	dataset        = flag.String("dataset", "", "ann dataset i.e. fiqa-st-minilm-384-dot-12k")
)

type testConfig struct {
	dataset        string
	distance       string
	searchProbes   []uint32
	requiredRecall float32
}

func getDistanceProvider(distance string) distancer.Provider {
	switch distance {
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

func Test_NoRace_HFreshRecallParquet(t *testing.T) {
	// Define test configurations
	testConfigs := []testConfig{
		{
			dataset:        "fiqa-st-minilm-384-dot-12k",
			distance:       "dot",
			searchProbes:   []uint32{128},
			requiredRecall: 0.6,
		},
		{
			dataset:        "beir-cohere-v3-1024-euclidean-20k",
			distance:       "l2-squared",
			searchProbes:   []uint32{64},
			requiredRecall: 0.8,
		},
		{
			dataset:        "dbpedia-openai-ada002-1536-angular-20k",
			distance:       "cosine",
			searchProbes:   []uint32{64},
			requiredRecall: 0.8,
		},
	}

	var testsToRun []testConfig
	if *dataset != "" {
		// If dataset flag is provided, run that specific test
		testsToRun = []testConfig{
			{
				dataset:        *dataset,
				distance:       *distanceMethod,
				searchProbes:   []uint32{64, 128, 256, 512},
				requiredRecall: 0.5,
			},
		}
	} else {
		testsToRun = testConfigs
	}

	for _, testCfg := range testsToRun {
		t.Run(fmt.Sprintf("%s_%s", testCfg.dataset, testCfg.distance), func(t *testing.T) {
			runRecallTest(t, testCfg)
		})
	}
}

func runRecallTest(t *testing.T, testCfg testConfig) {
	store := testinghelpers.NewDummyStore(t)
	tmpDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.RootPath = tmpDir
	cfg.ID = "hfresh"

	distanceProvider := getDistanceProvider(testCfg.distance)
	cfg.DistanceProvider = distanceProvider
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		GetViewThunk:          getViewThunk,
		DistanceProvider:      distanceProvider,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
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
	hf := datasets.NewHubDataset("weaviate/ann-datasets", testCfg.dataset)

	t.Logf("Using dataset %s, distance metric %s", testCfg.dataset, testCfg.distance)

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
		err := index.IndexMetadata.bucket.FlushAndSwitch()
		require.NoError(t, err)
		err = index.PostingStore.bucket.FlushAndSwitch()
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)
	}

	t.Logf("All background tasks done, took: %s", time.Since(before))

	var maxRecall float32
	for _, probe := range testCfg.searchProbes {
		index.searchProbe = probe
		recall, latency := testinghelpers.RecallAndLatency(t.Context(), queries, k, index, neighbors, nil)
		t.Logf("searchProbe=%d, recall=%.4f, latency=%.2f", index.searchProbe, recall, latency)
		if recall > maxRecall {
			maxRecall = recall
		}
	}

	// Check if recall meets the required threshold
	if testCfg.requiredRecall > 0 {
		require.GreaterOrEqual(t, maxRecall, testCfg.requiredRecall,
			"Maximum recall %.4f did not meet required recall %.4f", maxRecall, testCfg.requiredRecall)
	}
}
