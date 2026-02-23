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

package hnsw

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	annDatasets "github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type adaptiveEFTestConfig struct {
	dataset  string
	distance string
}

func getDistProvider(distance string) distancer.Provider {
	switch distance {
	case "cosine":
		return distancer.NewCosineDistanceProvider()
	case "dot":
		return distancer.NewDotProductProvider()
	default:
		return distancer.NewL2SquaredProvider()
	}
}

func Test_NoRace_AdaptiveEFRecallParquet(t *testing.T) {
	testConfigs := []adaptiveEFTestConfig{
		{
			dataset:  "fiqa-st-minilm-384-dot-12k",
			distance: "dot",
		},
		{
			dataset:  "dbpedia-openai-ada002-1536-angular-20k",
			distance: "cosine",
		},
	}

	targetRecalls := []float32{0.95, 0.99}

	for _, tc := range testConfigs {
		for _, targetRecall := range targetRecalls {
			name := fmt.Sprintf("%s_%s_recall%.0f", tc.dataset, tc.distance, targetRecall*100)
			tc := tc
			targetRecall := targetRecall
			t.Run(name, func(t *testing.T) {
				runAdaptiveEFRecallTest(t, tc, targetRecall)
			})
		}
	}
}

func runAdaptiveEFRecallTest(t *testing.T, tc adaptiveEFTestConfig, targetRecall float32) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	k := 10

	// Load dataset
	hf := annDatasets.NewHubDataset("weaviate/ann-datasets", tc.dataset)

	t.Log("Loading training data...")
	before := time.Now()
	ids, vectors, err := hf.LoadTrainData()
	require.NoError(t, err)
	t.Logf("Loaded %d training vectors in %s", len(ids), time.Since(before))

	t.Log("Loading test data...")
	neighbors, queries, err := hf.LoadTestData()
	require.NoError(t, err)
	t.Logf("Loaded %d test queries in %s", len(queries), time.Since(before))

	idToIndex := make(map[uint64]int, len(ids))
	for i, id := range ids {
		idToIndex[id] = i
	}

	dp := getDistProvider(tc.distance)
	store := testinghelpers.NewDummyStore(t)

	vectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
		if idx, ok := idToIndex[id]; ok {
			return vectors[idx], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", id)
	}

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "adaptive-ef-recall-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      dp,
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk:      vectorForID,
		GetViewThunk:          GetViewThunk,
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			return vectorForID(ctx, id)
		},
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		EF:                    64,
		VectorCacheMaxObjects: 1000000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	// Insert vectors concurrently
	t.Log("Indexing vectors...")
	before = time.Now()
	compressionhelpers.Concurrently(logger, uint64(len(ids)), func(i uint64) {
		err := index.Add(ctx, ids[i], vectors[i])
		require.NoError(t, err)
	})
	t.Logf("Indexing done in %s", time.Since(before))

	// Measure baseline recall (fixed EF=64)
	baselineRecall, baselineLatency := testinghelpers.RecallAndLatency(ctx, queries, k, index, neighbors)
	t.Logf("Baseline (EF=64): recall=%.4f, latency=%.2fus", baselineRecall, baselineLatency)

	// Calibrate adaptive EF
	t.Logf("Calibrating adaptive EF with target recall %.2f...", targetRecall)
	before = time.Now()
	err = index.CalibrateAdaptiveEF(ctx, targetRecall)
	require.NoError(t, err)
	t.Logf("Calibration done in %s", time.Since(before))

	cfg := index.adaptiveEf.Load()
	require.NotNil(t, cfg, "adaptive ef config should be set after calibration")
	t.Logf("Adaptive EF config: WAE=%d, table_entries=%d", cfg.WeightedAverageEf, len(cfg.Table))

	// Measure adaptive recall
	adaptiveRecall, adaptiveLatency := testinghelpers.RecallAndLatency(ctx, queries, k, index, neighbors)
	t.Logf("Adaptive (target=%.2f): recall=%.4f, latency=%.2fus", targetRecall, adaptiveRecall, adaptiveLatency)

	// Assert recall is within 5% of target
	minAcceptableRecall := targetRecall - 0.05
	require.GreaterOrEqualf(t, adaptiveRecall, minAcceptableRecall,
		"Adaptive recall %.4f should be within 8%% of target %.2f (min acceptable: %.2f)",
		adaptiveRecall, targetRecall, minAcceptableRecall)
}
