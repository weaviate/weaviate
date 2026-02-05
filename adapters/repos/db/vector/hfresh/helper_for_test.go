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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type TestHFresh struct {
	Index *HFresh
	Logs  *test.Hook
}

func createHFreshIndex(t *testing.T) TestHFresh {
	t.Helper()

	// Create logger + hook
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	cfg := DefaultConfig()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	cfg.Scheduler = scheduler
	cfg.RootPath = t.TempDir()

	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger

	scheduler.Start()
	t.Cleanup(func() {
		scheduler.Close()
	})

	uc := ent.NewDefaultUserConfig()
	store := testinghelpers.NewDummyStore(t)

	index, err := New(cfg, uc, store)
	require.NoError(t, err)

	return TestHFresh{
		Index: index,
		Logs:  hook,
	}
}

// createTestVectors creates a set of test vectors with the specified dimensions
func createTestVectors(dims int, count int) [][]float32 {
	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i*dims + j)
		}
		vectors[i] = vec
	}
	return vectors
}

// addVectorToIndex initializes dimensions if needed and adds a vector to the index
func addVectorToIndex(t *testing.T, tf *TestHFresh, vectorID uint64, vector []float32) {
	t.Helper()
	err := tf.Index.Add(t.Context(), vectorID, vector)
	require.NoError(t, err)
}

// initializeDimensions initializes the index dimensions without adding a vector
// (adding a vector via Add() would create a centroid as a side effect)
func initializeDimensions(t *testing.T, tf *TestHFresh, vector []float32) {
	t.Helper()
	tf.Index.initDimensionsOnce.Do(func() {
		size := uint32(len(vector))
		tf.Index.dims = size
		err := tf.Index.setMaxPostingSize()
		require.NoError(t, err)
		tf.Index.quantizer = compressionhelpers.NewBinaryRotationalQuantizer(int(tf.Index.dims), 42, tf.Index.config.DistanceProvider)
		tf.Index.Centroids.SetQuantizer(tf.Index.quantizer)
		tf.Index.distancer = &Distancer{
			quantizer: tf.Index.quantizer,
			distancer: tf.Index.config.DistanceProvider,
		}
	})
}

// createPostingWithVectors creates a posting with the given vectors
func createPostingWithVectors(t *testing.T, tf *TestHFresh, vectors [][]float32, startID uint64) (uint64, Posting) {
	t.Helper()

	// Initialize dimensions without adding a vector to the index
	if len(vectors) > 0 {
		initializeDimensions(t, tf, vectors[0])
	}

	postingID, err := tf.Index.IDs.Next()
	require.NoError(t, err)

	posting := Posting{}
	for i, vec := range vectors {
		vectorID := startID + uint64(i)
		version := VectorVersion(1)

		err := tf.Index.VersionMap.store.Set(t.Context(), vectorID, version)
		require.NoError(t, err)

		compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(vec))
		v := NewVector(vectorID, version, compressed)
		posting = posting.AddVector(v)
	}

	return postingID, posting
}
