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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// seedUncompressedCentroidState writes real hnsw node state — but no
// compression record — directly into the commit log directory the hfresh
// centroid graph at (rootPath, centroidID) will read back on its next
// construction. It bypasses HFresh's own Insert path deliberately: HFresh
// forces RQ on for its centroid graph (see NewHNSWIndex), and RQ
// initialization runs synchronously inside the very first insert once the
// cache is marked prefilled — which it always is immediately for a brand
// new, empty graph (see hnsw's restoreFromDisk: "mark the cache as prefilled
// for fresh indexes"). Going through HFresh's Insert here would therefore
// persist a compression record before this function returns, masking
// exactly the "nodes exist, no compression record yet" restart shape this
// test needs. That shape is real in production: a torn commit-log write
// tail, or a failed RQ initialization, leaves a centroid graph exactly like
// this — nodes on disk, no compression record — and it's the case the
// centroid-prefill bug this test pins was found in.
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
	}, enthnsw.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)

	require.NoError(t, idx.Add(context.Background(), 999, vec))
	// Flush so the node write is durable; deliberately do NOT persist any
	// compression record — that gap is exactly what this fixture pins.
	require.NoError(t, idx.Flush())
	require.NoError(t, idx.Shutdown(context.Background()))
}

// TestCentroidPrefillNeverReadsObjectStorage is the wiring-level regression
// test for the bug fixed alongside this test: hfresh's centroid graph has no
// object-vector identity of its own (see hfresh/hnsw.go NewHNSWIndex) — its
// vectors only ever arrive via Insert/Add — but its underlying hnsw.Config
// carries TargetVector "". Left unguarded, restarting a centroid graph that
// has real node state but no persisted compression record — a torn write
// tail, or a failed RQ initialization, both of which leave it in exactly
// this shape — falls back, during its parallel cache-prefill scan, to
// reading the LEGACY object vector and leaks real object data into the
// centroid cache. NewHNSWIndex installs a VectorFromObject override to close
// that gap.
//
// This test builds a real HFresh index — not a hand-assembled hnsw.Config —
// over a store whose objects bucket already holds legacy vectors, with the
// centroid graph's on-disk state pre-seeded (via seedUncompressedCentroidState)
// to the "nodes but no compression" shape the bug depends on. It exercises
// the actual production wiring (hfresh.New -> NewHNSWIndex) rather than the
// fallback mechanism in isolation — removing the VectorFromObject override
// in NewHNSWIndex makes this test fail, with the seeded legacy object
// vectors put into the objects bucket above appearing in the centroid
// cache.
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
	// New() already ran the centroid hnsw's synchronous prefill (its
	// WaitForCachePrefill is forced true by NewHNSWIndex); PostStartup here
	// only mirrors the real shard startup sequence, matching
	// makeHFreshWithConfig.
	index.PostStartup(t.Context())
	t.Cleanup(func() { index.Shutdown(t.Context()) })

	for _, docID := range []uint64{1, 2} {
		centroid, err := index.Centroids.Get(docID)
		require.NoError(t, err)
		assert.Emptyf(t, centroid.Uncompressed,
			"centroid graph must never prefill from object storage, but doc id %d was found in its cache", docID)
	}
}
