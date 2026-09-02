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

package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type centroidPrefillNoopBucketView struct{}

func (centroidPrefillNoopBucketView) ReleaseView() {}

// newCentroidLikeIndex builds an hnsw index shaped like hfresh's centroid
// graph: TargetVector "" (no object-vector identity of its own — see
// shard_init_vector.go's Centroids.HNSWConfig literal) and, when
// installVectorFromObject is true, the same VectorFromObject sentinel that
// literal installs on the real centroid config. store must already hold an
// objects bucket, matching prefillCacheParallel's expectations.
func newCentroidLikeIndex(t *testing.T, store *lsmkv.Store, installVectorFromObject bool) *hnsw {
	t.Helper()
	rootPath := t.TempDir()
	cfg := Config{
		RootPath:         rootPath,
		ID:               "vectors_tv_centroids",
		TargetVector:     "",
		Logger:           logrus.New(),
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			// Centroid graphs never look object vectors up by id either — hfresh
			// forces this same (nil, nil) thunk (see hfresh/hnsw.go NewHNSWIndex).
			return nil, nil
		},
		GetViewThunk: func() common.BucketView { return centroidPrefillNoopBucketView{} },
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(rootPath, "vectors_tv_centroids", logrus.New(),
				cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		MakeBucketOptions:   lsmkv.MakeNoopBucketOptions,
		AllocChecker:        memwatch.NewDummyMonitor(),
		WaitForCachePrefill: true,
	}
	if installVectorFromObject {
		cfg.VectorFromObject = func(objectBytes []byte) ([]float32, error) {
			return nil, nil
		}
	}

	uc := enthnsw.NewDefaultUserConfig()
	uc.VectorCacheMaxObjects = 1e12

	idx, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	return idx
}

// TestCentroidGraphNeverPrefillsFromObjectStorage pins hfresh's centroid-graph
// invariant: the centroid hnsw has no object-vector identity of its own — its
// vectors only ever arrive via Insert/Add (see hfresh/hnsw.go) — so its cache
// must never be populated by scanning the shard's objects bucket, no matter
// what real vectors that bucket holds.
//
// Before physical-ID plumbing (PR2 Task 2), the centroid hnsw's ID carried a
// "_centroids" suffix ("vectors_<tv>_centroids") that getTargetVector()
// stripped down to a nonexistent target-vector name ("<tv>_centroids"), so
// the parallel prefiller's per-object lookup always missed and silently
// no-opped. TargetVector is now explicit and empty for centroids, which —
// left unguarded — makes that same fallback resolve to the LEGACY vector
// name and read real object vectors into the centroid cache.
// shard_init_vector.go closes that gap with an explicit VectorFromObject
// override; this test pins both the failure mode it closes and the fix.
func TestCentroidGraphNeverPrefillsFromObjectStorage(t *testing.T) {
	ctx := context.Background()

	t.Run("without the centroid VectorFromObject override, empty TargetVector leaks legacy object vectors", func(t *testing.T) {
		store := newTestObjectsStore(t)
		bucket := store.Bucket(helpers.ObjectsBucketLSM)
		putTestObject(t, bucket, 1, []float32{0.1, 0.2, 0.3}, nil)
		putTestObject(t, bucket, 2, []float32{0.4, 0.5, 0.6}, nil)

		idx := newCentroidLikeIndex(t, store, false)
		t.Cleanup(func() { idx.Shutdown(context.Background()) })

		require.NoError(t, idx.prefillCacheParallel(ctx))

		// Documents the failure mode the fix closes: with no VectorFromObject
		// override, an empty TargetVector falls back to reading the legacy
		// vector, so the centroid cache ends up holding real OBJECT vectors
		// keyed by doc ID — data that does not belong in the centroid space.
		assert.Equal(t, int64(2), idx.cache.CountVectors(),
			"expected the unguarded fallback to leak legacy object vectors into the centroid cache")
	})

	t.Run("with the centroid VectorFromObject override, nothing is ever preloaded from object storage", func(t *testing.T) {
		store := newTestObjectsStore(t)
		bucket := store.Bucket(helpers.ObjectsBucketLSM)
		putTestObject(t, bucket, 1, []float32{0.1, 0.2, 0.3}, nil)
		putTestObject(t, bucket, 2, []float32{0.4, 0.5, 0.6}, nil)

		idx := newCentroidLikeIndex(t, store, true)
		t.Cleanup(func() { idx.Shutdown(context.Background()) })

		require.NoError(t, idx.prefillCacheParallel(ctx))

		assert.Equal(t, int64(0), idx.cache.CountVectors(),
			"centroid graph must never prefill from object storage")
	})
}
