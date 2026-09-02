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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type centroidPrefillNoopBucketView struct{}

func (centroidPrefillNoopBucketView) ReleaseView() {}

// TestPrefillWithoutVectorFromObjectNeverScansObjectStorage pins the
// structural guarantee that replaced the old TargetVector-based fallback: an
// index built with no VectorFromObject — the shape of hfresh's centroid
// graph, which has no object-vector identity of its own (see
// hfresh/hnsw.go) — must never read the objects bucket during startup
// prefill, no matter what it finds there. useParallelPrefill only takes the
// parallel objects-bucket scan when h.vectorFromObject is set (see
// startup.go); a nil closure here takes the serial, VectorForIDThunk-based
// path instead, which this test's thunk starves on purpose (returns nil,
// nil for every id) so any accidental object-storage read would show up as
// cached vectors.
//
// hfresh.NewHNSWIndex used to install a VectorFromObject override for
// exactly this reason; TestCentroidPrefillNeverReadsObjectStorage in the
// hfresh package now pins the same guarantee through the real hfresh.New()
// construction path, by construction rather than by override.
func TestPrefillWithoutVectorFromObjectNeverScansObjectStorage(t *testing.T) {
	store := testinghelpers.NewTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	testinghelpers.PutTestObject(t, bucket, 1, []float32{0.1, 0.2, 0.3}, nil)
	testinghelpers.PutTestObject(t, bucket, 2, []float32{0.4, 0.5, 0.6}, nil)

	rootPath := t.TempDir()
	cfg := Config{
		RootPath:            rootPath,
		ID:                  "vectors_tv_centroids",
		Logger:              logrus.New(),
		DistanceProvider:    distancer.NewCosineDistanceProvider(),
		WaitForCachePrefill: true,
		VectorForIDThunk:    func(ctx context.Context, id uint64) ([]float32, error) { return nil, nil },
		GetViewThunk:        func() common.BucketView { return centroidPrefillNoopBucketView{} },
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(rootPath, "vectors_tv_centroids", logrus.New(),
				cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		// VectorFromObject is deliberately left unset: this is the shape of
		// an index with no object-vector identity of its own.
	}

	uc := enthnsw.NewDefaultUserConfig()
	uc.VectorCacheMaxObjects = 1e12

	idx, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })

	idx.PostStartup(context.Background())

	assert.Equal(t, int64(0), idx.cache.CountVectors(),
		"an index without VectorFromObject must never scan the objects bucket during prefill")
}
