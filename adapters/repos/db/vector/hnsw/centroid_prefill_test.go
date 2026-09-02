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

// TestParallelPrefillFallsBackToLegacyVectorWhenTargetVectorEmpty pins the
// hnsw-level mechanism behind a real bug fixed in hfresh: an index whose
// TargetVector is "" — the shape of hfresh's centroid graph, which has no
// object-vector identity of its own (see hfresh/hnsw.go) — and which carries
// no VectorFromObject override falls back, on a parallel cache-prefill scan,
// to reading the LEGACY object vector for every object in the objects
// bucket.
//
// hfresh's centroid graph must never be fed this way: its vectors only ever
// arrive via Insert/Add. hfresh.NewHNSWIndex installs a VectorFromObject
// override that explicitly skips every object, and
// TestCentroidPrefillNeverReadsObjectStorage in the hfresh package pins that
// override through the real hfresh.New() construction path — deleting the
// override there fails that test. This test is narrower and lower-level: it
// does not touch the override at all, and instead pins the fallback
// mechanism the override exists to guard against, so a future change to
// hnsw's own TargetVector-empty resolution (e.g. no longer defaulting to the
// legacy vector) is caught here even though it wouldn't otherwise be visible
// through hfresh's wiring test.
func TestParallelPrefillFallsBackToLegacyVectorWhenTargetVectorEmpty(t *testing.T) {
	store := testinghelpers.NewTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	testinghelpers.PutTestObject(t, bucket, 1, []float32{0.1, 0.2, 0.3}, nil)
	testinghelpers.PutTestObject(t, bucket, 2, []float32{0.4, 0.5, 0.6}, nil)

	rootPath := t.TempDir()
	cfg := Config{
		RootPath:         rootPath,
		ID:               "vectors_tv_centroids",
		TargetVector:     "", // the shape of a centroid graph's config
		Logger:           logrus.New(),
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) { return nil, nil },
		GetViewThunk:     func() common.BucketView { return centroidPrefillNoopBucketView{} },
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(rootPath, "vectors_tv_centroids", logrus.New(),
				cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		// VectorFromObject is deliberately left unset: this test exercises
		// the fallback the hfresh-installed override exists to bypass.
	}

	uc := enthnsw.NewDefaultUserConfig()
	uc.VectorCacheMaxObjects = 1e12

	idx, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })

	require.NoError(t, idx.prefillCacheParallel(context.Background()))

	assert.Equal(t, int64(2), idx.cache.CountVectors(),
		`expected the unguarded TargetVector="" fallback to read legacy object vectors`)
}
