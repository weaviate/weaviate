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

//go:build integrationTest

package inverted

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// newIdxBucket creates a temporary lsmkv store and an empty RoaringSet bucket
// for use as the _idx meta bucket in nested executor tests.
func newIdxBucket(t *testing.T) *lsmkv.Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(t.TempDir(), t.TempDir(), logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	require.NoError(t, store.CreateOrLoadBucket(context.Background(),
		"testmeta", lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	return store.Bucket("testmeta")
}

// writeIdx writes positions for a single array element into the meta bucket.
// positions should already have the real docID encoded (not docID=0 templates).
func writeIdx(t *testing.T, bucket *lsmkv.Bucket, path string, elemIdx int, positions []uint64) {
	t.Helper()
	require.NoError(t, bucket.RoaringSetAddList(invnested.IdxKey(path, elemIdx), positions))
}

// newTrackingPool returns a BitmapBufPoolTracking that asserts at test cleanup
// time that every leased buffer has been returned. Detects pool-buffer leaks
// in the recursive executor.
func newTrackingPool(t *testing.T) *roaringset.BitmapBufPoolTracking {
	t.Helper()
	pool := roaringset.NewBitmapBufPoolTracking()
	t.Cleanup(func() {
		if n := pool.Outstanding(); n != 0 {
			t.Errorf("pool: %d bitmap buffer(s) not released", n)
		}
	})
	return pool
}

// requireBitmapValid asserts that bm's backing buffer has not been zeroed by
// the tracking pool's release function. A zeroed buffer has NumContainers()==0,
// indicating premature release.
func requireBitmapValid(t *testing.T, bm *sroar.Bitmap) {
	t.Helper()
	require.NotNil(t, bm)
	require.Positive(t, bm.NumContainers(), "bitmap backing buffer is zeroed — premature release?")
}

// newLifecycleOps returns a BitmapOps backed by a tracking pool.
func newLifecycleOps(t *testing.T) *invnested.BitmapOps {
	t.Helper()
	return invnested.NewBitmapOps(newTrackingPool(t))
}
