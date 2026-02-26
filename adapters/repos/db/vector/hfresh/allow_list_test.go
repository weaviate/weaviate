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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestPostingPassesFilterSlice_PassesIfAddsNewVector(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket, NewMetrics(monitoring.GetMetrics(), "n/a", "n/a"))
	ctx := t.Context()

	// posting 0 has allowed vector 0
	pm.FastAddVectorID(ctx, 0, 0, 0)

	// posting 1 has allowed vector 1 (different allowed vector)
	pm.FastAddVectorID(ctx, 1, 1, 0)

	hf := &HFresh{
		visitedPool: visited.NewPool(10, 10, 10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0, 1)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0) // pass, consumes vector 0
	require.True(t, ok)

	ok = hfAllowList.Contains(1) // pass, consumes vector 1 (new contribution)
	require.True(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_IgnoresDisallowedVectors(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket, NewMetrics(monitoring.GetMetrics(), "n/a", "n/a"))
	ctx := t.Context()

	// postings only have vectors that are NOT in allowList
	pm.FastAddVectorID(ctx, 0, 10, 0)
	pm.FastAddVectorID(ctx, 0, 11, 0)
	pm.FastAddVectorID(ctx, 1, 12, 0)

	hf := &HFresh{
		visitedPool: visited.NewPool(10, 10, 10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0) // only vector 0 allowed
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0)
	require.False(t, ok)

	ok = hfAllowList.Contains(1)
	require.False(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_SkipsAlreadyUsedAndFindsLaterNewOne(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket, NewMetrics(monitoring.GetMetrics(), "n/a", "n/a"))
	ctx := t.Context()

	// posting 0 consumes vector 0
	pm.FastAddVectorID(ctx, 0, 0, 0)

	// posting 1 has [0, 1], where 0 will be already used after checking posting 0
	pm.FastAddVectorID(ctx, 1, 0, 0)
	pm.FastAddVectorID(ctx, 1, 1, 0)

	hf := &HFresh{
		visitedPool: visited.NewPool(10, 10, 10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0, 1)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0) // pass, consumes vector 0
	require.True(t, ok)

	ok = hfAllowList.Contains(1) // should still pass, because it can contribute vector 1
	require.True(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_StopsOnFirstNewContribution(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket, NewMetrics(monitoring.GetMetrics(), "n/a", "n/a"))
	ctx := t.Context()

	// posting 0 has [0, 1]. It should consume ONLY 0 (first new allowed) and stop.
	pm.FastAddVectorID(ctx, 0, 0, 0)
	pm.FastAddVectorID(ctx, 0, 1, 0)

	// posting 1 has only [1]. If posting 0 incorrectly consumed both 0 and 1, this would fail.
	pm.FastAddVectorID(ctx, 1, 1, 0)

	hf := &HFresh{
		visitedPool: visited.NewPool(10, 10, 10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0, 1)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0) // pass, should consume vector 0 only
	require.True(t, ok)

	ok = hfAllowList.Contains(1) // must pass if 1 was NOT consumed earlier
	require.True(t, ok)

	hfAllowList.Close()
}

func TestPostingPassesFilterSlice_CachesAcceptedPostingID(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	pm := NewPostingMap(bucket, NewMetrics(monitoring.GetMetrics(), "n/a", "n/a"))
	ctx := t.Context()

	// posting 0 has allowed vector 0
	pm.FastAddVectorID(ctx, 0, 0, 0)

	hf := &HFresh{
		visitedPool: visited.NewPool(10, 10, 10),
		PostingMap:  pm,
	}

	allowList := helpers.NewAllowList(0)
	hfAllowList := hf.wrapAllowList(ctx, allowList)

	ok := hfAllowList.Contains(0)
	require.True(t, ok)

	// Second call should return true regardless of "new contribution" because the posting ID is cached as visited.
	ok = hfAllowList.Contains(0)
	require.True(t, ok)

	hfAllowList.Close()
}
