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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func makePostingSizes(t *testing.T) *PostingSizes {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)
	metrics := NewMetrics(monitoring.GetMetrics(), "", "")
	postingSizes, err := NewPostingSizes(bucket, metrics)
	require.NoError(t, err)
	return postingSizes
}

func TestPostingSizes(t *testing.T) {
	ctx := t.Context()

	t.Run("get unknown posting", func(t *testing.T) {
		postingSizes := makePostingSizes(t)

		_, err := postingSizes.Get(ctx, 1)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})

	t.Run("set and get posting size", func(t *testing.T) {
		postingSizes := makePostingSizes(t)

		err := postingSizes.Set(ctx, 1, 42)
		require.NoError(t, err)

		size, err := postingSizes.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, uint32(42), size)
	})

	t.Run("list posting ids", func(t *testing.T) {
		postingSizes := makePostingSizes(t)

		for i := uint64(1); i <= 50; i++ {
			size := uint32(i * 10)
			err := postingSizes.Set(ctx, i, size)
			require.NoError(t, err)
		}

		for i := uint64(1); i <= 100; i += 5 {
			_, err := postingSizes.Inc(ctx, i, 5)
			require.NoError(t, err)
		}

		for i := uint64(1); i <= 100; i += 10 {
			err := postingSizes.Set(ctx, i, 0)
			require.NoError(t, err)
		}

		ids, err := postingSizes.ListPostingIDs(ctx)
		require.NoError(t, err)
		require.Len(t, ids, 50)

		// simulate a restart
		postingSizes, err = NewPostingSizes(postingSizes.store.bucket, postingSizes.metrics)
		require.NoError(t, err)

		ids, err = postingSizes.ListPostingIDs(ctx)
		require.NoError(t, err)
		require.Len(t, ids, 50)
	})
}
