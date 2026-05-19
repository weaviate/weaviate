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
)

func makePostingSizes(t *testing.T) *PostingSizes {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, "test", StoreConfig{MakeBucketOptions: lsmkv.MakeNoopBucketOptions})
	require.NoError(t, err)

	return NewPostingSizes(bucket, NewMetrics(nil, "n/a", "n/a"))
}

func TestPostingSizes(t *testing.T) {
	ctx := t.Context()

	t.Run("Set persists after FastIncrement changed memory only", func(t *testing.T) {
		sizes := makePostingSizes(t)

		size := sizes.FastIncrement(42)
		require.EqualValues(t, 1, size)

		_, err := sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)

		err = sizes.Set(ctx, 42, 1)
		require.NoError(t, err)

		persisted, err := sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 1, persisted)
	})

	t.Run("Set unchanged persisted size keeps value", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, 10)
		require.NoError(t, err)

		err = sizes.Set(ctx, 42, 10)
		require.NoError(t, err)

		persisted, err := sizes.store.Get(ctx, 42)
		require.NoError(t, err)
		require.EqualValues(t, 10, persisted)
	})

	t.Run("Set zero on missing posting is a no-op", func(t *testing.T) {
		sizes := makePostingSizes(t)

		err := sizes.Set(ctx, 42, 0)
		require.NoError(t, err)

		_, err = sizes.store.Get(ctx, 42)
		require.ErrorIs(t, err, ErrPostingNotFound)
	})
}
