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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// A teardown deregisters the objects bucket while queries are still in flight,
// and Store.Bucket then returns nil by design. Taking a view straight off that
// nil pointer dereferences it and takes the node down, so every path that
// needs the objects bucket has to report it missing instead.
func TestObjectsBucketViewReportsMissingBucket(t *testing.T) {
	ctx := context.Background()

	vectors, _ := testinghelpers.RandomVecs(64, 1, 32)
	index := newSearchTestIndex(t, vectors, nil)

	require.NoError(t, index.store.ShutdownBucket(ctx, helpers.ObjectsBucketLSM))

	_, _, err := index.objectsBucketView()
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}

func TestSearchReportsMissingObjectsBucket(t *testing.T) {
	ctx := context.Background()

	vectors, queries := testinghelpers.RandomVecs(64, 1, 32)
	index := newSearchTestIndex(t, vectors, nil)

	require.NoError(t, index.store.ShutdownBucket(ctx, helpers.ObjectsBucketLSM))

	_, _, err := index.SearchByVector(ctx, queries[0], 10, nil)
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}
