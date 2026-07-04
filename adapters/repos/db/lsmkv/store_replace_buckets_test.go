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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestReplaceBuckets_SucceedsAndServesReplacement pins the happy path that had
// zero coverage: the displaced bucket's post-shutdown updateBucketDir was a
// silent no-op that the new-view refusal turned into an error, failing EVERY
// ReplaceBuckets call (filter→search migration, resetDimensionsLSM, inverted
// reindexer).
func TestReplaceBuckets_SucceedsAndServesReplacement(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	store, err := New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroup("compactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("compactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, "target", WithStrategy(StrategyReplace)))
	require.NoError(t, store.CreateOrLoadBucket(ctx, "replacement", WithStrategy(StrategyReplace)))

	require.NoError(t, store.Bucket("target").Put([]byte("old-key"), []byte("old-val")))
	require.NoError(t, store.Bucket("replacement").Put([]byte("new-key"), []byte("new-val")))
	// flush so the post-replace read exercises the rewritten segment paths,
	// not just the carried-over memtable
	require.NoError(t, store.Bucket("replacement").FlushAndSwitch())

	require.NoError(t, store.ReplaceBuckets(ctx, "target", "replacement"))

	b := store.Bucket("target")
	require.NotNil(t, b)
	got, err := b.Get([]byte("new-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("new-val"), got)

	gone, err := b.Get([]byte("old-key"))
	require.NoError(t, err)
	require.Nil(t, gone, "displaced bucket's data must not survive the replace")

	require.NoError(t, b.Put([]byte("post-key"), []byte("post-val")))
	require.Nil(t, store.Bucket("replacement"), "replacement name must be deregistered")
}
