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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestSegmentGroupReconcileMapToInverted pins that reopening a bucket as
// StrategyMapCollection whose first segment is StrategyInverted reconciles to
// Inverted — the TODO-marked branch a re-run change-algorithm depends on to
// avoid silently corrupting map/BM25 reads.
func TestSegmentGroupReconcileMapToInverted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	term := []byte("reconcileterm")

	// Write postings into an inverted bucket and flush to a .db segment.
	inv, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLoggerB(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	inv.SetMemtableThreshold(1e9)
	for id := uint64(1); id <= 25; id++ {
		require.NoError(t, inv.MapSet(term, NewMapPairFromDocIdAndTf(id, float32(id), 1, false)))
	}
	require.NoError(t, inv.FlushAndSwitch())

	// Baseline postings read while the bucket is genuinely inverted.
	wantInverted, err := inv.MapList(ctx, term)
	require.NoError(t, err)
	require.Len(t, wantInverted, 25)
	require.NoError(t, inv.Shutdown(ctx))

	// Reopen the SAME directory as a StrategyMapCollection bucket — the state a
	// re-submitted change-algorithm on an already-blockmax property produces.
	reopened, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLoggerB(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyMapCollection))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })

	// The reconcile branch flips the live strategy to Inverted despite the
	// MapCollection open request.
	require.Equal(t, StrategyInverted, reopened.Strategy(),
		"opened as map collection but first on-disk segment is inverted -> reconcile to inverted")

	// The non-corruption guarantee: a map/BM25 read returns identical postings.
	gotAsMap, err := reopened.MapList(ctx, term)
	require.NoError(t, err)
	require.Equal(t, wantInverted, gotAsMap,
		"map-requested read over inverted segments must return identical postings")
}
