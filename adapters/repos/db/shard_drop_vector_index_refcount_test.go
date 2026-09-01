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

package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// dropVectorIndexInBackground starts the drop and returns the channel its
// result lands on.
func dropVectorIndexInBackground(ctx context.Context, shard *Shard, targetVector string) chan error {
	dropped := make(chan error, 1)
	enterrors.GoWrapper(func() {
		dropped <- shard.DropVectorIndex(ctx, targetVector)
	}, shard.index.logger)
	return dropped
}

// requireVectorIndexPinnable asserts whether a reference can be taken on
// targetVector right now, releasing it either way.
func requireVectorIndexPinnable(t *testing.T, shard *Shard, targetVector string, want bool) {
	t.Helper()

	_, release, ok := shard.pinVectorIndex(targetVector)
	release()
	require.Equal(t, want, ok)
}

func requireVectorIndexDropped(t *testing.T, dropped chan error) {
	t.Helper()
	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(time.Minute): // the drain window plus the teardown behind it
		t.Fatal("drop never completed")
	}
}

// TestDropVectorIndexWaitsForInFlightReferences is the point of the drain: a
// search that resolved the index before the drop keeps working on it, and the
// drop waits rather than removing the buckets underneath it.
func TestDropVectorIndexWaitsForInFlightReferences(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "a", true)))

	index, release, ok := shard.pinVectorIndex("foo")
	require.True(t, ok)

	dropped := dropVectorIndexInBackground(ctx, shard, "foo")

	select {
	case err := <-dropped:
		t.Fatalf("drop completed while a reference was still held: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	// the whole point: the pinned index still reads its own buckets
	ids, _, err := index.SearchByVector(ctx, []float32{1, 2, 3}, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ids)

	// and no new reference may sneak in behind the drain
	requireVectorIndexPinnable(t, shard, "foo", false)

	release()
	requireVectorIndexDropped(t, dropped)

	requireVectorIndexPinnable(t, shard, "foo", false)
}

// TestDropVectorIndexProceedsWhenDrainTimesOut pins the escape hatch: the drain
// is bounded on purpose, so a reference held past the window must not wedge the
// drop, and must be logged. Runs for the full drain window (~30s).
func TestDropVectorIndexProceedsWhenDrainTimesOut(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "a", true)))

	logger, hook := test.NewNullLogger()
	shard.index.logger = logger

	_, release, ok := shard.pinVectorIndex("foo") // never released before the drop
	require.True(t, ok)
	defer release()

	start := time.Now()
	requireVectorIndexDropped(t, dropVectorIndexInBackground(ctx, shard, "foo"))
	// ~30s window; near-instant means it never waited
	require.Greater(t, time.Since(start), 10*time.Second, "drop gave up well short of the drain window")

	var warned bool
	for _, e := range hook.AllEntries() {
		warned = warned || (e.Level == logrus.ErrorLevel &&
			strings.Contains(e.Message, "proceeding with drop while references are still held"))
	}
	require.True(t, warned, "a drop that outran its drain must be logged, not silent")
}

// TestVectorIndexDropClaimIsReleased covers the states a claim must not
// outlive: a claim blocks every reference to its target, so one left behind
// would leave a live vector unqueryable.
func TestVectorIndexDropClaimIsReleased(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, ctx context.Context, shard *Shard)
	}{
		{
			name: "vector re-created after a completed drop",
			run: func(t *testing.T, ctx context.Context, shard *Shard) {
				require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
				require.NoError(t, shard.initTargetVector(ctx, "foo", hnsw.NewDefaultUserConfig(), false))
			},
		},
		{
			// the release a drop runs when it gives up before the vector is
			// gone. Checked on the claim itself: since the teardown got its own
			// budget, no caller-side input reaches that path any more.
			name: "claim released",
			run: func(t *testing.T, ctx context.Context, shard *Shard) {
				_, release := shard.claimVectorIndexDrop("foo")
				requireVectorIndexPinnable(t, shard, "foo", false)
				release()
			},
		},
		{
			// a re-enqueued drop overlapping the one it retries: the first to
			// finish must not re-open a target the other is still draining for
			name: "one of two overlapping claims released",
			run: func(t *testing.T, ctx context.Context, shard *Shard) {
				_, releaseFirst := shard.claimVectorIndexDrop("foo")
				_, releaseSecond := shard.claimVectorIndexDrop("foo")

				releaseFirst()
				requireVectorIndexPinnable(t, shard, "foo", false)

				releaseSecond()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := testCtx()
			shard, _ := setupDropVectorShard(t, ctx)

			test.run(t, ctx, shard)

			_, release, ok := shard.pinVectorIndex("foo")
			defer release()
			require.True(t, ok, "vector must be usable again")
		})
	}
}
