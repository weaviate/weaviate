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
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestCancelRaceWidenedByChunking_SilentSuccessInsteadOfContextError is a RED
// (pinning) regression test for a real, empirically-verified finding
// discovered while writing the C1 (chunked bulk resolution, GH 12242) test
// suite -- documented in Conversations/2026-07/2026-07-18-1246__pr-prep.md
// -- that goes beyond what the architect design-gate's static code review
// found. It is deliberately NOT fixed in the C1 diff (branch
// fix/containsany-chunked-resolution-12242): this branch carries the exact
// C1 commit plus only this diagnostic test, kept off the PR branch entirely.
//
// Finding: baseline spawns one errgroup goroutine per leaf, so a cancelled
// query gets thousands of independent per-leaf "chances" to observe context
// cancellation via each leaf's own resolveDocIDs ctxExpired check (returns
// a real, non-nil error that propagates to the caller). C1's chunking
// collapses this to numChunks goroutines (bounded by
// outerConcurrencyLimit-1), so a cancelled query gets only numChunks
// "chances." Within a single chunk, ONLY the very first post-cancellation
// leaf observed inside that chunk can race into the leaf-level ctxExpired
// error path (Path B); every leaf AFTER that in the same chunk is instead
// caught by the between-leaf gctx.Err() check in resolveDocIDsAndOr, which
// returns nil silently (Path A, identical in baseline -- this half of the
// behavior is NOT new). With far fewer independent goroutines each getting
// far fewer "rolls of the dice," it becomes measurably more likely that
// EVERY chunk happens to observe cancellation via Path A only, in which
// case resolveDocIDsAndOr's eg.Wait() returns nil (no chunk ever returned a
// real error) and the query returns a SILENT SUCCESS with an
// incomplete/empty result instead of a context error to the caller.
//
// Verified empirically (see the referenced brief for the full numbers):
// this exact fixture run 24 times against baseline never hit the silent-
// success outcome; against C1 it hit 1/24. This test amplifies that signal
// with many more attempts so it reliably shows red on this branch (C1
// present) and would show green on a baseline-only branch.
func TestCancelRaceWidenedByChunking_SilentSuccessInsteadOfContextError(t *testing.T) {
	const propName = "inverted-text-roaringset"
	const numValues = 5000
	const cancelDelay = 2 * time.Millisecond
	const attempts = 100 // amplifies a ~1/24 empirical rate to a near-certain repro

	silentSuccesses := 0
	for attempt := 0; attempt < attempts; attempt++ {
		dirName := t.TempDir()
		logger, _ := test.NewNullLogger()

		store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)

		maxDocID := uint64(numValues*3 + 10)
		bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(maxDocID))
		searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
			stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
			config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

		bucketName := helpers.BucketFromPropNameLSM(propName)
		require.NoError(t, store.CreateOrLoadBucket(context.Background(), bucketName,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		))
		bucket := store.Bucket(bucketName)

		values := make([]string, numValues)
		for i := 0; i < numValues; i++ {
			val := fmt.Sprintf("val_%05d", i)
			values[i] = val
			ids := []uint64{uint64(3 * i), uint64(3*i + 1), uint64(3*i + 2)}
			require.NoError(t, bucket.RoaringSetAddList([]byte(val), ids))
		}
		require.NoError(t, bucket.FlushAndSwitch())

		filter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.ContainsAny,
				On:       &filters.Path{Class: className, Property: schema.PropertyName(propName)},
				Value:    &filters.Value{Value: values, Type: schema.DataTypeText},
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancelFired := make(chan struct{})
		go func() {
			time.Sleep(cancelDelay)
			cancel()
			close(cancelFired)
		}()

		allowList, err := searcher.DocIDs(ctx, filter, additional.Properties{}, className)
		<-cancelFired
		cancel()
		store.Shutdown(context.Background())

		if err == nil {
			silentSuccesses++
			t.Logf("attempt %d: SILENT SUCCESS after cancel, len=%d (expected a context error)",
				attempt, allowList.Len())
			allowList.Close()
		}
	}

	// RED: this assertion is expected to FAIL on this branch (C1 present),
	// which is exactly the point -- it pins the finding described in the
	// file-level comment above so it shows up as a named failure for
	// whoever picks up the follow-up issue, rather than a probabilistic bug
	// nobody tracks. It is expected to PASS (0 silent successes) if run
	// against baseline code without C1's chunking.
	require.Zero(t, silentSuccesses,
		"expected 0 silent-success-after-cancel outcomes across %d attempts against chunked "+
			"resolveDocIDsAndOr, got %d -- chunking narrows the number of independent goroutines "+
			"that get a 'chance' to observe ctx cancellation via a real error (numChunks instead "+
			"of one per leaf), measurably widening the pre-existing race window where a cancelled "+
			"query returns a silent, incomplete success instead of a context error; see the "+
			"file-level comment for the full mechanism and Conversations/2026-07/"+
			"2026-07-18-1246__pr-prep.md for the empirical baseline-vs-chunked comparison",
		attempts, silentSuccesses)
}
