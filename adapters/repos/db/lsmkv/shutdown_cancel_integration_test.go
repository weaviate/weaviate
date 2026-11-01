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

package lsmkv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucket_ShutdownWithCancelledContext pins weaviate/0-weaviate-issues#213:
// Shutdown on an already-cancelled ctx must still abort in-flight compaction
// instead of letting it run.
func TestBucket_ShutdownWithCancelledContext(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	const segments = 2
	const perSegment = 20000

	compactionCallbacks := cyclemanager.NewCallbackGroup("compaction", nullLogger(), 1)
	compactionCycle := cyclemanager.NewManager("compaction",
		cyclemanager.NewFixedTicker(10*time.Millisecond),
		compactionCallbacks.CycleCallback, nullLogger())

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	flushCycle := cyclemanager.NewManager("flush",
		cyclemanager.NewFixedTicker(10*time.Millisecond),
		flushCallbacks.CycleCallback, nullLogger())

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		compactionCallbacks, flushCallbacks, WithStrategy(StrategyReplace))
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)

	// two on-disk segments so the compaction cycle has real work to merge/abort
	for seg := 0; seg < segments; seg++ {
		for i := 0; i < perSegment; i++ {
			key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
			require.NoError(t, bucket.Put(key, []byte(fmt.Sprintf("value-%d-%d", seg, i))))
		}
		require.NoError(t, bucket.FlushAndSwitch())
	}

	// Tickers aren't started yet, so the count is stable: compaction would
	// otherwise merge these two segments into one.
	require.Equal(t, segments, bucket.disk.Len(),
		"both flushed segments must be on disk before the compaction cycle starts")

	compactionCycle.Start()
	flushCycle.Start()

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	// Shutdown on a dead ctx must still complete cleanly and promptly.
	done := make(chan error, 1)
	go func() { done <- bucket.Shutdown(cancelledCtx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("Bucket.Shutdown hung on a cancelled context")
	}

	require.NoError(t, compactionCycle.StopAndWait(ctx))
	require.NoError(t, flushCycle.StopAndWait(ctx))

	// Reopening on the same dir proves shutdown left consistent on-disk state,
	// even with a compaction aborted mid-merge.
	reopened, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Shutdown(ctx)) }()

	for seg := 0; seg < segments; seg++ {
		for i := 0; i < perSegment; i++ {
			key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
			got, err := reopened.Get(key)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("value-%d-%d", seg, i), string(got),
				"key %q lost or corrupted after shutdown-during-compaction", key)
		}
	}
}
