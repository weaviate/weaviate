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

// TestBucket_ShutdownWithCancelledContext pins the Shutdown-cancellation
// contract from weaviate/0-weaviate-issues#213.
//
// A node terminating mid-swap calls Bucket.Shutdown with an already-cancelled
// ctx. Before the fix, that ctx was threaded straight into the compaction (and
// flush) cycle Unregister, which returned early on the dead ctx WITHOUT ever
// signalling the in-flight compaction to abort. The compaction ran on, the
// higher-level shutdown (a runtime-reindex swap) was left half-applied, and the
// caller saw a mislabelled "long-running compaction in progress: context
// canceled" error.
//
// The bucket is wired with real (non-noop) compaction and flush cycles so the
// test drives the genuine cyclemanager unregister path, then Shutdown is called
// with a cancelled ctx while the compaction cycle is live. Shutdown must return
// no error, must not hang, and the store must reopen with every key intact.
func TestBucket_ShutdownWithCancelledContext(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	const segments = 2
	const perSegment = 20000

	compactionCallbacks := cyclemanager.NewCallbackGroup("compaction", nullLogger(), 1)
	compactionCycle := cyclemanager.NewManager("compaction",
		cyclemanager.NewFixedTicker(10*time.Millisecond),
		compactionCallbacks.CycleCallback, nullLogger())
	compactionCycle.Start()

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	flushCycle := cyclemanager.NewManager("flush",
		cyclemanager.NewFixedTicker(10*time.Millisecond),
		flushCallbacks.CycleCallback, nullLogger())
	flushCycle.Start()

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
	require.GreaterOrEqual(t, len(bucket.disk.segments), 2,
		"need at least two segments on disk so a compaction can be in flight")

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

	// Reopen the store on the same dir: proves the shutdown (with a possibly
	// aborted compaction) left consistent on-disk state. init() cleans any
	// orphaned .tmp from the aborted merge.
	reopened, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer reopened.Shutdown(ctx)

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
