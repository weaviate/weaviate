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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// [truncatedByCancellation] can only tell a cancelled run from a broken shard
// if the steps it guards report the cancellation instead of swallowing it, so
// that premise is pinned here rather than assumed. A step that reported a
// cancellation as an error of its own would read as a broken shard and page an
// operator over a timeout.
//
// Both steps of the sweep that take the context are covered: the shard load
// waits for its permit on it, and the sidecar shutdown hands it to the bucket's
// shutdown, whose compaction and flush waits both wrap it.
func TestBothSweepStepsReportACancellationInTheErrorChain(t *testing.T) {
	tests := []struct {
		name string
		// step runs the sweep step against a context that is cancelled while it
		// is inside it, and returns what the step reported.
		step func(t *testing.T) error
	}{
		{
			name: "the shard load waits for its permit on the sweep's context",
			step: loadStoppedByACancelledContext,
		},
		{
			name: "the sidecar shutdown hands the sweep's context to the bucket",
			step: sidecarShutdownStoppedByACancelledContext,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.step(t)
			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled,
				"the guard matches on this; a step that dropped it would report a broken shard")
		})
	}
}

// loadStoppedByACancelledContext is the sweep's hydration step, stopped by the
// context it waits for its load permit on.
func loadStoppedByACancelledContext(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	logger, _ := logrustest.NewNullLogger()
	closingCtx, closeIndex := context.WithCancel(context.Background())
	t.Cleanup(closeIndex)
	closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
	t.Cleanup(func() { signalCloseRequested(nil) })

	idx := &Index{
		Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
		closingCtx:           closingCtx,
		closeRequestedCtx:    closeRequestedCtx,
		signalCloseRequested: signalCloseRequested,
		logger:               logger,
	}
	lazy := &LazyLoadShard{
		shardOpts:        &deferredShardOpts{name: "shard-a", index: idx, class: &models.Class{Class: "Movies"}},
		memMonitor:       &loadAttemptMonitor{admitLoad: true},
		shardLoadLimiter: newSweepLoadLimiter(),
	}

	_, err := lazy.Unwrap(ctx)
	return err
}

// sidecarShutdownStoppedByACancelledContext is the sweep's first side effect,
// stopped by the context the bucket takes into its shutdown. The bucket is
// deregistered before it drains its in-flight reads, so "gone from the store"
// is where the sweep is known to be inside the shutdown; a read pin holds it
// at that drain. The drain itself ignores the context — cancelling only takes
// effect once the pin is released and the shutdown reaches its first wait.
func sidecarShutdownStoppedByACancelledContext(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithCancel(testCtx())
	t.Cleanup(cancel)

	className := "CancelInShutdown" + uuid.NewString()[:8]
	shd, _ := testShardWithSettings(t, testCtx(),
		newTestClassWithProps(className, []string{"category"}),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(testCtx()) })

	mkTrackerDir(t, shard.pathLSM(), "enable_filterable_category_2", "started.mig")
	const sidecar = "property_category__enable_filterable_ingest_2"
	require.NoError(t, shard.store.CreateOrLoadBucket(testCtx(), sidecar,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))

	pinned, unpin := shard.store.AcquireBucketForRead(sidecar)
	require.NotNil(t, pinned)

	swept := make(chan error, 1)
	go func() {
		_, err := shard.CleanStalePartialReindexState(ctx, "category", "filterable")
		swept <- err
	}()

	require.Eventually(t, func() bool {
		_, stillRegistered := shard.store.GetBucketsByName()[sidecar]
		return !stillRegistered
	}, 30*time.Second, time.Millisecond,
		"the sweep never reached the bucket shutdown, so the cancellation could not land in it")

	cancel()
	unpin()
	return <-swept
}
