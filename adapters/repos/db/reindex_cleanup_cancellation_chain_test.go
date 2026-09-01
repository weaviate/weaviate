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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
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
// stopped by the context the bucket takes into its shutdown. It is the shard's
// own sweep rather than the index-level one, so nothing above the shard can
// re-tag what the shutdown reported.
func sidecarShutdownStoppedByACancelledContext(t *testing.T) error {
	t.Helper()
	_, err := sweepStoppedInABucketShutdown(t,
		func(ctx context.Context, _ *Index, shard *Shard) error {
			_, err := shard.CleanStalePartialReindexState(ctx, "category", "filterable")
			return err
		})
	return err
}
