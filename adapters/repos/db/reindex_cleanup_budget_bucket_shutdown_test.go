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
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// sweepStoppedInABucketShutdown runs sweep against a shard holding one
// cancelled attempt's sidecar bucket, and cancels the sweep's context while it
// is inside that bucket's shutdown. It returns the log the index wrote and what
// the sweep reported.
//
// The bucket is deregistered before its shutdown drains its in-flight reads, so
// "gone from the store" is the point where the sweep is known to be inside the
// shutdown and past the check at the top of its turn. A read pin holds it there
// until the cancel lands, which is what makes the cancellation reach the
// shutdown every run rather than most runs. The drain itself ignores the
// context: cancelling only takes effect once the pin is released and the
// shutdown reaches its first wait.
func sweepStoppedInABucketShutdown(t *testing.T,
	sweep func(ctx context.Context, idx *Index, shard *Shard) error,
) (*logrustest.Hook, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(testCtx())
	t.Cleanup(cancel)

	className := "SweepInShutdown" + uuid.NewString()[:8]
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	shd, idx := testShardWithSettings(t, testCtx(),
		newTestClassWithProps(className, []string{"category"}),
		enthnsw.UserConfig{Skip: true}, false, false, false,
		func(i *Index) { i.logger = logger })
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(testCtx()) })
	idx.logger = logger

	// A cancelled attempt at gen 2: started but never completed, so the sweep
	// has to shut its ingest bucket down rather than preserve it.
	mkTrackerDir(t, shard.pathLSM(), "enable_filterable_category_2", "started.mig")
	const sidecar = "property_category__enable_filterable_ingest_2"
	require.NoError(t, shard.store.CreateOrLoadBucket(testCtx(), sidecar,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))

	// Pinned for read, so the shutdown blocks where a bucket with in-flight
	// reads blocks in production.
	pinned, unpin := shard.store.AcquireBucketForRead(sidecar)
	require.NotNil(t, pinned)

	swept := make(chan error, 1)
	go func() {
		swept <- sweep(ctx, idx, shard)
	}()

	require.Eventually(t, func() bool {
		_, stillRegistered := shard.store.GetBucketsByName()[sidecar]
		return !stillRegistered
	}, 30*time.Second, time.Millisecond,
		"the sweep never reached the bucket shutdown, so the cancellation could not land in it")

	cancel()
	unpin()
	return hook, <-swept
}

// The run's budget can also expire inside a shard's own sweep, not just inside
// the load that precedes it: shutting a sidecar bucket down takes the same
// context, and a bucket still draining in-flight reads is where a slow one
// sits. Reported as a shard that could not be swept, that timeout pages an
// operator at Error and claims confirmed state on a shard the run never
// finished.
func TestACleanupBudgetThatExpiresInABucketShutdownIsTruncatedNotFailed(t *testing.T) {
	hook, err := sweepStoppedInABucketShutdown(t,
		func(ctx context.Context, idx *Index, _ *Shard) error {
			return idx.cleanStalePartialReindexState(ctx, "category", "filterable", nil)
		})

	require.Error(t, err)
	require.Contains(t, err.Error(), "shutting down stale sidecar bucket",
		"the run has to have stopped in the bucket shutdown, not in the load before it")

	outcome, _ := ClassifyCleanupSweep(err)
	require.Equal(t, CleanupSweepUnknown, outcome,
		"the run ran out of time, so the shard it stopped on was never finished")
	require.NotErrorIs(t, err, ErrCleanupShardFailed,
		"the bucket was not broken; the run simply stopped on it")

	wantMsg, wantLevel := CleanupSweepSummary(sweepPhaseIndexCleanup, CleanupSweepUnknown)
	var summary []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Data["operation"] == "CleanStalePartialReindexState" {
			summary = append(summary, entry)
		}
	}
	require.Len(t, summary, 1)
	require.Equal(t, wantLevel, summary[0].Level,
		"a run out of time must not reach the operator at the level reserved for a broken shard")
	require.Contains(t, summary[0].Message, wantMsg)
}
