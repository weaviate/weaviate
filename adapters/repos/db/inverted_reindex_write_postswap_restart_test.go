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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost is the RED
// pinning test for the RESTART residual of weaviate/0-weaviate-issues#319.
//
// # Mechanism it pins
//
// The force-index overlay that covers the post-swap pre-flip write window
// (see inverted_reindex_write_postswap_preflip_test.go) is per-shard,
// in-memory state. A node that restarts INSIDE the window loses it, and
// nothing on the local node re-arms it before the shard starts accepting
// writes:
//
//   - FinalizeCompletedMigrations (shard_init.go) promotes the tidied
//     ingest dir to canonical AND removes the .migrations tracker dir, so
//     no local sentinel survives that says "swap committed, cluster-wide
//     schema flip still pending".
//   - The migration task hooks see IsStarted=false (tracker gone) and
//     return early — no PreReindexHook, no callback registration, no
//     overlay arm (pinned by TestMapToBlockmaxMigration_RuntimeSwap_
//     ThenRestart's restart-no-refire comment).
//   - Worse: initPropertyBuckets keys on the pre-flip schema, so the
//     restarted shard does not even LOAD the promoted canonical bucket
//     (the first assertion below fails already at that layer). Until
//     something re-loads it, in-window writes are silently dropped, and
//     if the cluster-wide flip landed while the bucket is unloaded,
//     every write to the property would error with "no bucket for prop
//     found" — nothing on the enable path ever creates/loads buckets on
//     a property UPDATE (updatePropertyBuckets only removes).
//   - The distributed-task machinery can converge: a task in SWAPPING is
//     not pre-marked at scheduler bootstrap, so OnGroupCompleted replays;
//     with the tracker gone that replay re-runs the migration from
//     scratch (PreReindexHook re-loads the bucket, the re-iteration
//     re-scans objects written in the gap, the barrier defers the flip
//     until the re-swap). But that convergence is entirely at the DTM
//     layer — there is no local guarantee, the replay can be arbitrarily
//     delayed (RAFT leader loss, scheduler backoff), and nothing covers
//     writes between shard-ready and the replay's callback registration
//     if the replay's fresh reindexStarted races their timestamps.
//
// The same root cause also drops the READ-side tokenization overlay of a
// change-tokenization migration across a restart in its window (queries
// mis-tokenize until the flip applies locally) — one durable "flip
// pending" marker would close both.
//
// # Why this is pinned rather than fixed here
//
// A correct fix needs a durable, per-shard "swap committed, flip pending"
// record that (a) survives FinalizeCompletedMigrations, (b) carries the
// target schema (flags + tokenization) so the overlay can be rebuilt at
// shard init before the first write, and (c) is retired exactly when the
// local node observes the flip. That is new migration lifecycle state with
// its own crash-recovery semantics — the same lifecycle segment the
// pre-commit staging design for weaviate/0-weaviate-issues#220
// (weaviate/weaviate#11995) is restructuring around the ack barrier.
// Bolting an ad-hoc sentinel onto the current lifecycle would conflict
// with that design; the staging work should own this marker. Un-skip when
// it lands.
func TestReindexPostSwapPreFlip_RestartInWindow_InsertNotLost(t *testing.T) {
	t.Skip("RED pin for weaviate/0-weaviate-issues#319 restart residual: a node restart inside " +
		"the post-swap pre-flip window loses the in-memory force-index overlay, does not re-load the " +
		"promoted canonical bucket (initPropertyBuckets keys on the pre-flip schema), and no durable " +
		"local state re-arms either before the shard accepts writes — convergence depends entirely on " +
		"the DTM replay re-running the migration. Needs a durable flip-pending marker; see the " +
		"pre-commit staging design (weaviate/0-weaviate-issues#220, weaviate/weaviate#11995). " +
		"Un-skip when that lands — the assertions below then go green.")

	const propName = "title"
	ctx := testCtx()
	className := "PostSwapPreFlipRestart_" + uuid.NewString()[:8]
	class := newNoIndexTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	require.NoError(t, shard.PutObject(ctx, objWithTitle(className, uuid.NewString(), "alpha")))

	driveEnableFilterableToPostSwapWindow(t, shard, idx, className, propName)

	// Restart INSIDE the window: the cluster-wide schema flip has not
	// happened (the class fixture still has IndexFilterable=false).
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)
	defer shard2.Shutdown(ctx)

	// Post-restart, still pre-flip: the canonical bucket holds the swapped
	// (backfilled) data...
	bucket := shard2.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotNil(t, bucket, "canonical filterable bucket must be loaded after restart-finalize")
	require.NotEmpty(t, fingerprintRoaringSetBucket(t, bucket)["alpha"],
		"backfilled data must survive the restart")

	// ...but a write arriving before the (replayed) cluster-wide flip is
	// analyzed under the pre-flip schema with no overlay to save it.
	require.NoError(t, shard2.PutObject(ctx, objWithTitle(className, uuid.NewString(), "resttoken")),
		"post-restart in-window insert must not error")

	fp := fingerprintRoaringSetBucket(t, bucket)
	require.NotEmptyf(t, fp["resttoken"],
		"weaviate/0-weaviate-issues#319 restart residual: an insert after a restart inside the "+
			"post-swap pre-flip window must reach the migrated canonical bucket; got %v", fp)
}
