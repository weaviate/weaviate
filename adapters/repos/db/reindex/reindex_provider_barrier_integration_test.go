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

package reindex_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Integration tests for reindex.ReindexProvider's PREP→SWAP boundary on a real
// shard with a real LSM store (the acceptance equivalent is slow and
// flaky). Tests exercise runShardPrepPhase / runShardSwapPhase
// directly — outer GetIndex/lookupShardByName orchestration is unit-
// tested elsewhere.
//
// T1: PREP boundary — IsReindexed → IsMerged via runShardPrepPhase.
// T2: SWAP boundary — IsMerged → IsSwapped+IsTidied via runShardSwapPhase.
// T3: Crash between persistRecoveryRecord and markStarted — discover
//     must skip the dir; payload.mig survives intact for retry.
// T4: markReindexed durability — sentinel survives process death
//     without fsync from the test (foundation of issue #214 / commit
//     073d47b460's IsReindexed dispatch).

// barrierIntegrationProvider builds the minimal reindex.ReindexProvider these
// tests need — runShardPrepPhase / runShardSwapPhase only touch
// logger + serverCtx, not the db/schemaManager/recorder fields.
func barrierIntegrationProvider(t *testing.T) (*reindex.ReindexProvider, *logrustest.Hook) {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	p := &reindex.ReindexProvider{
		Logger:    logger,
		LocalNode: "node1",
		ServerCtx: context.Background(),
	}
	return p, hook
}

// barrierIntegrationDrivenToReindexed halts iteration at markReindexed
// (the FINALIZING-barrier handoff) via skipSwapOnFinish=true.
func barrierIntegrationDrivenToReindexed(
	t *testing.T,
	ctx context.Context,
	shard *db.Shard,
	logger logrus.FieldLogger,
) (*reindex.ShardReindexTaskGeneric, *testMigrationStrategy) {
	t.Helper()
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: reindex.MapToBlockmaxStrategy{Generation: 1}}
	task := newTestTask(logger, strategy)
	task.SkipSwapOnFinish.Store(true)

	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	// Sanity: iteration must have halted at the barrier (markReindexed
	// written, runtimePrepare NOT called).
	rt := reindex.NewFileMapToBlockmaxReindexTracker(shard.PathLSM(), &reindex.UuidKeyParser{})
	require.True(t, rt.IsReindexed(),
		"helper precondition: iteration must reach IsReindexed under skipSwapOnFinish=true")
	require.False(t, rt.IsMerged(),
		"helper precondition: runtimePrepare must NOT run under skipSwapOnFinish=true")
	return task, strategy
}

// barrierIntegrationSeedObjects writes a deterministic set of objects so
// the iteration has something to process. 25 objects is plenty for the
// LSM cursor to produce per-prop posting lists without inflating runtime.
func barrierIntegrationSeedObjects(t *testing.T, ctx context.Context, shard *db.Shard, className string, n int) []*storobj.Object {
	t.Helper()
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		out[i] = createTestObjectWithText(className, "barrier integration "+uuid.NewString())
		require.NoError(t, shard.PutObject(ctx, out[i]))
	}
	return out
}

// TestReindexProviderBarrierIntegration_OnGroupCompletedPrep pins the
// PREP-phase contract: given a unit at IsReindexed (the post-iteration,
// pre-merge state that OnGroupCompleted lands in for barrier-mode tasks),
// the provider's runShardPrepPhase must advance the on-disk sentinels
// to IsMerged. This is the "OnGroupCompleted → RunPrepareOnShard boundary"
// gap T2.3 was scoped against — it has no direct unit-test coverage
// today (the existing unit tests cover RunSwapOnShard's sentinel-aware
// dispatch, not the PREP-phase boundary).
func TestReindexProviderBarrierIntegration_OnGroupCompletedPrep(t *testing.T) {
	ctx := testCtx()
	className := "BarrierIntegPrep"
	class := newTestClass(className)

	shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	defer shard.Shutdown(ctx)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

	// Drive to IsReindexed via the barrier path.
	task, _ := barrierIntegrationDrivenToReindexed(t, ctx, shard, f.Logger())

	// Pre-PREP invariants: reindexed yes, merged no.
	rtPre := reindex.NewFileMapToBlockmaxReindexTracker(shard.PathLSM(), &reindex.UuidKeyParser{})
	require.True(t, rtPre.IsReindexed(), "pre-PREP: must be reindexed")
	require.False(t, rtPre.IsMerged(), "pre-PREP: must NOT be merged")

	// Invoke the provider's runShardPrepPhase — same call OnGroupCompleted
	// makes per-shard. rehydrate=false matches the in-process happy path
	// (the cached task instance still has its double-write callbacks).
	p, _ := barrierIntegrationProvider(t)
	ok, res := p.RunShardPrepPhase(ctx, "unit-1", shard,
		[]*reindex.ShardReindexTaskGeneric{task}, false, p.Logger)
	require.True(t, ok, "PREP must succeed: %v", res.Errs)
	require.Empty(t, res.Errs, "PREP must not accumulate errors")

	// Post-PREP invariants: sentinels advance from IsReindexed → IsMerged.
	// IsPrepended is the intermediate sentinel runtimePrepare writes
	// between the per-prop PrependSegmentsFromBucket loop and
	// markMerged — its presence pins that runtimePrepare ran to
	// completion.
	rtPost := reindex.NewFileMapToBlockmaxReindexTracker(shard.PathLSM(), &reindex.UuidKeyParser{})
	assert.True(t, rtPost.IsReindexed(), "post-PREP: reindexed sentinel preserved")
	assert.True(t, rtPost.IsPrepended(), "post-PREP: prepended sentinel written by runtimePrepare")
	assert.True(t, rtPost.IsMerged(), "post-PREP: merged sentinel — RunPrepareOnShard advanced from IsReindexed to IsMerged")
	assert.False(t, rtPost.IsSwapped(), "post-PREP: SWAP must NOT run yet (that's OnSwapRequested's job)")
	assert.False(t, rtPost.IsTidied(), "post-PREP: TIDY must NOT run yet")
}

// TestReindexProviderBarrierIntegration_OnSwapRequestedSwap pins the
// SWAP-phase contract: given a unit at IsMerged (the state the PREP
// barrier produces), the provider's runShardSwapPhase must produce
// IsSwapped + IsTidied sentinels. This is the
// "OnSwapRequested arrival after Phase A.5 transition" gap T2.3 was
// scoped against.
//
// The test runs PREP first (via runShardPrepPhase) to stage the
// IsMerged state, then runs SWAP — mirroring the cluster-wide barrier:
// PREP per node → cluster-wide PreparationCompleteAck → OnSwapRequested
// per node.
func TestReindexProviderBarrierIntegration_OnSwapRequestedSwap(t *testing.T) {
	ctx := testCtx()
	className := "BarrierIntegSwap"
	class := newTestClass(className)

	shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	defer shard.Shutdown(ctx)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

	// Stage 1: drive to IsReindexed.
	task, strategy := barrierIntegrationDrivenToReindexed(t, ctx, shard, f.Logger())

	// Stage 2: run PREP to advance to IsMerged (what the cluster-wide
	// barrier observes via PreparationCompleteAck).
	p, _ := barrierIntegrationProvider(t)
	ok, prepRes := p.RunShardPrepPhase(ctx, "unit-1", shard,
		[]*reindex.ShardReindexTaskGeneric{task}, false, p.Logger)
	require.True(t, ok, "PREP setup must succeed: %v", prepRes.Errs)

	rtMid := reindex.NewFileMapToBlockmaxReindexTracker(shard.PathLSM(), &reindex.UuidKeyParser{})
	require.True(t, rtMid.IsMerged(), "mid: must be merged before SWAP")
	require.False(t, rtMid.IsSwapped(), "mid: must NOT be swapped before SWAP")

	// Stage 3: SWAP. Use a synthetic payload that won't trigger tokenization
	// overlay (MapToBlockmax is not a tokenization-changing migration);
	// the runShardSwapPhase code-path is the same regardless.
	payload := &reindex.ReindexTaskPayload{
		MigrationType: reindex.ReindexTypeChangeAlgorithm,
		Collection:    className,
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"unit-1": shard.Name()},
		UnitToNode:    map[string]string{"unit-1": "node1"},
	}
	swapRes := p.RunShardSwapPhase(ctx, payload, "unit-1", shard.Name(), shard,
		[]*reindex.ShardReindexTaskGeneric{task}, p.Logger)
	require.Empty(t, swapRes.Errs, "SWAP must succeed")

	// Post-SWAP invariants: IsSwapped + IsTidied. In runtimeSwap the
	// per-prop swap → markSwapped → tidy sequence is atomic (Phase 2a
	// pins this contract — see TestRuntimeSwap_Phase2a_AtomicTightLoop)
	// so both sentinels appear together once swap+tidy returns clean.
	rtFinal := reindex.NewFileMapToBlockmaxReindexTracker(shard.PathLSM(), &reindex.UuidKeyParser{})
	assert.True(t, rtFinal.IsSwapped(), "post-SWAP: swapped sentinel — runShardSwapPhase flipped the bucket pointer")
	assert.True(t, rtFinal.IsTidied(), "post-SWAP: tidied sentinel — runShardSwapPhase tidied backup buckets")
	assert.True(t, strategy.migrationCompleted,
		"post-SWAP: OnMigrationComplete must have fired (tail of every recovery branch)")

	// Bucket strategy must have flipped to Inverted.
	bucketName := helpers.BucketSearchableFromPropNameLSM("title")
	postBucket := shard.Store().Bucket(bucketName)
	require.NotNil(t, postBucket, "post-SWAP: searchable bucket must still exist")
	assert.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-SWAP: searchable bucket strategy must be Inverted")
}

// TestReindexProviderBarrierIntegration_CrashAfterPersistRecoveryRecord
// pins the contract that a crash between persistRecoveryRecord and the
// first sentinel write (markStarted) leaves the system in a state where
// recovery discovery (reindex.DiscoverInFlightReindexTasks) safely skips the
// half-initialized migration directory — i.e. the worst case is "no
// recovery work to do", not "load corrupt recovery state".
//
// Why this matters: persistRecoveryRecord is what allows post-restart
// recovery to rebuild the right reindex.ShardReindexTaskGeneric strategy +
// generation. If we wrote payload.mig and then crashed before the
// iteration could even begin (started.mig never appears), the DTM
// scheduler will retry the task; processOneUnit will call
// persistRecoveryRecord AGAIN (it's idempotent on identical content) and
// run the iteration. The on-disk state must be benign across this
// retry window. The acceptance test for this is multi-hour and
// chaos-driven; the integration version pins the on-disk invariant
// deterministically.
func TestReindexProviderBarrierIntegration_CrashAfterPersistRecoveryRecord(t *testing.T) {
	ctx := testCtx()
	className := "BarrierIntegCrashRecord"
	class := newTestClass(className)

	shd, idx, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	defer shard.Shutdown(ctx)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 10)

	// Construct a task instance (matches what processOneUnit's
	// createReindexTasks would produce for ChangeAlgorithm/MapToBlockmax).
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: reindex.MapToBlockmaxStrategy{Generation: 1}}
	task := newTestTask(f.Logger(), strategy)

	// Build a synthetic task + payload that processOneUnit would have
	// constructed before calling persistRecoveryRecord.
	taskID := "test-crash-after-persist-" + uuid.NewString()[:8]
	dtmTask := &distributedtask.Task{
		Namespace: reindex.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{
			ID:      taskID,
			Version: 1,
		},
	}
	payload := &reindex.ReindexTaskPayload{
		MigrationType: reindex.ReindexTypeChangeAlgorithm,
		Collection:    className,
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"unit-1": shard.Name()},
		UnitToNode:    map[string]string{"unit-1": "node1"},
	}

	// Call persistRecoveryRecord ALONE — simulating a crash immediately
	// after this write but before markStarted / iteration.
	p, _ := barrierIntegrationProvider(t)
	require.NoError(t, p.PersistRecoveryRecord(dtmTask, payload, "unit-1",
		shard.PathLSM(), []*reindex.ShardReindexTaskGeneric{task}))

	// Sanity: payload.mig is on disk in the migration dir.
	migDir := filepath.Join(shard.PathLSM(), ".migrations", task.MigrationDirName())
	payloadPath := filepath.Join(migDir, reindex.ReindexRecoveryPayloadFile)
	rawPayload, err := os.ReadFile(payloadPath)
	require.NoError(t, err, "payload.mig must exist after persistRecoveryRecord")
	require.NotEmpty(t, rawPayload, "payload.mig must not be empty")
	var decoded reindex.ReindexRecoveryRecord
	require.NoError(t, json.Unmarshal(rawPayload, &decoded),
		"payload.mig must round-trip as valid JSON — recovery's json.Unmarshal would fail otherwise")
	require.Equal(t, taskID, decoded.TaskID, "recovery record must preserve taskID")
	require.Equal(t, "unit-1", decoded.UnitID, "recovery record must preserve unitID")

	// Sanity: started.mig was NOT written (the crash beat the iteration
	// to it). This is the invariant the discover path keys off.
	startedPath := filepath.Join(migDir, "started.mig")
	_, err = os.Stat(startedPath)
	require.True(t, os.IsNotExist(err),
		"started.mig must NOT exist — iteration never ran")

	// Now simulate process restart: reindex.DiscoverInFlightReindexTasks walks
	// the data dir and must SKIP this migration (started.mig absent).
	// We pass nil schemaManager — the discover path is read-only against
	// disk and never invokes schema operations until buildRecoveryTasks
	// fires (which only fires for dirs with started + reindexed).
	rootPath := idx.Config.RootPath
	recovered, err := reindex.DiscoverInFlightReindexTasks(rootPath, f.Logger(), nil)
	require.NoError(t, err, "discover must not error on a started.mig-less dir")
	for _, r := range recovered {
		assert.NotEqualf(t, taskID, r.Descriptor.ID,
			"discover MUST skip migration with no started.mig sentinel (load-bearing for crash-between-persist-and-markStarted recovery)")
	}

	// Confirm payload.mig is still intact: a retry of processOneUnit
	// could call persistRecoveryRecord again with the same content
	// (idempotent — same TaskID + UnitID + Payload → bytes.Equal short
	// circuit at SaveRecoveryPayload line 279). Re-call to verify
	// idempotency.
	require.NoError(t, p.PersistRecoveryRecord(dtmTask, payload, "unit-1",
		shard.PathLSM(), []*reindex.ShardReindexTaskGeneric{task}),
		"persistRecoveryRecord must be idempotent against an existing identical record")
	rawPayload2, err := os.ReadFile(payloadPath)
	require.NoError(t, err)
	assert.Equal(t, rawPayload, rawPayload2,
		"idempotent persist must leave the file bit-identical (no rewrite)")
}

// TestReindexProviderBarrierIntegration_MarkReindexedDurabilityBarrier
// pins commit 073d47b460 (weaviate/0-weaviate-issues#214):
// FlushAndSwitch happens-BEFORE markReindexed, so a follow-up restart
// dispatching via IsReindexed never sees the marker without the data.
// A refactor that moved FlushAndSwitch after the marker would fail
// here.
func TestReindexProviderBarrierIntegration_MarkReindexedDurabilityBarrier(t *testing.T) {
	ctx := testCtx()
	className := "BarrierIntegDurability"
	class := newTestClass(className)

	shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

	// Drive iteration to the barrier point — markReindexed has fired
	// AFTER the FlushAndSwitch durability barrier
	// (inverted_reindex_task_generic.go:1461-1469).
	_, _ = barrierIntegrationDrivenToReindexed(t, ctx, shard, f.Logger())

	// Record the on-disk path of reindexed.mig BEFORE shutdown so we can
	// re-stat it post-shutdown without relying on a tracker rebuild
	// (rebuild would mask a file that was missing-but-cached).
	migDir := filepath.Join(shard.PathLSM(),
		".migrations", reindex.MigrationDirSearchableMapToBlockmax+reindex.GenSuffix(1))
	reindexedPath := filepath.Join(migDir, "reindexed.mig")

	// Capture the file's content and stat before shutdown — content must
	// be a parseable RFC3339Nano timestamp (the tracker's encodeTimeNow
	// format). A zero-byte / corrupt write would surface here.
	preContent, err := os.ReadFile(reindexedPath)
	require.NoError(t, err, "reindexed.mig must exist on disk after iteration")
	require.NotEmpty(t, preContent, "reindexed.mig must not be empty")
	_, err = time.Parse(time.RFC3339Nano, string(preContent))
	require.NoError(t, err,
		"reindexed.mig must contain a parseable RFC3339Nano timestamp; got %q", string(preContent))

	// Verify the per-property reindex bucket has on-disk segments at this
	// point — the FlushAndSwitch barrier means iteration data is durable
	// in segment files, not just memtables. The reindex bucket sits at
	// <lsm>/<bucketName + ReindexSuffix>/<segment files>.
	reindexDirName := helpers.BucketSearchableFromPropNameLSM("title") +
		"__blockmax_reindex" + reindex.GenSuffix(1)
	reindexBucketDir := filepath.Join(shard.PathLSM(), reindexDirName)
	stat, err := os.Stat(reindexBucketDir)
	require.NoError(t, err, "reindex bucket dir must exist after FlushAndSwitch barrier")
	require.True(t, stat.IsDir(), "reindex bucket path must be a directory")

	// Shut the shard down — this exercises the LSM store's flush + close
	// path. Anything that's still memtable-only at this point would be
	// lost; everything segment-backed survives.
	require.NoError(t, shard.Shutdown(ctx))

	// Post-shutdown: reindexed.mig must be readable with bit-identical
	// content. If a refactor introduces ANY lazy-write path for the
	// sentinel (or removes the FlushAndSwitch barrier and the bucket's
	// memtables are now lost), this read will surface the regression.
	postContent, err := os.ReadFile(reindexedPath)
	require.NoError(t, err,
		"reindexed.mig must persist across shard shutdown (FlushAndSwitch durability barrier contract)")
	assert.Equal(t, preContent, postContent,
		"reindexed.mig content must be bit-identical across shutdown")

	// Reindex bucket dir must also persist on disk. If FlushAndSwitch
	// returned but the segments weren't really on disk, the file might
	// be missing after shard close — the test guards against that
	// regression too.
	statPost, err := os.Stat(reindexBucketDir)
	require.NoError(t, err,
		"reindex bucket dir must persist across shutdown (FlushAndSwitch barrier ⇒ segments are on disk)")
	require.True(t, statPost.IsDir())

	// Rebuild a tracker from the on-disk state (the same path
	// reindex.DiscoverInFlightReindexTasks uses on real startup) and verify
	// IsReindexed reports true. This pins the END-TO-END contract:
	// the barrier persisted, AND the recovery path sees it as
	// IsReindexed (which is the dispatch key for
	// RunSwapOnShard's resumeFromReindexed branch).
	rtRecovered := reindex.NewFileMapToBlockmaxReindexTracker(shard.PathLSM(), &reindex.UuidKeyParser{})
	assert.True(t, rtRecovered.IsReindexed(),
		"recovered tracker must report IsReindexed=true (durability barrier ⇒ marker survives)")
	assert.False(t, rtRecovered.IsMerged(),
		"recovered tracker must report IsMerged=false (barrier path stops at IsReindexed)")
}
