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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Integration tests for ReindexProvider's PREP→SWAP boundary on a real
// shard with a real LSM store (the acceptance equivalent is slow and
// flaky). Tests exercise runShardPrepPhase / runShardSwapPhase
// directly — outer GetIndex/lookupShardByName orchestration is unit-
// tested elsewhere.
//
// T1: PREP boundary — Iterated to Merged via runShardPrepPhase.
// T2: SWAP boundary — Merged to Swapped via runShardSwapPhase.
// T3: Crash between persistRecoveryRecord and markStarted — discover
//     must skip the dir; payload.mig survives intact for retry.
// T4: markReindexed durability — sentinel survives process death
//     without fsync from the test (foundation of issue #214 / commit
//     073d47b460's IsReindexed dispatch).

// barrierIntegrationProvider builds the minimal ReindexProvider these
// tests need — runShardPrepPhase / runShardSwapPhase only touch
// logger + serverCtx, not the db/schemaManager/recorder fields.
func barrierIntegrationProvider(t *testing.T) (*ReindexProvider, *logrustest.Hook) {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	p := &ReindexProvider{
		logger:    logger,
		localNode: "node1",
		serverCtx: context.Background(),
	}
	return p, hook
}

// barrierIntegrationDrivenToReindexed halts iteration at markReindexed
// (the FINALIZING-barrier handoff) via skipSwapOnFinish=true.
func barrierIntegrationDrivenToReindexed(
	t *testing.T,
	ctx context.Context,
	shard *Shard,
	logger logrus.FieldLogger,
) (*ShardReindexTaskGeneric, *testMigrationStrategy) {
	t.Helper()
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(logger, strategy)
	task.skipSwapOnFinish.Store(true)

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
	rec, ok := task.migrationRecord(shard)
	require.True(t, ok, "helper precondition: iteration must leave a record")
	require.Equal(t, MigrationStateIterated, rec.State(),
		"helper precondition: iteration must reach Iterated under skipSwapOnFinish=true")
	require.False(t, rec.DataCommitted(),
		"helper precondition: runtimePrepare must NOT run under skipSwapOnFinish=true")
	return task, strategy
}

// barrierIntegrationSeedObjects writes a deterministic set of objects so
// the iteration has something to process. 25 objects is plenty for the
// LSM cursor to produce per-prop posting lists without inflating runtime.
func barrierIntegrationSeedObjects(t *testing.T, ctx context.Context, shard *Shard, className string, n int) []*storobj.Object {
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

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

	// Drive to IsReindexed via the barrier path.
	task, _ := barrierIntegrationDrivenToReindexed(t, ctx, shard, idx.logger)

	// Pre-PREP invariants: reindexed yes, merged no.
	recPre, ok := task.migrationRecord(shard)
	require.True(t, ok)
	require.Equal(t, MigrationStateIterated, recPre.State(), "pre-PREP: the rebuild is done and nothing is staged")

	// Invoke the provider's runShardPrepPhase — same call OnGroupCompleted
	// makes per-shard. rehydrate=false matches the in-process happy path
	// (the cached task instance still has its double-write callbacks).
	p, _ := barrierIntegrationProvider(t)
	ok, res := p.runShardPrepPhase(ctx, "unit-1", shard,
		[]*ShardReindexTaskGeneric{task}, false, p.logger)
	require.True(t, ok, "PREP must succeed: %v", res.Errs)
	require.Empty(t, res.Errs, "PREP must not accumulate errors")

	// Post-PREP invariants: the record advances from Iterated to Merged.
	// IsPrepended is the intermediate sentinel runtimePrepare writes
	// between the per-prop PrependSegmentsFromBucket loop and
	// markMerged — its presence pins that runtimePrepare ran to
	// completion.
	recPost, ok := task.migrationRecord(shard)
	require.True(t, ok)
	assert.Equal(t, MigrationStateMerged, recPost.State(),
		"post-PREP: RunPrepareOnShard advanced the record from Iterated to Merged")
	assert.False(t, recPost.PointerSwapped(),
		"post-PREP: the flip must NOT be decided yet, that is OnSwapRequested's job")
}

// TestReindexProviderBarrierIntegration_OnSwapRequestedSwap pins the
// SWAP-phase contract: given a unit at Merged (the state the PREP
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

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

	// Stage 1: drive to IsReindexed.
	task, strategy := barrierIntegrationDrivenToReindexed(t, ctx, shard, idx.logger)

	// Stage 2: run PREP to advance to IsMerged (what the cluster-wide
	// barrier observes via PreparationCompleteAck).
	p, _ := barrierIntegrationProvider(t)
	ok, prepRes := p.runShardPrepPhase(ctx, "unit-1", shard,
		[]*ShardReindexTaskGeneric{task}, false, p.logger)
	require.True(t, ok, "PREP setup must succeed: %v", prepRes.Errs)

	recMid, ok := task.migrationRecord(shard)
	require.True(t, ok)
	require.Equal(t, MigrationStateMerged, recMid.State(), "mid: must be merged before SWAP")

	// Stage 3: SWAP. Use a synthetic payload that won't trigger tokenization
	// overlay (MapToBlockmax is not a tokenization-changing migration);
	// the runShardSwapPhase code-path is the same regardless.
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeChangeAlgorithm,
		Collection:    className,
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"unit-1": shard.Name()},
		UnitToNode:    map[string]string{"unit-1": "node1"},
	}
	swapRes := p.runShardSwapPhase(ctx, payload, "unit-1", shard.Name(), shard,
		[]*ShardReindexTaskGeneric{task}, p.logger)
	require.Empty(t, swapRes.Errs, "SWAP must succeed")

	// Post-SWAP invariants: IsSwapped + IsTidied. In runtimeSwap the
	// per-prop swap → markSwapped → tidy sequence is atomic (Phase 2a
	// pins this contract — see TestRuntimeSwap_Phase2a_AtomicTightLoop)
	// so both sentinels appear together once swap+tidy returns clean.
	recFinal, ok := task.migrationRecord(shard)
	require.True(t, ok)
	assert.Equal(t, MigrationStateSwapped, recFinal.State(),
		"post-SWAP: runShardSwapPhase decided the flip and moved the bucket pointer")
	displaced, hasDisplaced := recFinal.(MigrationRecordSwapped).DisplacedDir("title")
	require.True(t, hasDisplaced, "post-SWAP: the flip records the directory it displaced")
	assert.False(t, dirExists(filepath.Join(shard.pathLSM(), displaced)),
		"post-SWAP: the displaced directory is removed at the handle the record names")
	assert.True(t, strategy.migrationCompleted,
		"post-SWAP: OnMigrationComplete must have fired (tail of every recovery branch)")

	// Bucket strategy must have flipped to Inverted.
	bucketName := helpers.BucketSearchableFromPropNameLSM("title")
	postBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, postBucket, "post-SWAP: searchable bucket must still exist")
	assert.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-SWAP: searchable bucket strategy must be Inverted")
}

// TestReindexProviderBarrierIntegration_CrashAfterPersistRecoveryRecord
// pins the contract that a crash between persistRecoveryRecord and the
// migration's first durable record leaves the system in a state where
// recovery discovery (DiscoverInFlightReindexTasks) safely skips the
// half-initialized migration directory — i.e. the worst case is "no
// recovery work to do", not "load corrupt recovery state".
//
// Why this matters: persistRecoveryRecord is what allows post-restart
// recovery to rebuild the right ShardReindexTaskGeneric strategy +
// generation. If we wrote payload.mig and then crashed before the
// iteration could even begin (no record ever appears), the DTM
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

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 10)

	// Construct a task instance (matches what processOneUnit's
	// createReindexTasks would produce for ChangeAlgorithm/MapToBlockmax).
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// Build a synthetic task + payload that processOneUnit would have
	// constructed before calling persistRecoveryRecord.
	taskID := "test-crash-after-persist-" + uuid.NewString()[:8]
	dtmTask := &distributedtask.Task{
		Namespace: ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{
			ID:      taskID,
			Version: 1,
		},
	}
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeChangeAlgorithm,
		Collection:    className,
		Properties:    []string{"title"},
		UnitToShard:   map[string]string{"unit-1": shard.Name()},
		UnitToNode:    map[string]string{"unit-1": "node1"},
	}

	// Call persistRecoveryRecord ALONE — simulating a crash immediately
	// after this write but before markStarted / iteration.
	p, _ := barrierIntegrationProvider(t)
	require.NoError(t, p.persistRecoveryRecord(dtmTask, payload, "unit-1",
		shard, []*ShardReindexTaskGeneric{task}))

	// Sanity: payload.mig is on disk in the migration dir.
	migDir := task.migrationPath(shard.pathLSM())
	payloadPath := filepath.Join(migDir, reindexRecoveryPayloadFile)
	rawPayload, err := os.ReadFile(payloadPath)
	require.NoError(t, err, "payload.mig must exist after persistRecoveryRecord")
	require.NotEmpty(t, rawPayload, "payload.mig must not be empty")
	var decoded reindexRecoveryRecord
	require.NoError(t, json.Unmarshal(rawPayload, &decoded),
		"payload.mig must round-trip as valid JSON — recovery's json.Unmarshal would fail otherwise")
	require.Equal(t, taskID, decoded.TaskID, "recovery record must preserve taskID")
	require.Equal(t, "unit-1", decoded.UnitID, "recovery record must preserve unitID")

	// Sanity: no record was written (the crash beat the iteration to it).
	// This is the invariant the discover path keys off.
	records, frozen, _ := migrationRecordsAt(shard.pathLSM(), idx.logger)
	require.False(t, frozen)
	require.Empty(t, records, "no record may exist — iteration never ran")

	// Now simulate process restart: DiscoverInFlightReindexTasks walks
	// the data dir and must SKIP this migration. We pass nil schemaManager —
	// the discover path is read-only against disk and never invokes schema
	// operations until buildRecoveryTasks fires, which needs a record whose
	// rebuild is complete and whose flip is not yet decided.
	rootPath := idx.Config.RootPath
	recovered, err := DiscoverInFlightReindexTasks(rootPath, idx.logger, nil)
	require.NoError(t, err, "discover must not error on a recordless dir")
	for _, r := range recovered {
		assert.NotEqualf(t, taskID, r.Descriptor.ID,
			"discover MUST skip a migration with no record (load-bearing for the crash between persisting the payload and the first durable state)")
	}

	// Confirm payload.mig is still intact: a retry of processOneUnit
	// could call persistRecoveryRecord again with the same content
	// (idempotent — same TaskID + UnitID + Payload → bytes.Equal short
	// circuit at SaveRecoveryPayload line 279). Re-call to verify
	// idempotency.
	require.NoError(t, p.persistRecoveryRecord(dtmTask, payload, "unit-1",
		shard, []*ShardReindexTaskGeneric{task}),
		"persistRecoveryRecord must be idempotent against an existing identical record")
	rawPayload2, err := os.ReadFile(payloadPath)
	require.NoError(t, err)
	assert.Equal(t, rawPayload, rawPayload2,
		"idempotent persist must leave the file bit-identical (no rewrite)")
}

// TestReindexProviderBarrierIntegration_MarkReindexedDurabilityBarrier
// pins commit 073d47b460 (weaviate/0-weaviate-issues#214):
// FlushAndSwitch happens BEFORE the Iterated record, so a restart that
// dispatches on that record never sees it without the data behind it. A
// refactor that moved FlushAndSwitch after the record write would fail here.
func TestReindexProviderBarrierIntegration_MarkReindexedDurabilityBarrier(t *testing.T) {
	ctx := testCtx()
	className := "BarrierIntegDurability"
	class := newTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

	// Drive iteration to the barrier point — the Iterated record is written
	// AFTER the FlushAndSwitch durability barrier.
	task, _ := barrierIntegrationDrivenToReindexed(t, ctx, shard, idx.logger)

	// Read the record off disk rather than out of the shard's store, so a
	// value the process is only holding in memory cannot pass for a durable
	// one.
	recordPath := filepath.Join(shard.pathLSM(), migrationsDir, "records",
		task.migrationRecordKey().fileName())
	preContent, err := os.ReadFile(recordPath)
	require.NoError(t, err, "the record must be on disk after iteration")
	require.NotEmpty(t, preContent, "the record must not be empty")
	recPre, err := decodeMigrationRecord(preContent)
	require.NoError(t, err, "the record on disk must decode")
	require.Equal(t, MigrationStateIterated, recPre.State())

	// Verify the per-property reindex bucket has on-disk segments at this
	// point — the FlushAndSwitch barrier means iteration data is durable
	// in segment files, not just memtables. The reindex bucket sits at
	// <lsm>/<bucketName + ReindexSuffix>/<segment files>.
	reindexDirName := helpers.BucketSearchableFromPropNameLSM("title") +
		"__blockmax_reindex" + genSuffix(1)
	reindexBucketDir := filepath.Join(shard.pathLSM(), reindexDirName)
	stat, err := os.Stat(reindexBucketDir)
	require.NoError(t, err, "reindex bucket dir must exist after FlushAndSwitch barrier")
	require.True(t, stat.IsDir(), "reindex bucket path must be a directory")

	// Shut the shard down — this exercises the LSM store's flush + close
	// path. Anything that's still memtable-only at this point would be
	// lost; everything segment-backed survives.
	require.NoError(t, shard.Shutdown(ctx))

	// Post-shutdown: the record must be readable with bit-identical content.
	// If a refactor introduces ANY lazy-write path for it, or removes the
	// FlushAndSwitch barrier so the bucket's memtables are lost, this read
	// surfaces the regression.
	postContent, err := os.ReadFile(recordPath)
	require.NoError(t, err,
		"the record must persist across shard shutdown (FlushAndSwitch durability barrier contract)")
	assert.Equal(t, preContent, postContent,
		"the record's content must be bit-identical across shutdown")

	// Reindex bucket dir must also persist on disk. If FlushAndSwitch
	// returned but the segments weren't really on disk, the file might
	// be missing after shard close — the test guards against that
	// regression too.
	statPost, err := os.Stat(reindexBucketDir)
	require.NoError(t, err,
		"reindex bucket dir must persist across shutdown (FlushAndSwitch barrier ⇒ segments are on disk)")
	require.True(t, statPost.IsDir())

	// Re-read the record the way a real startup does. This pins the
	// end-to-end contract: the barrier persisted, and the recovery path sees
	// the state that dispatches RunSwapOnShard into its resume branch.
	recovered, frozen, _ := migrationRecordsAt(shard.pathLSM(), idx.logger)
	require.False(t, frozen)
	require.Len(t, recovered, 1)
	assert.Equal(t, MigrationStateIterated, recovered[0].State(),
		"the recovered record must still say Iterated: the barrier means the state never outruns its data")
	assert.False(t, recovered[0].DataCommitted(),
		"the barrier path stops at Iterated; nothing is staged yet")
}
