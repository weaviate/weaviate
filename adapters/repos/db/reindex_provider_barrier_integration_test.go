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
// T3: Record durability at Iterated — the record survives process death
//     without the test fsyncing.

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

// barrierIntegrationDrivenToReindexed halts iteration at the Iterated record
// (the FINALIZING-barrier handoff).
func barrierIntegrationDrivenToReindexed(
	t *testing.T,
	ctx context.Context,
	shard *Shard,
	logger logrus.FieldLogger,
) (*ShardReindexTaskGeneric, *testMigrationStrategy) {
	t.Helper()
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(logger, strategy, shard.migrationUnit())

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	// Sanity: iteration must have halted at the barrier (the Iterated record
	// written, runtimePrepare NOT called).
	rec, ok := task.migrationRecord(shard)
	require.True(t, ok, "helper precondition: iteration must leave a record")
	require.Equal(t, MigrationStateIterated, rec.State(),
		"helper precondition: RunReindexOnlyOnShard must reach Iterated")
	require.False(t, rec.StagedDataComplete(),
		"helper precondition: RunReindexOnlyOnShard must NOT run runtimePrepare")
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

func TestReindexProviderBarrierIntegration_OnGroupCompletedPrep(t *testing.T) {
	ctx := testCtx()
	className := "BarrierIntegPrep"
	class := newTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

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

	recPost, ok := task.migrationRecord(shard)
	require.True(t, ok)
	assert.Equal(t, MigrationStateMerged, recPost.State(),
		"post-PREP: RunPrepareOnShard advanced the record from Iterated to Merged")
	assert.False(t, recPost.FlipDecided(),
		"post-PREP: the flip must NOT be decided yet, that is OnSwapRequested's job")
}

// TestReindexProviderBarrierIntegration_OnSwapRequestedSwap pins the
// SWAP-phase contract: given a unit at Merged (the state the PREP
// barrier produces), the provider's runShardSwapPhase must record the flip
// and remove the directory it displaced. This is the
// "OnSwapRequested arrival after Phase A.5 transition" gap T2.3 was
// scoped against.
//
// The test runs PREP first (via runShardPrepPhase) to stage the
// Merged state, then runs SWAP — mirroring the cluster-wide barrier:
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

	task, strategy := barrierIntegrationDrivenToReindexed(t, ctx, shard, idx.logger)

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

	recFinal, ok := task.migrationRecord(shard)
	require.True(t, ok)
	assert.Equal(t, MigrationStateSwapped, recFinal.State(),
		"post-SWAP: runShardSwapPhase decided the flip and moved the bucket pointer")
	displaced, hasDisplaced := recFinal.(MigrationRecordSwapped).DisplacedDir("title")
	require.True(t, hasDisplaced, "post-SWAP: the flip records the directory it displaced")
	assert.False(t, dirExists(t, filepath.Join(shard.pathLSM(), displaced)),
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

func TestReindexProviderBarrierIntegration_IteratedRecordDurabilityBarrier(t *testing.T) {
	ctx := testCtx()
	className := "BarrierIntegDurability"
	class := newTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	barrierIntegrationSeedObjects(t, ctx, shard, className, 25)

	task, _ := barrierIntegrationDrivenToReindexed(t, ctx, shard, idx.logger)

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

	recovered, someRecordsUnreadable, _ := migrationRecordsAt(shard.pathLSM(), idx.logger)
	require.False(t, someRecordsUnreadable)
	require.Len(t, recovered, 1)
	assert.Equal(t, MigrationStateIterated, recovered[0].State(),
		"the recovered record must still say Iterated: the barrier means the state never outruns its data")
	assert.False(t, recovered[0].StagedDataComplete(),
		"the barrier path stops at Iterated; nothing is staged yet")
}
