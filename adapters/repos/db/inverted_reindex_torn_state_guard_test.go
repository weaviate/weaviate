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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A durable claim that the rebuild finished, which the data on disk does not
// back, has to be distrusted and the rebuild re-run (#240 Symptom A, #244).
// [TestReconcileReverseEdge] pins that edge against a synthetic fixture. What
// these two tests add is that a real shard load runs it, that the re-run
// converges on exactly what a clean migration produces, and that the edge does
// not fire once the migration is past the state it belongs to.

const (
	tornGuardNumObjects = 25
	tornGuardPropName   = "title"
)

// runTornStateMigrationToIterated drives a fresh shard to a complete rebuild
// and NO further, so the reverse edge's preconditions hold.
func runTornStateMigrationToIterated(t *testing.T, ctx context.Context,
	className string, class *models.Class,
) (*Shard, *Index, *ShardReindexTaskGeneric) {
	t.Helper()

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeConvergenceTestObjects(t, tornGuardNumObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	task.skipSwapOnFinish.Store(true) // halt at a complete rebuild, BEFORE the swap
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	require.Equal(t, MigrationStateIterated, tornGuardStateOf(t, shard, task),
		"precondition: the rebuild must be recorded complete and no further")

	return shard, idx, task
}

func tornGuardStateOf(t *testing.T, shard *Shard, task *ShardReindexTaskGeneric) MigrationState {
	t.Helper()
	rec, ok := shard.migrationRecords.Get(task.migrationRecordKey())
	require.True(t, ok, "the migration should have a record on this shard")
	return rec.State()
}

// tornGuardReload shuts the shard down and loads it again with a fresh task
// installed, which is the only route that runs reconciliation.
func tornGuardReload(t *testing.T, ctx context.Context, shard *Shard, idx *Index,
	class *models.Class,
) (*Shard, *ShardReindexTaskGeneric) {
	t.Helper()
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	task := newTestTask(idx.logger, &testMigrationStrategy{
		MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1},
	})
	idx.shardReindexer = &testShardReindexer{task: task}

	shd, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "shard re-init must succeed")
	idx.shards.Store(shardName, shd)
	return shd.(*Shard), task
}

// TestTornState_RebuiltDataGone_ReiteratesToBaseline pins the reverse edge
// end to end: a rebuild whose data is gone restarts from the beginning, and
// the restarted rebuild produces exactly the index a clean run produces.
//
// Resuming instead from the recorded checkpoint would swap in a bucket holding
// only the objects that happen to sort above the stale key, which is why the
// checkpoint has to clear along with the state
// (weaviate/0-weaviate-issues#244).
func TestTornState_RebuiltDataGone_ReiteratesToBaseline(t *testing.T) {
	baseline := computeBaselineFingerprint(t, tornGuardPropName, tornGuardNumObjects)
	require.NotEmpty(t, baseline)

	ctx := testCtx()
	className := "TornGuardReiterate_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{tornGuardPropName})
	shard, idx, task := runTornStateMigrationToIterated(t, ctx, className, class)

	// Remove the directory the rebuild wrote into while the record claiming it
	// complete survives: a crash before any iteration data reached disk, or a
	// restore that materialized the class tree without it.
	reindexDir := filepath.Join(shard.pathLSM(), task.reindexBucketName(tornGuardPropName))
	require.DirExists(t, reindexDir, "fixture: the rebuild's dir must exist before we remove it")
	require.NoError(t, os.RemoveAll(reindexDir))

	shard2, task2 := tornGuardReload(t, ctx, shard, idx, class)
	defer shard2.Shutdown(ctx)

	rec, ok := shard2.migrationRecords.Get(task2.migrationRecordKey())
	require.True(t, ok)
	require.Equal(t, MigrationStateIterating, rec.State(),
		"a record whose rebuilt data is gone must be distrusted and the rebuild re-run")
	require.Equal(t, MigrationCheckpoint{}, rec.(MigrationRecordIterating).Checkpoint(),
		"the checkpoint has to clear with the state, or the rebuild resumes past data it never wrote")

	for {
		rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	bucket := shard2.store.Bucket(helpers.BucketSearchableFromPropNameLSM(tornGuardPropName))
	require.NotNil(t, bucket)
	require.Equal(t, lsmkv.StrategyInverted, bucket.Strategy())

	got := fingerprintInvertedBucket(t, bucket)
	assert.Equal(t, len(baseline), len(got),
		"the re-run must produce the baseline's term count, not a truncated resume")
	for term, expectedIDs := range baseline {
		gotIDs, ok := got[term]
		assert.Truef(t, ok, "term %q in baseline missing after the re-run", term)
		assert.Equalf(t, expectedIDs, gotIDs,
			"term %q after the re-run diverges from baseline", term)
	}
}

// TestTornState_CommittedRecordKeepsItsMissingSidecarDirs pins the edge's
// scope. Once the staged data is complete the rebuild's own directories have
// been consumed and removed on purpose, so their absence is correct rather
// than torn. Reading it as torn would send every restart of a committed
// migration back to iteration.
func TestTornState_CommittedRecordKeepsItsMissingSidecarDirs(t *testing.T) {
	ctx := testCtx()
	className := "TornGuardCommitted_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{tornGuardPropName})
	shard, idx, _ := runTornStateMigrationToIterated(t, ctx, className, class)

	prepTask := newTestTask(idx.logger, &testMigrationStrategy{
		MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1},
	})
	require.NoError(t, prepTask.RunPrepareOnShard(ctx, shard))
	require.Equal(t, MigrationStateMerged, tornGuardStateOf(t, shard, prepTask))

	reindexDir := filepath.Join(shard.pathLSM(), prepTask.reindexBucketName(tornGuardPropName))
	require.NoDirExists(t, reindexDir,
		"fixture: the prep must have removed the rebuild's dir; a failure here is the fixture, not the edge")

	shard2, task2 := tornGuardReload(t, ctx, shard, idx, class)
	defer shard2.Shutdown(ctx)

	assert.Equal(t, MigrationStateMerged, tornGuardStateOf(t, shard2, task2),
		"a committed migration must not be sent back to iteration by its consumed rebuild dirs")
}
