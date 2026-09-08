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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A migration can be interrupted in any state its record holds, and every one of
// them is reached here by calling the entry point that writes it. Nothing in
// this file plants a record or a directory: a fabricated state proves only that
// the fabrication was accepted.
//
// The restart is the point. [TestRunSwapOnShard_DispatchMatrix] drives the same
// states in-process; what a shard load adds is reconciliation, which owns the
// promotion and the retirement and which an in-process run never runs.

// Every state the record machine writes, in the order it writes them.
var allMigrationStates = []MigrationState{
	MigrationStateIterating,
	MigrationStateIterated,
	MigrationStateMerged,
	MigrationStateSwapped,
	MigrationStatePromoted,
}

// Promotion is written by reconciliation, which runs on shard load, so it is not
// among the states an in-process run reaches.
var migrationStatesBeforePromotion = allMigrationStates[:len(allMigrationStates)-1]

const migrationRestartObjects = 25

// driveToMigrationState stops a migration at one recorded state using production
// entry points only. There is nothing between merged and swapped: the swap
// writes its record before the first pointer moves, so a crash inside it reads
// as swapped either way.
func driveToMigrationState(t *testing.T, ctx context.Context, shard *Shard,
	task *ShardReindexTaskGeneric, state MigrationState,
) {
	t.Helper()
	switch state {
	case MigrationStateIterating:
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	case MigrationStateIterated:
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	case MigrationStateMerged:
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
		require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	case MigrationStateSwapped:
		require.NoError(t, task.RunOnShard(ctx, shard))
	default:
		t.Fatalf("no drive reaches %q without a shard load", state)
	}
}

// seedConvergenceObjects and checkConvergenceTokensIndexed are the seed and the
// baseline guard every matrix over a text property shares: a clean run has to
// have indexed each token the dictionary writes, or a defect that indexes
// nothing would sit in the baseline and every row would agree with it.
func seedConvergenceObjects(t *testing.T, ctx context.Context, shard *Shard, className string) {
	t.Helper()
	for _, obj := range makeConvergenceTestObjects(t, migrationRestartObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
}

func checkConvergenceTokensIndexed(t *testing.T, fingerprint map[string][]uint64) {
	t.Helper()
	for _, token := range convergenceTokens {
		require.NotEmptyf(t, fingerprint[token],
			"a clean run must index token %q, which the fixture writes", token)
	}
}

// migrationRestartFixture holds the one shard under test and the schema the
// cluster would hold for it. The class is a pointer the migration writes back
// into, because a restart reads the schema, not the record, to decide whether to
// open a migrated property's bucket.
type migrationRestartFixture struct {
	idx       *Index
	class     *models.Class
	shard     *Shard
	shardName string
}

// restart closes the shard and loads it again. Shard load is where
// reconciliation runs, so this is the only way a test reaches the promotion and
// the retirement.
//
// No reindexer is installed on the way back up: startup recovery re-arms a
// migration it found in flight, and installing one unconditionally would start a
// fresh migration on a shard whose migration is already finished.
func (f *migrationRestartFixture) restart(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, f.shard.Shutdown(ctx))

	reloaded, err := f.idx.initShard(ctx, f.shardName, f.class, nil, true, true)
	require.NoError(t, err, "the shard must load again")
	f.idx.shards.Store(f.shardName, reloaded)
	f.shard = reloaded.(*Shard)
}

// recordState reports the migration's recorded state, and whether a record is
// there at all. Reconciliation removes the record once the promotion is durable
// and the schema confirms it, so "gone" is a state of its own.
func (f *migrationRestartFixture) recordState(t *testing.T,
	task *ShardReindexTaskGeneric,
) (MigrationState, bool) {
	t.Helper()
	rec, ok := task.migrationRecord(f.shard)
	if !ok {
		return "", false
	}
	return rec.State(), true
}

func (f *migrationRestartFixture) bucket(name string) *lsmkv.Bucket {
	return f.shard.store.Bucket(name)
}

// migrationRestartMatrix is the shared body of the per-strategy "a restart from
// every interrupted state converges" matrices. Only the fixture and the migrated
// bucket vary per strategy; which states exist, how each is reached, and what a
// restart owes at each are the record machine's, and live here.
//
// K is the fingerprint key type: a term for the searchable and filterable
// strategies, a lexicographic key for the rangeable one.
type migrationRestartMatrix[K comparable] struct {
	// namePrefix seeds the throw-away collection names.
	namePrefix string
	// buildClass and seedObjects build the pre-migration fixture.
	buildClass  func(className string) *models.Class
	seedObjects func(t *testing.T, ctx context.Context, shard *Shard, className string)
	// buildTask returns a fresh task plus a reader for its completion flag. It
	// takes the fixture because a strategy whose completion hook commits schema
	// has to commit it into the class the next load reads.
	buildTask func(t *testing.T, f *migrationRestartFixture) (*ShardReindexTaskGeneric, func() bool)
	// bucketName is the migrated property's canonical bucket and wantStrategy
	// the strategy it must carry once the migration has settled.
	bucketName   string
	wantStrategy string
	// servesBeforeMigration is true where the property already answers from
	// bucketName before the migration runs. Where it is false the migration
	// creates the bucket, and a load below the flip must leave no bucket at all.
	servesBeforeMigration bool
	fingerprint           func(t *testing.T, b *lsmkv.Bucket) map[K][]uint64
	// checkBaseline asserts what a clean run must have indexed. A baseline taken
	// from the same build as the rows would otherwise absorb a defect they
	// share, and every row would agree on an empty index.
	checkBaseline func(t *testing.T, fingerprint map[K][]uint64)
}

// run takes the clean-run baseline once, then interrupts a migration in each
// state and requires a restart from it to converge on that baseline.
func (m migrationRestartMatrix[K]) run(t *testing.T) {
	baseline := m.baseline(t)
	for _, state := range allMigrationStates {
		t.Run(string(state), func(t *testing.T) {
			m.runCase(t, state, baseline)
		})
	}
}

// baseline is one uninterrupted migration followed by the load that promotes it,
// so the rows compare against an index reached the same way theirs is.
func (m migrationRestartMatrix[K]) baseline(t *testing.T) map[K][]uint64 {
	t.Helper()
	ctx := testCtx()
	f := m.newFixture(t, ctx, m.namePrefix+"Clean")

	task, completed := m.buildTask(t, f)
	require.NoError(t, task.RunOnShard(ctx, f.shard))
	require.True(t, completed(), "a clean run must complete the migration")

	f.restart(t, ctx)

	bucket := f.bucket(m.bucketName)
	require.NotNil(t, bucket, "a clean run must leave the migrated bucket serving")

	fingerprint := m.fingerprint(t, bucket)
	require.NotEmpty(t, fingerprint,
		"a baseline with no keys cannot anchor a convergence assertion")
	m.checkBaseline(t, fingerprint)
	return fingerprint
}

func (m migrationRestartMatrix[K]) newFixture(t *testing.T, ctx context.Context,
	namePrefix string,
) *migrationRestartFixture {
	t.Helper()
	className := namePrefix + "_" + uuid.NewString()[:8]
	class := m.buildClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	m.seedObjects(t, ctx, shard, className)

	f := &migrationRestartFixture{idx: idx, class: class, shard: shard, shardName: shard.Name()}
	// Whichever shard the last restart left behind, not the one opened here.
	t.Cleanup(func() { f.shard.Shutdown(ctx) })
	return f
}

// driveTo stops the migration at state. Promotion is reconciliation's to write,
// so the only route to a promoted record is a load over a swapped one.
func (m migrationRestartMatrix[K]) driveTo(t *testing.T, ctx context.Context,
	f *migrationRestartFixture, task *ShardReindexTaskGeneric, state MigrationState,
) {
	t.Helper()
	if state == MigrationStatePromoted {
		driveToMigrationState(t, ctx, f.shard, task, MigrationStateSwapped)
		f.restart(t, ctx)
		return
	}
	driveToMigrationState(t, ctx, f.shard, task, state)
}

// runCase interrupts a migration at one state, restarts, lets the unit relaunch
// if it had not reported complete, and requires the index to end up bit-equal to
// a clean run.
//
// The two failures this is built to catch are opposite: a restart that reports
// the migration finished over an index nothing rebuilt, and a restart that
// abandons a rebuild that was already complete.
func (m migrationRestartMatrix[K]) runCase(t *testing.T, state MigrationState,
	baseline map[K][]uint64,
) {
	ctx := testCtx()
	f := m.newFixture(t, ctx, m.namePrefix+string(state))

	task, _ := m.buildTask(t, f)
	m.driveTo(t, ctx, f, task, state)

	reached, present := f.recordState(t, task)
	require.True(t, present, "the drive must leave a record")
	require.Equal(t, state, reached,
		"the drive landed somewhere other than the state this row interrupts")

	// Below the flip the rebuild is the task's to finish and the load must not
	// touch it; at or above it the load owes the promotion, and the load after
	// that owes the retirement.
	flipped := state == MigrationStateSwapped || state == MigrationStatePromoted

	f.restart(t, ctx)

	settledOnce, present := f.recordState(t, task)
	switch {
	case !flipped:
		require.True(t, present, "the load must not drop a record it has not settled")
		assert.Equal(t, state, settledOnce,
			"a load below the flip leaves the record where the interrupted run left it")
	case state == MigrationStateSwapped:
		require.True(t, present, "the load owes the promotion, not the retirement")
		assert.Equal(t, MigrationStatePromoted, settledOnce,
			"the state one load leaves behind")
	default:
		assert.False(t, present, "the load after the promotion retires the record")
	}

	// Which way this goes is the whole difference between the two halves of the
	// matrix. Below the flip a live index would be one a load opened over a
	// rebuild that never finished; at or above it, a load that leaves no index
	// has lost the rebuild.
	if flipped || m.servesBeforeMigration {
		assert.NotNil(t, f.bucket(m.bucketName),
			"the index must be serving after a load that owes the promotion")
	} else {
		assert.Nil(t, f.bucket(m.bucketName),
			"a migration whose flip is not decided must not leave its index live")
	}

	if !flipped {
		// The unit had not reported complete, which is what makes the
		// distributed task manager run it again.
		relaunched, completed := m.buildTask(t, f)
		require.NoError(t, relaunched.RunOnShard(ctx, f.shard),
			"the relaunched unit must run to completion")

		reached, present = f.recordState(t, relaunched)
		require.True(t, present)
		assert.Equal(t, MigrationStateSwapped, reached,
			"a relaunch must reach the flip rather than report the unit done without it")
		assert.True(t, completed(),
			"a relaunch must run the completion hook that commits the schema")
	}

	f.restart(t, ctx)

	settled, present := f.recordState(t, task)
	if flipped {
		assert.False(t, present,
			"once the promotion is durable and the schema shows it, the record is retired")
	} else {
		require.True(t, present)
		assert.Equal(t, MigrationStatePromoted, settled,
			"the load after a relaunch's flip promotes it")
	}

	bucket := f.bucket(m.bucketName)
	require.NotNil(t, bucket,
		"the migrated property must be serving from its canonical bucket")
	require.Equal(t, m.wantStrategy, bucket.Strategy(),
		"the canonical bucket must carry the strategy the migration produces")

	// Compared per key so a failure names the posting list that diverged instead
	// of dumping the whole index.
	got := m.fingerprint(t, bucket)
	assert.Equal(t, len(baseline), len(got), "key count diverges from a clean run")
	for key, wantIDs := range baseline {
		gotIDs, ok := got[key]
		if !ok {
			assert.Failf(t, "missing key",
				"key %v is in a clean run but not after recovery", key)
			continue
		}
		assert.Equalf(t, wantIDs, gotIDs,
			"key %v holds a different set of objects than a clean run\n  clean (%d): %v\n  got   (%d): %v",
			key, len(wantIDs), wantIDs, len(gotIDs), gotIDs)
	}
}
