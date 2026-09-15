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

// Every interrupted state here is reached by calling the entry point that writes
// it; nothing is planted, since a fabricated state proves only that the
// fabrication was accepted. What a shard load adds over the in-process
// [TestRunSwapOnShard_DispatchMatrix] is reconciliation, which owns the
// promotion and the retirement.

// Every state the record machine writes, in the order it writes them.
var allMigrationStates = []MigrationState{
	MigrationStateIterating,
	MigrationStateIterated,
	MigrationStateMerged,
	MigrationStateSwapped,
	MigrationStatePromoted,
}

// Reconciliation writes promotion on shard load, so an in-process run never
// reaches it.
var migrationStatesBeforePromotion = allMigrationStates[:len(allMigrationStates)-1]

const migrationRestartObjects = 25

// driveToMigrationState stops a migration at one recorded state through
// production entry points. Nothing sits between merged and swapped: the swap
// records itself before the first pointer moves.
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

// The seed and baseline guard every text-property matrix shares. Without the
// guard, a defect that indexes nothing would sit in the baseline and every row
// would agree with it.
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

// migrationRestartFixture holds the shard under test and the schema the cluster
// would hold for it. The class is a pointer the migration writes back into,
// since a restart reads the schema, not the record, to decide what to open.
type migrationRestartFixture struct {
	idx       *Index
	class     *models.Class
	shard     *Shard
	shardName string
}

// restart closes the shard and loads it again, which is the only way a test
// reaches reconciliation's promotion and retirement.
//
// No reindexer is installed on the way back up: startup recovery re-arms a
// migration it found in flight, and installing one would start a fresh
// migration on a shard whose migration is already finished.
func (f *migrationRestartFixture) restart(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, f.shard.Shutdown(ctx))

	reloaded, err := f.idx.initShard(ctx, f.shardName, f.class, nil, true, true)
	require.NoError(t, err, "the shard must load again")
	f.idx.shards.Store(f.shardName, reloaded)
	f.shard = reloaded.(*Shard)
}

// recordState reports the recorded state and whether a record is there at all:
// reconciliation removes it once promoted, so "gone" is a state of its own.
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
// every interrupted state converges" matrices. Which states exist and what a
// restart owes at each belong to the record machine and live here; only the
// fixture and the migrated bucket vary per strategy.
//
// K is the fingerprint key type: a term for searchable and filterable, a
// lexicographic key for rangeable.
type migrationRestartMatrix[K comparable] struct {
	// namePrefix seeds the throw-away collection names.
	namePrefix string
	// buildClass and seedObjects build the pre-migration fixture.
	buildClass  func(className string) *models.Class
	seedObjects func(t *testing.T, ctx context.Context, shard *Shard, className string)
	// buildTask returns a fresh task plus a reader for its completion flag. It
	// takes the fixture so a strategy whose completion hook commits schema
	// commits it into the class the next load reads.
	buildTask func(t *testing.T, f *migrationRestartFixture) (*ShardReindexTaskGeneric, func() bool)
	// bucketName is the migrated property's canonical bucket and wantStrategy
	// the strategy it must carry once the migration has settled.
	bucketName   string
	wantStrategy string
	// servesBeforeMigration is true where the property already answers from
	// bucketName. Where false the migration creates the bucket, and a load below
	// the flip must leave no bucket at all.
	servesBeforeMigration bool
	fingerprint           func(t *testing.T, b *lsmkv.Bucket) map[K][]uint64
	// checkBaseline asserts what a clean run must have indexed; without it the
	// baseline absorbs a defect the rows share and they all agree on it.
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

// One uninterrupted migration plus the load that promotes it, so the rows
// compare against an index reached the same way theirs is.
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

// driveTo stops the migration at state. Only reconciliation writes promotion,
// so the route there is a load over a swapped record.
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

// runCase interrupts a migration at one state, restarts, and requires the index
// to end up bit-equal to a clean run. The two failures it catches are opposite:
// reporting finished over an index nothing rebuilt, and abandoning a rebuild
// that was already complete.
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

	// Below the flip the rebuild is the task's to finish; at or above it the load
	// owes the promotion, and the next one the retirement.
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

	// Below the flip a live index means a load opened over an unfinished rebuild;
	// at or above it, no index means the load lost the rebuild.
	if flipped || m.servesBeforeMigration {
		assert.NotNil(t, f.bucket(m.bucketName),
			"the index must be serving after a load that owes the promotion")
	} else {
		assert.Nil(t, f.bucket(m.bucketName),
			"a migration whose flip is not decided must not leave its index live")
	}

	if !flipped {
		// The unit never reported complete, so the task manager runs it again.
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

	// Per key, so a failure names the posting list that diverged rather than
	// dumping the whole index.
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
