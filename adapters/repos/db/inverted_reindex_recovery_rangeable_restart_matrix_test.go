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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A range-filter migration can be interrupted in any of the five states its
// record has, and every one of them is reached here by calling the entry point
// that writes it. Nothing in this file plants a record or a directory: a
// fabricated state proves only that the fabrication was accepted.
//
// The restart is the point. [TestRunSwapOnShard_DispatchMatrix] already drives
// the same states in-process; what a shard load adds is reconciliation, which
// owns the promotion and the retirement and which an in-process run never runs.

const rangeableRestartObjects = 25

// rangeableRestartFixture holds the one shard under test and the schema the
// cluster would hold for it. The class is a pointer the migration writes back
// into, because a restart reads the schema, not the record, to decide whether
// to open the property's rangeable bucket.
type rangeableRestartFixture struct {
	idx       *Index
	class     *models.Class
	shard     *Shard
	shardName string
}

// flipSchema stands in for the RAFT round trip in
// [FilterableToRangeableStrategy.OnMigrationComplete], which sets
// IndexRangeFilters on every migrated property. Without it the next load reads
// a property with no rangeable index and never opens the bucket the promotion
// just renamed into place, so every row would report an empty index.
func (f *rangeableRestartFixture) flipSchema() error {
	enabled := true
	for _, prop := range f.class.Properties {
		if prop.Name == filterableToRangeablePropName {
			prop.IndexRangeFilters = &enabled
		}
	}
	return nil
}

// newTask builds the migration this fixture runs, wired so that completing it
// updates the fixture's schema the way the production hook does.
func (f *rangeableRestartFixture) newTask(t *testing.T) (*ShardReindexTaskGeneric, func() bool) {
	t.Helper()
	task, wrapped := newFilterableToRangeableTask(t, f.idx, f.class.Class,
		filterableToRangeablePropName, testMigrationUnitFor(f.idx, f.shardName))
	wrapped.onComplete = f.flipSchema
	return task, func() bool { return wrapped.migrationCompleted }
}

// restart closes the shard and loads it again. Shard load is where
// reconciliation runs, so this is the only way a test reaches the promotion and
// the retirement.
//
// No reindexer is installed on the way back up: startup recovery re-arms a
// migration it found in flight, and installing one unconditionally would start
// a fresh migration on a shard whose migration is already finished.
func (f *rangeableRestartFixture) restart(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, f.shard.Shutdown(ctx))

	reloaded, err := f.idx.initShard(ctx, f.shardName, f.class, nil, true, true)
	require.NoError(t, err, "the shard must load again")
	f.idx.shards.Store(f.shardName, reloaded)
	f.shard = reloaded.(*Shard)
}

// recordState reports the migration's recorded state, and whether a record is
// there at all. Reconciliation removes the record once the promotion is
// durable and the schema confirms it, so "gone" is a state of its own.
func (f *rangeableRestartFixture) recordState(t *testing.T,
	task *ShardReindexTaskGeneric,
) (MigrationState, bool) {
	t.Helper()
	rec, ok := task.migrationRecord(f.shard)
	if !ok {
		return "", false
	}
	return rec.State(), true
}

func (f *rangeableRestartFixture) rangeableBucket() *lsmkv.Bucket {
	return f.shard.store.Bucket(
		helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName))
}

func newRangeableRestartFixture(t *testing.T, ctx context.Context, namePrefix string) *rangeableRestartFixture {
	t.Helper()
	className := namePrefix + "_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeFilterableToRangeableTestObjects(t, rangeableRestartObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	f := &rangeableRestartFixture{idx: idx, class: class, shard: shard, shardName: shard.Name()}
	// Whichever shard the last restart left behind, not the one opened here.
	t.Cleanup(func() { f.shard.Shutdown(ctx) })
	return f
}

// rangeableRestartCase is one interrupted state and what a restart from it owes.
type rangeableRestartCase struct {
	// interruptAt is the recorded state the migration is stopped in.
	interruptAt MigrationState
	// driveTo stops the migration at interruptAt using production entry points
	// only. It may restart the fixture, which is the only way to reach a state
	// reconciliation writes.
	driveTo func(t *testing.T, ctx context.Context, f *rangeableRestartFixture, task *ShardReindexTaskGeneric)

	// afterRestart is the record state one shard load leaves behind, and
	// indexLive says whether that load may leave the property's rangeable
	// bucket serving. A migration whose flip is not decided must not go live.
	afterRestart     MigrationState
	afterRestartGone bool
	indexLive        bool

	// relaunches is true where the unit had not reported complete, which is
	// what makes the distributed task manager run it again.
	relaunches bool

	// settled is the record state after the load that follows the flip. The
	// record is removed once the promotion is durable and the schema shows it.
	settled     MigrationState
	settledGone bool
}

func rangeableRestartCases() []rangeableRestartCase {
	// A rebuild that has not been flipped is resumed by the task, not by the
	// load, so these three share one shape: the load leaves the record alone
	// and withholds the index, and the relaunch finishes the job.
	unflipped := func(at MigrationState) rangeableRestartCase {
		return rangeableRestartCase{
			interruptAt:  at,
			afterRestart: at,
			indexLive:    false,
			relaunches:   true,
			settled:      MigrationStatePromoted,
		}
	}

	iterating := unflipped(MigrationStateIterating)
	iterating.driveTo = func(t *testing.T, ctx context.Context, f *rangeableRestartFixture, task *ShardReindexTaskGeneric) {
		require.NoError(t, task.OnAfterLsmInit(ctx, f.shard))
	}

	iterated := unflipped(MigrationStateIterated)
	iterated.driveTo = func(t *testing.T, ctx context.Context, f *rangeableRestartFixture, task *ShardReindexTaskGeneric) {
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, f.shard))
	}

	merged := unflipped(MigrationStateMerged)
	merged.driveTo = func(t *testing.T, ctx context.Context, f *rangeableRestartFixture, task *ShardReindexTaskGeneric) {
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, f.shard))
		require.NoError(t, task.RunPrepareOnShard(ctx, f.shard))
	}

	return []rangeableRestartCase{
		iterating,
		iterated,
		merged,
		{
			// The flip is durable and the unit reported complete, so no task
			// runs again. The load alone owes the promotion.
			interruptAt: MigrationStateSwapped,
			driveTo: func(t *testing.T, ctx context.Context, f *rangeableRestartFixture, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunOnShard(ctx, f.shard))
			},
			afterRestart: MigrationStatePromoted,
			indexLive:    true,
			relaunches:   false,
			settledGone:  true,
		},
		{
			// Reached by letting a load promote, so the state under test is
			// one reconciliation wrote rather than one this test composed.
			interruptAt: MigrationStatePromoted,
			driveTo: func(t *testing.T, ctx context.Context, f *rangeableRestartFixture, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunOnShard(ctx, f.shard))
				f.restart(t, ctx)
			},
			afterRestartGone: true,
			indexLive:        true,
			relaunches:       false,
			settledGone:      true,
		},
	}
}

// TestRecoveryConvergence_FilterableToRangeable_FromEachState interrupts a
// range-filter migration in each state its record can hold, restarts the shard,
// and requires the index to end up bit-equal to a clean run.
//
// The two failures this is built to catch are opposite: a restart that reports
// the migration finished over an index nothing rebuilt, and a restart that
// abandons a rebuild that was already complete.
func TestRecoveryConvergence_FilterableToRangeable_FromEachState(t *testing.T) {
	baseline := rangeableRestartBaseline(t)
	require.Len(t, baseline, filterableToRangeableNumDistinctValues,
		"a baseline that does not cover every value cannot anchor a convergence assertion")

	for _, tc := range rangeableRestartCases() {
		t.Run(string(tc.interruptAt), func(t *testing.T) {
			ctx := testCtx()
			f := newRangeableRestartFixture(t, ctx, "RangeableRestart"+string(tc.interruptAt))

			task, _ := f.newTask(t)
			tc.driveTo(t, ctx, f, task)

			state, present := f.recordState(t, task)
			require.True(t, present, "the drive must leave a record")
			require.Equal(t, tc.interruptAt, state,
				"the drive landed somewhere other than the state this row interrupts")

			f.restart(t, ctx)

			state, present = f.recordState(t, task)
			if tc.afterRestartGone {
				assert.False(t, present, "the load must retire the record it settled")
			} else {
				require.True(t, present, "the load must not drop a record it has not settled")
				assert.Equal(t, tc.afterRestart, state, "the state one load leaves behind")
			}

			// Which way this goes is the whole difference between the two halves
			// of the matrix. Below the flip a live index would be one a load
			// opened over a rebuild that never finished; at or above it, a load
			// that leaves no index has lost the rebuild.
			if tc.indexLive {
				assert.NotNil(t, f.rangeableBucket(),
					"the load owes the promotion, so the index must be serving after it")
			} else {
				assert.Nil(t, f.rangeableBucket(),
					"a migration whose flip is not decided must not leave its index live")
			}

			if tc.relaunches {
				relaunched, completed := f.newTask(t)
				require.NoError(t, relaunched.RunOnShard(ctx, f.shard),
					"the relaunched unit must run to completion")

				state, present = f.recordState(t, relaunched)
				require.True(t, present)
				assert.Equal(t, MigrationStateSwapped, state,
					"a relaunch must reach the flip rather than report the unit done without it")
				assert.True(t, completed(),
					"a relaunch must run the completion hook that commits the schema")
			}

			f.restart(t, ctx)

			state, present = f.recordState(t, task)
			if tc.settledGone {
				assert.False(t, present,
					"once the promotion is durable and the schema shows it, the record is retired")
			} else {
				require.True(t, present)
				assert.Equal(t, tc.settled, state, "the settled state")
			}

			bucket := f.rangeableBucket()
			require.NotNil(t, bucket,
				"the migrated property must be serving from its canonical bucket")
			require.Equal(t, lsmkv.StrategyRoaringSetRange, bucket.Strategy())

			// Compared per value so a failure names the posting list that
			// diverged instead of dumping the whole index.
			got := filterableToRangeableFingerprint(t, bucket)
			assert.Equal(t, len(baseline), len(got), "value count diverges from a clean run")
			for value, wantIDs := range baseline {
				gotIDs, ok := got[value]
				if !ok {
					assert.Failf(t, "missing value",
						"value %d is in a clean run but not after recovery", value)
					continue
				}
				assert.Equalf(t, wantIDs, gotIDs,
					"value %d holds a different set of objects than a clean run\n  clean (%d): %v\n  got   (%d): %v",
					value, len(wantIDs), wantIDs, len(gotIDs), gotIDs)
			}
		})
	}
}

// rangeableRestartBaseline is one uninterrupted migration followed by the load
// that promotes it, so the rows compare against an index reached the same way
// theirs is.
//
// The seeded cardinality is asserted here rather than only compared to. A
// baseline taken from the same build as the rows would otherwise absorb a
// defect they share, and every row would agree on an empty index.
func rangeableRestartBaseline(t *testing.T) map[uint64][]uint64 {
	t.Helper()
	ctx := testCtx()
	f := newRangeableRestartFixture(t, ctx, "RangeableRestartClean")

	task, completed := f.newTask(t)
	require.NoError(t, task.RunOnShard(ctx, f.shard))
	require.True(t, completed(), "a clean run must complete the migration")

	f.restart(t, ctx)

	bucket := f.rangeableBucket()
	require.NotNil(t, bucket, "a clean run must leave the rangeable bucket serving")

	fingerprint := filterableToRangeableFingerprint(t, bucket)
	require.Len(t, fingerprint, filterableToRangeableNumDistinctValues,
		"a clean run must index every value the fixture writes")
	perValue := rangeableRestartObjects / filterableToRangeableNumDistinctValues
	for value, ids := range fingerprint {
		require.Lenf(t, ids, perValue,
			"a clean run must index all %d objects carrying value %d, got %d",
			perValue, value, len(ids))
	}
	return fingerprint
}
