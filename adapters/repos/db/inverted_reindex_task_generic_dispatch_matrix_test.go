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
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Full [ShardReindexTaskGeneric.RunSwapOnShard] dispatch matrix: 8 strategies
// × every recorded state the dispatch resumes from.
//
// Extends [TestRunSwapOnShard_RecordAwareDispatch], which only covers
// MapToBlockmax. weaviate/0-weaviate-issues#214 Phase 7c is the dispatch fix
// being pinned; without it a rolling restart past the prepend would call the
// full prep+swap on a reindex bucket that is no longer there, flip the
// cluster-wide task to FAILED, and leave the already-swapped replicas inverted
// against the schema.
//
// Promoted has no row: the dispatch branches on PointerSwapped, which Promoted
// answers exactly as Swapped does, so a Promoted cell would repeat the Swapped
// one. It is also only ever produced by a load, never in-process.

// dispatchMatrixStates is the canonical iteration order, so the failure output
// reads left-to-right along the state machine.
var dispatchMatrixStates = []MigrationState{
	MigrationStateIterating,
	MigrationStateIterated,
	MigrationStateMerged,
	MigrationStateSwapped,
}

// dispatchMatrixStrategyCase describes one row in the strategy axis. The
// closures cover everything that varies by strategy: class fixture
// construction (some strategies need IndexFilterable=false, others need
// UsingBlockMaxWAND=true), task construction (different wrapper structs),
// the strategy-specific bucket name to fingerprint, and the fingerprint
// flavor (Inverted vs RoaringSet — RoaringSetRange isn't covered here
// because the existing FilterableToRangeable test uses a different
// `map[uint64][]uint64` shape; we normalize to `map[string][]uint64` by
// stringifying the lex key for matrix-wide assertion uniformity).
type dispatchMatrixStrategyCase struct {
	strategyName string
	// path indicates whether the strategy uses the trio (semantic) or
	// inline (non-semantic) drive-to-state primitives. Affects how each cell
	// is reached.
	path dispatchMatrixPath
	// buildClass returns the class fixture this strategy operates on
	// (and the property name to migrate — same for every cell).
	buildClass func(className string) (*models.Class, string)
	// buildTask returns a fresh task instance ready for a clean
	// migration. Each cell builds a new shard + task; the task is the
	// same instance used for both driveToState and RunSwapOnShard
	// (mirroring the production "cached task" preservation rule).
	buildTask func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric
	// fingerprintBucketName returns the canonical bucket name whose
	// post-migration content we compare against the baseline. For
	// EnableSearchable / RebuildSearchable / SearchableRetokenize this
	// is the searchable bucket; for the rest it's the filterable bucket.
	fingerprintBucketName func(propName string) string
	// fingerprint reads the named bucket and returns a deterministic
	// (term → sortedDocIDs) snapshot.
	fingerprint func(t *testing.T, shard *Shard, bucketName string) map[string][]uint64
}

// dispatchMatrixPath distinguishes the trio (semantic) drive primitives
// from the inline (non-semantic) ones. Inline strategies don't expose
// RunPrepareOnShard / RunSwapOnShard as their normal production
// invocation route — they're driven inline by OnAfterLsmInitAsync — but
// the trio methods are still well-defined and callable. The dispatch
// matrix uses the production-natural primitives for each path: trio
// methods for semantic strategies, OnAfterLsmInit+loop for inline.
type dispatchMatrixPath int

const (
	dispatchMatrixPathInline dispatchMatrixPath = iota // OnAfterLsmInit + async loop
	dispatchMatrixPathTrio                             // RunReindexOnlyOnShard + RunPrepareOnShard
)

// dispatchMatrixStrategyCases enumerates all 8 strategy structs.
func dispatchMatrixStrategyCases() []dispatchMatrixStrategyCase {
	return []dispatchMatrixStrategyCase{
		{
			strategyName: "MapToBlockmax",
			path:         dispatchMatrixPathInline,
			buildClass: func(className string) (*models.Class, string) {
				return newTestClassWithProps(className, []string{"title"}), "title"
			},
			buildTask: func(t *testing.T, idx *Index, _, _ string) *ShardReindexTaskGeneric {
				strategy := &testMigrationStrategy{
					MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1},
				}
				return newTestTask(idx.logger, strategy)
			},
			fingerprintBucketName: helpers.BucketSearchableFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return fingerprintInvertedBucket(t, shard.store.Bucket(name))
			},
		},
		{
			strategyName: "RebuildSearchable",
			path:         dispatchMatrixPathTrio,
			buildClass: func(className string) (*models.Class, string) {
				return newRebuildSearchableTestClass(className, []string{"title"}), "title"
			},
			buildTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newRebuildSearchableTask(t, idx, className, propName)
				return task
			},
			fingerprintBucketName: helpers.BucketSearchableFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return fingerprintInvertedBucket(t, shard.store.Bucket(name))
			},
		},
		{
			strategyName: "RoaringSetRefresh",
			path:         dispatchMatrixPathInline,
			buildClass: func(className string) (*models.Class, string) {
				return newTestClassWithProps(className, []string{"title"}), "title"
			},
			buildTask: func(t *testing.T, idx *Index, _, _ string) *ShardReindexTaskGeneric {
				task, _ := newRoaringSetRefreshTask(t, idx)
				return task
			},
			fingerprintBucketName: helpers.BucketFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return fingerprintRoaringSetBucket(t, shard.store.Bucket(name))
			},
		},
		{
			strategyName: "FilterableToRangeable",
			path:         dispatchMatrixPathInline,
			buildClass: func(className string) (*models.Class, string) {
				return newFilterableToRangeableTestClass(className), filterableToRangeablePropName
			},
			buildTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newFilterableToRangeableTask(t, idx, className, propName)
				return task
			},
			// FilterableToRangeable's target bucket is the rangeable
			// (RoaringSetRange) bucket, which uses a different cursor
			// model. We normalize to map[string][]uint64 by encoding the
			// 8-byte lex key as a fixed-width hex string so the matrix
			// assertion path is uniform with the other strategies. The
			// fingerprintBucketName/fingerprint pair below wires that.
			fingerprintBucketName: helpers.BucketRangeableFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return dispatchMatrixRangeableFingerprintAsString(t,
					shard.store.Bucket(name))
			},
		},
		{
			strategyName: "EnableFilterable",
			path:         dispatchMatrixPathTrio,
			buildClass: func(className string) (*models.Class, string) {
				return newEnableFilterableTestClass(className, "title"), "title"
			},
			buildTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, propName)
				return task
			},
			fingerprintBucketName: helpers.BucketFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return fingerprintRoaringSetBucket(t, shard.store.Bucket(name))
			},
		},
		{
			strategyName: "EnableSearchable",
			path:         dispatchMatrixPathTrio,
			buildClass: func(className string) (*models.Class, string) {
				return newEnableSearchableTestClass(className, []string{"title"}), "title"
			},
			buildTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, propName,
					models.PropertyTokenizationWord)
				return task
			},
			fingerprintBucketName: helpers.BucketSearchableFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return fingerprintInvertedBucket(t, shard.store.Bucket(name))
			},
		},
		{
			strategyName: "FilterableRetokenize",
			path:         dispatchMatrixPathTrio,
			buildClass: func(className string) (*models.Class, string) {
				return newTestClassWithProps(className, []string{"title"}), "title"
			},
			buildTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newFilterableRetokenizeTask(t, idx, className, propName,
					models.PropertyTokenizationField)
				return task
			},
			fingerprintBucketName: helpers.BucketFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return fingerprintRoaringSetBucket(t, shard.store.Bucket(name))
			},
		},
		{
			strategyName: "SearchableRetokenize",
			path:         dispatchMatrixPathTrio,
			buildClass: func(className string) (*models.Class, string) {
				return newTestClassWithProps(className, []string{"title"}), "title"
			},
			buildTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				// SearchableRetokenize needs to know the source bucket
				// strategy (MapCollection here, given UsingBlockMaxWAND=false
				// in newTestClassWithProps). Resolve it from the live shard
				// before constructing the task.
				task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
					models.PropertyTokenizationField,
					dispatchMatrixSearchableSourceStrategy(t, idx, className, propName),
				)
				return task
			},
			fingerprintBucketName: helpers.BucketSearchableFromPropNameLSM,
			fingerprint: func(t *testing.T, shard *Shard, name string) map[string][]uint64 {
				return fingerprintInvertedBucket(t, shard.store.Bucket(name))
			},
		},
	}
}

// dispatchMatrixSearchableSourceStrategy looks up the searchable bucket's
// strategy on a freshly-built class. SearchableRetokenize needs it at
// construction time. Captured in a helper to keep the table init readable.
func dispatchMatrixSearchableSourceStrategy(t *testing.T, idx *Index, className, propName string) string {
	t.Helper()
	// Build a transient shard with the same class to look up the source
	// strategy. We use a dedicated short-lived shard so this helper
	// doesn't perturb the cell's shard state.
	ctx := testCtx()
	class := newTestClassWithProps(className+"__probe", []string{propName})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	return shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)).Strategy()
}

// dispatchMatrixRangeableFingerprintAsString wraps the existing rangeable
// (RoaringSetRange) fingerprint helper into the matrix-wide
// `map[string][]uint64` shape expected by the comparison loop. The
// rangeable helper natively returns `map[uint64][]uint64` keyed by the
// 8-byte big-endian lex key; we re-key it as a 16-character zero-padded
// hex string so the matrix assertion path doesn't need to fork by key
// type and the failure output is reversible to the original lex key.
func dispatchMatrixRangeableFingerprintAsString(t *testing.T, b *lsmkv.Bucket) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	if b == nil {
		return out
	}
	for key, ids := range filterableToRangeableFingerprint(t, b) {
		out[fmt.Sprintf("%016x", key)] = ids
	}
	return out
}

// dispatchMatrixDriveCell drives the test shard to the requested recorded
// state along each strategy's own route: inline strategies iterate through
// OnAfterLsmInitAsync, semantic ones through the trio. Prep has no inline
// entry point of its own, so both paths reach Merged through
// RunPrepareOnShard, which is well-defined for either.
func dispatchMatrixDriveCell(
	t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric,
	path dispatchMatrixPath, state MigrationState,
) {
	t.Helper()
	switch state {
	case MigrationStateIterating:
		// Armed, rebuild not started: the dispatch has to resume the
		// iteration itself before it can decide anything.
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	case MigrationStateIterated:
		dispatchMatrixDriveToIterated(t, ctx, shard, task, path)
	case MigrationStateMerged:
		dispatchMatrixDriveToIterated(t, ctx, shard, task, path)
		require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	case MigrationStateSwapped:
		dispatchMatrixDriveToSwapped(t, ctx, shard, task, path)
	default:
		t.Fatalf("dispatchMatrix: no drive for state %q", state)
	}
}

func dispatchMatrixDriveToIterated(
	t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric,
	path dispatchMatrixPath,
) {
	t.Helper()
	switch path {
	case dispatchMatrixPathTrio:
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	case dispatchMatrixPathInline:
		task.skipSwapOnFinish.Store(true)
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		for {
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err)
			if rerunAt.IsZero() {
				break
			}
		}
		// Release the flag: RunSwapOnShard re-issues prep + swap itself, and
		// skipSwapOnFinish on a swap-only call is undefined. We want the
		// dispatch to behave exactly as it does under OnGroupCompleted.
		task.skipSwapOnFinish.Store(false)
	}
}

func dispatchMatrixDriveToSwapped(
	t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric,
	path dispatchMatrixPath,
) {
	t.Helper()
	switch path {
	case dispatchMatrixPathTrio:
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
		require.NoError(t, task.RunPrepareOnShard(ctx, shard))
		require.NoError(t, task.RunSwapOnShard(ctx, shard))
	case dispatchMatrixPathInline:
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		for {
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err)
			if rerunAt.IsZero() {
				break
			}
		}
	}
}

// dispatchMatrixRecordOf reads what the migration actually left on the shard,
// so a broken drive cannot let a downstream pass mask a missed setup.
func dispatchMatrixRecordOf(t *testing.T, shard *Shard, task *ShardReindexTaskGeneric) MigrationRecord {
	t.Helper()
	rec, ok := shard.migrationRecords.Get(task.migrationRecordKey())
	require.True(t, ok, "the migration should have a record on this shard")
	return rec
}

// dispatchMatrixComputeBaseline computes the post-clean-migration
// fingerprint on a throw-away shard for this strategy. Each strategy row
// caches the baseline once and reuses it across its cells: every cell's
// post-RunSwapOnShard fingerprint must match it.
func dispatchMatrixComputeBaseline(
	t *testing.T, sc dispatchMatrixStrategyCase, numObjects int,
) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "DispatchMatrixBaseline_" + sc.strategyName + "_" + uuid.NewString()[:6]
	class, propName := sc.buildClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	dispatchMatrixSeedObjects(t, ctx, shard, sc, className, numObjects)

	task := sc.buildTask(t, idx, className, propName)
	dispatchMatrixDriveToSwapped(t, ctx, shard, task, sc.path)

	// Swapped rather than Promoted: the flip is durable, and the
	// staged-to-canonical rename is deliberately left to the next load.
	rec := dispatchMatrixRecordOf(t, shard, task)
	require.Equal(t, MigrationStateSwapped, rec.State())
	require.Equal(t, []string{propName}, rec.(MigrationRecordSwapped).Flipped())

	return sc.fingerprint(t, shard, sc.fingerprintBucketName(propName))
}

// dispatchMatrixSeedObjects writes the per-strategy seed objects. The
// rangeable strategy needs numeric props; everything else uses the
// text-based 3-token-window generator. Centralized so the cell setup is
// a single line.
func dispatchMatrixSeedObjects(
	t *testing.T, ctx context.Context, shard *Shard,
	sc dispatchMatrixStrategyCase, className string, numObjects int,
) {
	t.Helper()
	if sc.strategyName == "FilterableToRangeable" {
		for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
			require.NoError(t, shard.PutObject(ctx, obj))
		}
		return
	}
	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
}

// TestRunSwapOnShard_DispatchMatrix: full state × strategy cross product. Each
// cell drives to the target state via the strategy's primitives, verifies the
// setup landed, calls RunSwapOnShard, and asserts the target bucket
// fingerprint matches the baseline.
func TestRunSwapOnShard_DispatchMatrix(t *testing.T) {
	const numObjects = 10

	strategies := dispatchMatrixStrategyCases()

	for _, sc := range strategies {
		sc := sc // capture for closure
		t.Run(sc.strategyName, func(t *testing.T) {
			baseline := dispatchMatrixComputeBaseline(t, sc, numObjects)
			require.NotEmptyf(t, baseline,
				"baseline fingerprint for %s must be non-empty (a strategy whose clean migration produces no terms can't anchor convergence assertions)",
				sc.strategyName)

			for _, state := range dispatchMatrixStates {
				state := state
				t.Run(string(state), func(t *testing.T) {
					dispatchMatrixRunCell(t, sc, state, numObjects, baseline)
				})
			}
		})
	}
}

// dispatchMatrixRunCell runs one (strategy, state) cell.
func dispatchMatrixRunCell(
	t *testing.T,
	sc dispatchMatrixStrategyCase,
	state MigrationState,
	numObjects int,
	baseline map[string][]uint64,
) {
	ctx := testCtx()
	className := "DispatchMatrixCell_" + sc.strategyName + "_" + string(state) + "_" + uuid.NewString()[:6]
	class, propName := sc.buildClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	dispatchMatrixSeedObjects(t, ctx, shard, sc, className, numObjects)

	task := sc.buildTask(t, idx, className, propName)

	dispatchMatrixDriveCell(t, ctx, shard, task, sc.path, state)

	require.Equalf(t, state, dispatchMatrixRecordOf(t, shard, task).State(),
		"the drive landed somewhere other than the state this cell dispatches from (strategy=%s)",
		sc.strategyName)

	// Call RunSwapOnShard — the dispatch under test.
	require.NoErrorf(t, task.RunSwapOnShard(ctx, shard),
		"RunSwapOnShard should succeed for (strategy=%s, state=%s)",
		sc.strategyName, state)

	require.Equalf(t, MigrationStateSwapped, dispatchMatrixRecordOf(t, shard, task).State(),
		"every dispatch branch must leave the flip durable and the promotion to the next load (strategy=%s state=%s)",
		sc.strategyName, state)

	// Fingerprint convergence: every term in the baseline must appear
	// in the post-dispatch bucket with the same sorted docID list. We
	// don't require strict equality of map length here (some strategies'
	// post-dispatch state may carry extra metadata terms invisible to
	// the clean baseline path), but every baseline term MUST be present
	// and identical.
	postBucketName := sc.fingerprintBucketName(propName)
	gotFP := sc.fingerprint(t, shard, postBucketName)
	for term, expectedIDs := range baseline {
		gotIDs, ok := gotFP[term]
		if !ok {
			assert.Failf(t, "missing term post-RunSwapOnShard",
				"term %q present in baseline but missing post-dispatch (strategy=%s state=%s)",
				term, sc.strategyName, state)
			continue
		}
		assert.Equalf(t, expectedIDs, gotIDs,
			"term %q post-dispatch doc-id list diverges from baseline (strategy=%s state=%s)",
			term, sc.strategyName, state)
	}
}
