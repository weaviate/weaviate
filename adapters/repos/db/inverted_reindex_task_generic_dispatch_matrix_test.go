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

// Full sentinel-aware [ShardReindexTaskGeneric.RunSwapOnShard] dispatch
// matrix: 8 strategies × 5 sentinels = 40 cells (32 executed, 8 skipped).
//
// Extends [TestRunSwapOnShard_SentinelAwareDispatch] which only covers
// MapToBlockmax at IsTidied / IsSwapped. weaviate/0-weaviate-issues#214
// Phase 7c is the dispatch fix being pinned; without it a rolling
// restart past markPrepended() would call runtimeSwap on a missing
// reindex bucket, flip the cluster-wide task to FAILED, and leave the
// already-swapped replicas inverted against the schema.
//
// IsPrepended cells are skipped: that branch's recoverRuntimeSwapBuckets
// renames the live mmap'd main bucket dir, which corrupts the segment
// registry in-process. Production reaches it only post-restart; the
// recovery-convergence matrices in this directory cover that path.

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

// dispatchMatrixStrategyCases enumerates all 8 strategy structs.
func dispatchMatrixStrategyCases() []dispatchMatrixStrategyCase {
	return []dispatchMatrixStrategyCase{
		{
			strategyName: "MapToBlockmax",
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
// strategy on a freshly-built class. SearchableRetokenize requires this
// at construction time so it can stamp the right BackupStrategy on the
// tracker. Captured in a helper to keep the table init readable.
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

// dispatchMatrixDriveCell drives the test shard to the requested sentinel
// state, skipping the one cell that is not safely reachable in-process.
func dispatchMatrixDriveCell(
	t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric,
	sentinel reindexSentinelState,
) {
	t.Helper()
	if sentinel == sentinelStatePrepended {
		// recoverRuntimeSwapBuckets renames the live main bucket dir
		// while the in-memory store still mmaps its segments; that
		// corrupts the segment registry and any subsequent path-based
		// resolve. Production never reaches this dispatch branch in
		// the same process — it's only entered post-restart on a node
		// where FinalizeCompletedMigrations did not already advance
		// the sentinel. See the file godoc above.
		t.Skip("IsPrepended dispatch branch requires post-restart in-memory state; not safely reachable in a same-process unit test (recoverRuntimeSwapBuckets renames live mmap'd bucket dirs). Convergence matrices cover the post-restart path.")
	}
	driveToSentinelState(t, ctx, shard, task, sentinel)
}

// dispatchMatrixComputeBaseline computes the post-clean-migration
// fingerprint on a throw-away shard for this strategy. Each strategy row
// caches the baseline once and reuses it across the five sentinel cells
// — every cell's post-RunSwapOnShard fingerprint must match it.
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
	driveToSentinelState(t, ctx, shard, task, sentinelStateTidied)

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

// TestRunSwapOnShard_DispatchMatrix: full sentinel × strategy cross
// product. Each cell drives to the target sentinel via the strategy's
// primitives, verifies the setup landed, calls RunSwapOnShard, and
// asserts the target bucket fingerprint matches the baseline. See
// file-level godoc for the IsPrepended skip rationale.
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

			for _, sentinel := range allReindexSentinelStates {
				sentinel := sentinel
				t.Run(string(sentinel), func(t *testing.T) {
					dispatchMatrixRunCell(t, sc, sentinel, numObjects, baseline)
				})
			}
		})
	}
}

// dispatchMatrixRunCell runs one (strategy, sentinel) cell.
func dispatchMatrixRunCell(
	t *testing.T,
	sc dispatchMatrixStrategyCase,
	sentinel reindexSentinelState,
	numObjects int,
	baseline map[string][]uint64,
) {
	ctx := testCtx()
	className := "DispatchMatrixCell_" + sc.strategyName + "_" + string(sentinel) + "_" + uuid.NewString()[:6]
	class, propName := sc.buildClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	dispatchMatrixSeedObjects(t, ctx, shard, sc, className, numObjects)

	task := sc.buildTask(t, idx, className, propName)

	// driveToState may call t.Skip for unreachable cells; if so, the
	// subtest is recorded as skipped (not failed). The defer-Shutdown
	// above is still honored on the skip path.
	dispatchMatrixDriveCell(t, ctx, shard, task, sentinel)

	// Verify the drive halted at the intended sentinel snapshot.
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	want := expectedSentinelsAt(sentinel)
	got := readReindexSentinels(rt)
	for name, w := range want {
		assert.Equalf(t, w, got[name],
			"pre-RunSwapOnShard sentinel %q (strategy=%s state=%s): want=%v got=%v full=%v",
			name, sc.strategyName, sentinel, w, got[name], got)
	}

	// Call RunSwapOnShard — the dispatch under test.
	require.NoError(t, task.RunSwapOnShard(ctx, shard),
		"RunSwapOnShard should succeed for (strategy=%s, state=%s)",
		sc.strategyName, sentinel)

	// Post-call: every sentinel should be set (terminal state).
	rtPost, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	postGot := readReindexSentinels(rtPost)
	postWant := map[string]bool{
		"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": true,
	}
	for name, w := range postWant {
		assert.Equalf(t, w, postGot[name],
			"post-RunSwapOnShard sentinel %q (strategy=%s state=%s): want=%v got=%v full=%v",
			name, sc.strategyName, sentinel, w, postGot[name], postGot)
	}

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
				term, sc.strategyName, sentinel)
			continue
		}
		assert.Equalf(t, expectedIDs, gotIDs,
			"term %q post-dispatch doc-id list diverges from baseline (strategy=%s state=%s)",
			term, sc.strategyName, sentinel)
	}
}
