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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// simulateProcessRestartBucketCleanup releases every bucket-dir path
// under shardLSMPath from the GlobalBucketRegistry. This mirrors what
// the OS does at real process restart — the next bucket creation must
// be able to reclaim those paths without "bucket already registered".
//
// Required when the prior in-process flow left orphan Bucket objects
// not in any Store's bucketsByName map (e.g., the OLD main buckets
// held only by the runtimeSwap-local `oldMainBuckets` map after a
// panic between Phase 2a and Phase 2b unwound the stack frame). The
// graceful Shard.Shutdown only releases buckets it can reach through
// the Store; orphans leak their registry entries until process death.
func simulateProcessRestartBucketCleanup(t *testing.T, shardLSMPath string) {
	t.Helper()
	err := filepath.WalkDir(shardLSMPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil // tolerate transient walk errors; we're best-effort cleaning
		}
		if d.IsDir() {
			// Remove is idempotent; safe to call on paths that aren't registered.
			lsmkv.GlobalBucketRegistry.Remove(path)
		}
		return nil
	})
	require.NoError(t, err, "walk shard LSM path for registry cleanup")
}

// TestRecoveryConvergence_MultiProp_FromEachState pins recovery
// convergence for MapToBlockmax with MULTIPLE properties migrating in
// lock-step. The single-prop tests in
// inverted_reindex_recovery_convergence_test.go showed convergence at
// the per-shard sentinel boundaries; this test additionally pins that
// the internal per-prop loops in runtimePrepare / runtimeSwap don't
// drop or swap prop content across properties when a crash interrupts
// the per-shard recovery.
func TestRecoveryConvergence_MultiProp_FromEachState(t *testing.T) {
	const numObjects = 25
	propNames := []string{"title", "subtitle", "description"}

	baseline := computeMultiPropBaseline(t, propNames, numObjects)
	for _, propName := range propNames {
		require.NotEmptyf(t, baseline[propName],
			"multi-prop baseline must have non-empty fingerprint for prop %q", propName)
	}

	cases := []recoveryConvergenceCase{
		{
			name: "MultiProp_IsReindexed_via_skipSwapOnFinish",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				task.skipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "MultiProp_IsMerged_via_runtimePrepare",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				task.skipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				props, err := task.readPropsToReindex(rt)
				require.NoError(t, err)
				require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, rt, props))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "MultiProp_IsTidied_full_migration",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MultiPropCase_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, propNames)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)
			tc.driveToState(t, ctx, shard, task)

			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			for name, want := range tc.expectedPostStateSentinels {
				var got bool
				switch name {
				case "reindexed":
					got = rt.IsReindexed()
				case "prepended":
					got = rt.IsPrepended()
				case "merged":
					got = rt.IsMerged()
				case "swapped":
					got = rt.IsSwapped()
				case "tidied":
					got = rt.IsTidied()
				}
				assert.Equalf(t, want, got, "multi-prop sentinel %q (case %q)", name, tc.name)
			}

			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task2 := newTestTask(idx.logger, strategy2)
			task2.skipSwapOnFinish.Store(false)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "multi-prop shard re-init (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoError(t, err, "multi-prop recovery OnAfterLsmInitAsync (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			// Per-prop convergence check — every prop must converge.
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard2.store.Bucket(bucketName)
				require.NotNilf(t, bucket, "multi-prop bucket %q must exist (case %q)", propName, tc.name)
				require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
					"multi-prop bucket %q must be StrategyInverted (case %q)", propName, tc.name)

				got := fingerprintInvertedBucket(t, bucket)
				expected := baseline[propName]

				assert.Equalf(t, len(expected), len(got),
					"multi-prop term count for %q diverges (case %q)", propName, tc.name)
				for term, expectedIDs := range expected {
					gotIDs, ok := got[term]
					if !ok {
						assert.Failf(t, "multi-prop missing term",
							"term %q on prop %q present in baseline but missing post-recovery (case %q)",
							term, propName, tc.name)
						continue
					}
					assert.Equalf(t, expectedIDs, gotIDs,
						"multi-prop term %q on prop %q diverges (case %q)\n  baseline (%d): %v\n  got      (%d): %v",
						term, propName, tc.name, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
				}
			}
		})
	}
}

// computeMultiPropBaseline returns per-property fingerprints after a
// clean multi-prop migration. Used by the multi-prop convergence cases.
func computeMultiPropBaseline(t *testing.T, propNames []string, numObjects int) map[string]map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "MultiPropBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, strategy.migrationCompleted)

	out := make(map[string]map[string][]uint64, len(propNames))
	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		out[propName] = fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
	}
	return out
}

// TestRecoveryConvergence_MidPropSwap_Loop pins recovery convergence
// when runtimeSwap's Phase 2a per-prop SwapBucketPointer loop is
// interrupted after K props out of N. On disk: K props with
// markSwappedProp written + their old buckets renamed; the rest with
// main pointing at old. Recovery must complete the remaining swaps
// and converge on every prop's bucket fingerprint.
//
// Mechanism: hook panic+recover. Production exposes
// testHookPostPropSwap (fires after each per-prop swap); production
// leaves it nil. The test wires the hook to panic after K=2 props
// have been swapped. The runtime call panics, control returns to the
// test's deferred recover, on-disk state has 2 of 4 markSwappedProp
// sentinels and 2 renamed buckets. Restart drives recovery.
func TestRecoveryConvergence_MidPropSwap_Loop(t *testing.T) {
	const numObjects = 25
	propNames := []string{"title", "subtitle", "description", "keywords"}
	const haltAfter = 2 // after props 0 and 1 swapped, panic before prop 2

	baseline := computeMultiPropBaseline(t, propNames, numObjects)

	ctx := testCtx()
	className := "MidPropSwap_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// Phase 1: drive iteration to markReindexed, then runtimePrepare
	// to markMerged. Now runtimeSwap's Phase 2a is the next step.
	task.skipSwapOnFinish.Store(true)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	props, err := task.readPropsToReindex(rt)
	require.NoError(t, err)
	require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, rt, props))

	// Phase 2: install the halt hook. Panic after `haltAfter` props
	// have been observed. The defer-recover below catches the panic
	// so the test continues.
	//
	// Sentinel value carries a fixed prefix that the recover() handler
	// matches: this lets the test discriminate between THE expected
	// hook panic vs. an unrelated panic from runtimeSwap (which would
	// otherwise be silently swallowed and mask a real bug).
	const haltPanicPrefix = "mid-loop halt: simulated crash"
	task.testHookPostPropSwap = func(propIdx int) {
		if propIdx == haltAfter-1 {
			panic(haltPanicPrefix + " after prop " + uuid.NewString()[:4])
		}
	}

	var (
		panicked     bool
		panicValue   interface{}
		swapReturned bool
		swapErr      error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicValue = r
			}
		}()
		swapErr = task.runtimeSwap(ctx, task.logger, shard, rt, props)
		swapReturned = true
	}()

	// The expected path is: hook panics → runtimeSwap unwinds →
	// recover() above catches it. If runtimeSwap returned (with or
	// without error) the hook didn't fire — that's a test-harness
	// failure, NOT a production bug, and silently passing here would
	// mask the convergence-from-mid-Phase-2a invariant the test is
	// supposed to pin.
	require.Falsef(t, swapReturned,
		"runtimeSwap returned without panicking (err=%v); hook did not fire — test harness invalid",
		swapErr)
	require.Truef(t, panicked,
		"expected panic from testHookPostPropSwap; got swapErr=%v", swapErr)
	panicStr, ok := panicValue.(string)
	require.Truef(t, ok && strings.HasPrefix(panicStr, haltPanicPrefix),
		"recovered panic was not from the hook (want prefix %q; got %T %v)",
		haltPanicPrefix, panicValue, panicValue)
	t.Logf("recovered expected hook panic: %v", panicValue)

	// Verify partial state: at least `haltAfter` markSwappedProp
	// sentinels written.
	swappedCount := 0
	for _, p := range props {
		if rt.IsSwappedProp(p) {
			swappedCount++
		}
	}
	assert.GreaterOrEqualf(t, swappedCount, haltAfter,
		"after Phase 2a halt, expected ≥%d markSwappedProp sentinels (got %d)",
		haltAfter, swappedCount)
	assert.Lessf(t, swappedCount, len(propNames),
		"after Phase 2a halt, expected <%d markSwappedProp sentinels (got %d) — halt didn't fire",
		len(propNames), swappedCount)

	// Phase 3: restart, drive recovery.
	shardName := shard.Name()
	shardLSMPath := shard.pathLSM()
	require.NoError(t, shard.Shutdown(ctx))

	// The panic between Phase 2a and Phase 2b left K already-swapped
	// OLD main buckets orphaned (held only in runtimeSwap's stack-local
	// oldMainBuckets map; unrecoverable after stack unwind). Their
	// registry entries did not get released by the graceful Shutdown
	// because they were not in any Store's bucketsByName map. In
	// production this state is reached only via process death, where
	// the OS reclaims the registry. We simulate that explicitly so the
	// next initShard can register the canonical paths.
	simulateProcessRestartBucketCleanup(t, shardLSMPath)

	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	task2.skipSwapOnFinish.Store(false)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "mid-prop-swap shard re-init")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	for {
		rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
		require.NoError(t, err, "mid-prop-swap recovery OnAfterLsmInitAsync")
		if rerunAt.IsZero() {
			break
		}
	}

	// Phase 4: per-prop convergence — every prop must converge to baseline.
	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard2.store.Bucket(bucketName)
		require.NotNilf(t, bucket, "mid-prop-swap bucket %q must exist post-recovery", propName)
		require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
			"mid-prop-swap bucket %q must be StrategyInverted post-recovery", propName)

		got := fingerprintInvertedBucket(t, bucket)
		expected := baseline[propName]

		assert.Equalf(t, len(expected), len(got),
			"mid-prop-swap term count for %q diverges", propName)
		for term, expectedIDs := range expected {
			gotIDs, ok := got[term]
			if !ok {
				assert.Failf(t, "mid-prop-swap missing term",
					"term %q on prop %q present in baseline but missing post-recovery", term, propName)
				continue
			}
			assert.Equalf(t, expectedIDs, gotIDs,
				"mid-prop-swap term %q on prop %q diverges\n  baseline (%d): %v\n  got      (%d): %v",
				term, propName, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
		}
	}
}

// TestRecoveryConvergence_CrossReplica_DivergentFlushTiming pins the
// per-replica determinism invariant for the inverted-index migration.
//
// Two replicas with the SAME logical object set but DIFFERENT segment
// layouts (one big in-memory memtable vs. many small flushed segments,
// possibly in reverse insertion order) must produce IDENTICAL inverted
// index buckets post-migration. If they don't, the migration's output
// is dependent on the input segment layout — which is the leading
// hypothesis for the per-replica divergence observed in 0-weaviate-issues#240.
//
// This test is the direct unit-level counterpart to the e2e "did
// replicas A and B end up with the same data" observation. If this
// test passes, the e2e bug must come from a different source (e.g.,
// objects actually differing on the wire, or RAFT FSM apply order).
// If this test fails, the failure log is the reproducer for #240.
func TestRecoveryConvergence_CrossReplica_DivergentFlushTiming(t *testing.T) {
	const numObjects = 30
	propNames := []string{"title", "subtitle", "description"}

	// Build the canonical object set. Both replicas receive these
	// exact objects (same IDs, same values, same prop content) — only
	// the WRITE ORDER and FLUSH BOUNDARIES differ.
	className := "CrossReplicaSame_" + uuid.NewString()[:8]
	canonical := makeMultiPropConvergenceObjects(t, numObjects, className, propNames)

	// Replica A: insert in forward order, no per-batch flush (1 segment per prop)
	fpA := runCrossReplicaMigration(t, propNames, className, canonical, false, 0)

	// Replica B: insert in REVERSE order, flush every 3 objects (10 segments per prop)
	fpB := runCrossReplicaMigration(t, propNames, className, canonical, true, 3)

	// Invariant: both replicas migrated the same logical object set →
	// their post-migration inverted buckets must be byte-identical at
	// the (term → []docID) level.
	for _, propName := range propNames {
		a := fpA[propName]
		b := fpB[propName]
		assert.Equalf(t, len(a), len(b),
			"cross-replica: term count for prop %q diverges (replica A: %d, replica B: %d)",
			propName, len(a), len(b))
		for term, uuidsA := range a {
			uuidsB, ok := b[term]
			if !ok {
				assert.Failf(t, "cross-replica missing term",
					"term %q on prop %q present in replica A but missing on replica B", term, propName)
				continue
			}
			assert.Equalf(t, uuidsA, uuidsB,
				"cross-replica term %q on prop %q diverges\n  replica A (%d objects, 1 segment/prop): %v\n  replica B (%d objects, reverse+10 segments/prop): %v",
				term, propName, len(uuidsA), uuidsA, len(uuidsB), uuidsB)
		}
		// Symmetric check: every term in B is in A.
		for term := range b {
			if _, ok := a[term]; !ok {
				assert.Failf(t, "cross-replica extra term",
					"term %q on prop %q present in replica B but missing on replica A", term, propName)
			}
		}
	}
}

// TestRecoveryConvergence_CrossReplica_DivergentLayoutAndCrash pins
// the strongest cross-replica invariant: two replicas with DIFFERENT
// segment layouts AND DIFFERENT mid-migration crash points must both
// converge to the SAME per-term UUID set after recovery completes.
//
// This is the unit-level reproducer for the exact production
// pathology in weaviate/0-weaviate-issues#240: a rolling restart
// catches replica A and replica B at distinct points in the migration
// state machine. Production must guarantee both converge identically
// after recovery; if not, queries against the same logical content
// would return different result sets depending on which replica
// answered — the symptom observed in the e2e flakes.
//
// Failure log of this test == reproducer for the bug.
func TestRecoveryConvergence_CrossReplica_DivergentLayoutAndCrash(t *testing.T) {
	const numObjects = 30
	propNames := []string{"title", "subtitle", "description"}

	className := "CrossRepCrash_" + uuid.NewString()[:8]
	canonical := makeMultiPropConvergenceObjects(t, numObjects, className, propNames)

	// Replica A: forward order, no per-batch flush, crash at IsReindexed
	// (the earliest meaningful sentinel; recovery must re-run runtimePrepare
	// + runtimeSwap from scratch).
	fpA := runCrossReplicaMigrationWithCrash(t, propNames, className, canonical,
		false, 0, crashAtReindexed)

	// Replica B: reverse order, flush every 3 objects, crash at IsSwapped
	// (post-swap, pre-tidy; recovery's RunSwapOnShard IsSwapped branch
	// runs only the tidy + markTidied portion).
	fpB := runCrossReplicaMigrationWithCrash(t, propNames, className, canonical,
		true, 3, crashAtSwapped)

	// Invariant: both replicas have converged to a fully-completed
	// migration on the same logical object set. Per-term UUID sets MUST
	// match — same data → same posting lists, regardless of crash path
	// or segment layout.
	for _, propName := range propNames {
		a := fpA[propName]
		b := fpB[propName]
		assert.Equalf(t, len(a), len(b),
			"layout+crash divergence: term count for prop %q diverges (A: forward+IsReindexed=%d, B: reverse+IsSwapped=%d)",
			propName, len(a), len(b))
		for term, uuidsA := range a {
			uuidsB, ok := b[term]
			if !ok {
				assert.Failf(t, "layout+crash: missing term on B",
					"term %q on prop %q present in A but missing on B", term, propName)
				continue
			}
			assert.Equalf(t, uuidsA, uuidsB,
				"layout+crash: term %q on prop %q diverges\n  replica A (forward, IsReindexed crash): %v\n  replica B (reverse+flush, IsSwapped crash): %v",
				term, propName, uuidsA, uuidsB)
		}
		for term := range b {
			if _, ok := a[term]; !ok {
				assert.Failf(t, "layout+crash: extra term on B",
					"term %q on prop %q present in B but missing on A", term, propName)
			}
		}
	}
}

// crashSentinel chooses where during the migration the test simulates
// a crash. Each case stops driveToState at that on-disk state, then
// restarts the shard and lets recovery run to completion.
type crashSentinel int

const (
	crashAtReindexed crashSentinel = iota
	crashAtSwapped
)

// runCrossReplicaMigrationWithCrash runs the migration up to a crash
// sentinel, restarts the shard, runs recovery to completion, and
// returns the per-prop UUID fingerprint.
func runCrossReplicaMigrationWithCrash(t *testing.T, propNames []string, className string,
	canonical []*storobj.Object, reverse bool, flushEvery int, crash crashSentinel,
) map[string]map[string][]string {
	t.Helper()
	ctx := testCtx()
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer func() {
		if shard != nil {
			shard.Shutdown(ctx)
		}
	}()

	insertOrder := make([]*storobj.Object, len(canonical))
	copy(insertOrder, canonical)
	if reverse {
		for i, j := 0, len(insertOrder)-1; i < j; i, j = i+1, j-1 {
			insertOrder[i], insertOrder[j] = insertOrder[j], insertOrder[i]
		}
	}
	for i, obj := range insertOrder {
		require.NoError(t, shard.PutObject(ctx, obj))
		if flushEvery > 0 && (i+1)%flushEvery == 0 {
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard.store.Bucket(bucketName)
				if bucket != nil {
					require.NoError(t, bucket.FlushAndSwitch())
				}
			}
		}
	}

	// Drive migration to the crash sentinel.
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	switch crash {
	case crashAtReindexed:
		task.skipSwapOnFinish.Store(true)
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		for {
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err, "layout+crash A: drive to IsReindexed")
			if rerunAt.IsZero() {
				break
			}
		}
		rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
		require.True(t, rt.IsReindexed(), "layout+crash A: expected IsReindexed=true")
		require.False(t, rt.IsTidied(), "layout+crash A: expected IsTidied=false")
	case crashAtSwapped:
		// Run to completion, then synthetically remove tidied.mig to
		// simulate crash between markSwapped() and markTidied(). This
		// mirrors the IsSwapped_synthetic_tidied_sentinel_removed case
		// from FromEachState.
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		for {
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err, "layout+crash B: drive to completion")
			if rerunAt.IsZero() {
				break
			}
		}
		rt, err := task.newReindexTracker(shard.pathLSM())
		require.NoError(t, err)
		ftr := rt.(*fileReindexTracker)
		tidiedPath := filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied)
		require.NoError(t, os.Remove(tidiedPath), "layout+crash B: remove tidied.mig")
		require.True(t, rt.IsSwapped(), "layout+crash B: expected IsSwapped=true")
		require.False(t, rt.IsTidied(), "layout+crash B: expected IsTidied=false")
	}

	// Restart the shard.
	shardName := shard.Name()
	shardLSMPath := shard.pathLSM()
	require.NoError(t, shard.Shutdown(ctx))
	shard = nil // disable the defer's Shutdown
	simulateProcessRestartBucketCleanup(t, shardLSMPath)

	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	task2.skipSwapOnFinish.Store(false)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "layout+crash: shard re-init must succeed")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	for {
		rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
		require.NoError(t, err, "layout+crash: recovery loop")
		if rerunAt.IsZero() {
			break
		}
	}

	// Fingerprint each prop's post-recovery bucket, resolving to UUIDs.
	objectsBucket := shard2.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, objectsBucket, "layout+crash: objects bucket must exist post-recovery")

	out := make(map[string]map[string][]string, len(propNames))
	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard2.store.Bucket(bucketName)
		require.NotNilf(t, bucket, "layout+crash: bucket %q must exist post-recovery", propName)
		require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
			"layout+crash: bucket %q must be StrategyInverted post-recovery", propName)
		docIDFingerprint := fingerprintInvertedBucket(t, bucket)
		out[propName] = resolveDocIDFingerprintToUUIDs(t, idx.logger, objectsBucket, docIDFingerprint)
	}
	return out
}

// runCrossReplicaMigration builds a fresh shard, inserts canonical
// objects with the requested ordering and flush cadence, runs the
// full MapToBlockmax migration, and returns per-prop fingerprints
// keyed on the object UUID — NOT on the local docID, because docIDs
// are local-per-replica (assigned by insertion order on each shard).
// The cross-replica invariant is about object identity, not local handles.
func runCrossReplicaMigration(t *testing.T, propNames []string, className string,
	canonical []*storobj.Object, reverse bool, flushEvery int,
) map[string]map[string][]string {
	t.Helper()
	ctx := testCtx()
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	_ = idx

	// Insert objects, optionally reverse, flushing every flushEvery objects
	// to create multiple segments. Touch every prop's searchable bucket so
	// the flush has data to push to disk.
	insertOrder := make([]*storobj.Object, len(canonical))
	copy(insertOrder, canonical)
	if reverse {
		for i, j := 0, len(insertOrder)-1; i < j; i, j = i+1, j-1 {
			insertOrder[i], insertOrder[j] = insertOrder[j], insertOrder[i]
		}
	}
	for i, obj := range insertOrder {
		require.NoError(t, shard.PutObject(ctx, obj))
		if flushEvery > 0 && (i+1)%flushEvery == 0 {
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard.store.Bucket(bucketName)
				if bucket != nil {
					require.NoError(t, bucket.FlushAndSwitch(),
						"flush bucket %q (insertIdx %d, reverse=%v, flushEvery=%d)",
						propName, i, reverse, flushEvery)
				}
			}
		}
	}

	// Run the full migration pipeline.
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err, "cross-replica migration loop (reverse=%v)", reverse)
		if rerunAt.IsZero() {
			break
		}
	}

	// Fingerprint each prop's post-migration bucket, resolving local
	// docIDs to global object UUIDs so the comparison is replica-invariant.
	objectsBucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, objectsBucket, "cross-replica: objects bucket must exist")

	out := make(map[string]map[string][]string, len(propNames))
	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard.store.Bucket(bucketName)
		require.NotNilf(t, bucket, "cross-replica: bucket %q must exist post-migration (reverse=%v)", propName, reverse)
		require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
			"cross-replica: bucket %q must be StrategyInverted post-migration (reverse=%v)", propName, reverse)
		docIDFingerprint := fingerprintInvertedBucket(t, bucket)
		out[propName] = resolveDocIDFingerprintToUUIDs(t, idx.logger, objectsBucket, docIDFingerprint)
	}
	return out
}

// resolveDocIDFingerprintToUUIDs converts a `term → []docID` map into
// `term → []uuid` so cross-replica comparison is identity-based, not
// position-based. Returns deterministically sorted UUIDs per term.
//
// Uses ObjectsByDocIDWithEmpty and explicitly fails the test if any
// docID does not resolve to an object: a posting list that names a
// docID with no payload IS a corruption signal (or a write-path bug
// landing a docID in the index without persisting the object), and
// the convergence tests exist precisely to surface those — silently
// dropping nils would let two replicas BOTH miss the same docID and
// still compare equal.
func resolveDocIDFingerprintToUUIDs(t *testing.T, logger logrus.FieldLogger,
	objectsBucket *lsmkv.Bucket, docIDMap map[string][]uint64,
) map[string][]string {
	t.Helper()
	out := make(map[string][]string, len(docIDMap))
	for term, docIDs := range docIDMap {
		objs, err := storobj.ObjectsByDocIDWithEmpty(objectsBucket, docIDs, additional.Properties{}, nil, logger)
		require.NoErrorf(t, err, "resolve docIDs for term %q", term)
		require.Lenf(t, objs, len(docIDs),
			"ObjectsByDocIDWithEmpty must return one slot per input docID for term %q (input=%d, got=%d)",
			term, len(docIDs), len(objs))
		uuids := make([]string, 0, len(objs))
		for i, obj := range objs {
			require.NotNilf(t, obj,
				"docID %d on term %q has no payload — posting list references a missing object (corruption or write-path bug); see resolveDocIDFingerprintToUUIDs godoc",
				docIDs[i], term)
			uuids = append(uuids, string(obj.ID()))
		}
		sort.Strings(uuids)
		out[term] = uuids
	}
	return out
}

// makeMultiPropConvergenceObjects produces objects with values for
// every prop. Each prop gets a different 3-token slice of the
// dictionary so per-prop fingerprints are non-trivial and distinct —
// catches a bug where one prop's posting list bleeds into another.
func makeMultiPropConvergenceObjects(t *testing.T, n int, className string, propNames []string) []*storobj.Object {
	t.Helper()
	tokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		props := map[string]interface{}{}
		for j, propName := range propNames {
			a := tokens[(i+j*7)%len(tokens)]
			b := tokens[(i+j*7+1)%len(tokens)]
			c := tokens[(i+j*7+2)%len(tokens)]
			props[propName] = a + " " + b + " " + c
		}
		out[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: props,
			},
		}
	}
	return out
}
