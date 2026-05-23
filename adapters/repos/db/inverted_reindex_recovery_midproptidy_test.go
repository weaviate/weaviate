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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// midPropTidyPhase identifies which per-prop loop in the runtime
// swap/tidy code path the test halts inside.
type midPropTidyPhase int

const (
	midPropTidyPhaseSwap midPropTidyPhase = iota
	midPropTidyPhaseTidy
)

func (p midPropTidyPhase) String() string {
	switch p {
	case midPropTidyPhaseSwap:
		return "swap"
	case midPropTidyPhaseTidy:
		return "tidy"
	default:
		return "unknown"
	}
}

// midPropTidyCase is one cell of the (phase, haltAfter) matrix that
// TestRecoveryConvergence_MidPropSwapOrTidy_Loop iterates over. See
// the test godoc for the per-cell semantics.
type midPropTidyCase struct {
	phase     midPropTidyPhase
	haltAfter int
}

// midPropTidyHaltPanicPrefix is the sentinel string that every hook-
// driven panic carries so the recover() handler can reliably tell our
// expected halt apart from an unrelated panic that would otherwise be
// silently swallowed and mask a real bug.
const midPropTidyHaltPanicPrefix = "mid-prop-tidy halt: simulated crash"

// midPropTidyInstallSwapHook installs testHookPostPropSwap so that the
// hook panics after haltAfter props have been swapped+marked. A
// haltAfter of 0 leaves the hook a no-op (baseline / no-halt cell).
func midPropTidyInstallSwapHook(task *ShardReindexTaskGeneric, haltAfter int) {
	task.testHookPostPropSwap = func(propIdx int) {
		if haltAfter > 0 && propIdx == haltAfter-1 {
			panic(midPropTidyHaltPanicPrefix + " (phase=swap, after prop " + uuid.NewString()[:4] + ")")
		}
	}
}

// midPropTidyInstallTidyHook installs testHookPostPropTidy so that the
// hook panics after haltAfter props have been tidied. fireCount tracks
// how many times the hook ran across all goroutines (always incremented
// regardless of haltAfter so callers can assert hook wiring on the
// no-halt baseline cell). Concurrency MUST be set to 1 by the caller
// for the panic to fire in a deterministic propIdx order; without that,
// parallel tidy goroutines race for the hook count and the halt point
// becomes non-deterministic.
//
// Unlike the swap hook, panics here are RECOVERED by the per-goroutine
// deferFunc in the errgroup wrapper (see [entities/errors/error_group_
// wrapper.go]) — so subsequent tidy goroutines continue to launch under
// SetLimit(1) FIFO, and the only on-disk visible effect of the halt is
// that the aggregate markTidied() at the bottom of tidyBackupBuckets is
// never reached (eg.Wait() returns the wrapper's "panic occurred" error
// instead). See the docstring on the test below for what this means in
// terms of the recovery state the test then exercises.
func midPropTidyInstallTidyHook(task *ShardReindexTaskGeneric, haltAfter int, fireCount *atomic.Int64) {
	task.testHookPostPropTidy = func(propIdx int) {
		n := fireCount.Add(1)
		if haltAfter <= 0 {
			return
		}
		// Panic exactly once, after the haltAfter-th completion. Without
		// the once-only guard, every subsequent goroutine would also
		// trigger the wrapper's recovery path; the resulting error
		// message races are harmless but make assertion harder to read.
		if n == int64(haltAfter) {
			panic(midPropTidyHaltPanicPrefix + " (phase=tidy, after prop " + uuid.NewString()[:4] + ")")
		}
	}
}

// midPropTidyRunSwapWithRecover runs runtimeSwap inside a defer-recover
// frame, returning (panicked, panicValue, swapReturned, swapErr).
//
// Phase 2a runs as a sequential per-prop loop in the calling goroutine
// — a panic from testHookPostPropSwap propagates straight up the stack
// and the deferred recover() catches it.
func midPropTidyRunSwapWithRecover(ctx context.Context, task *ShardReindexTaskGeneric,
	shard *Shard, rt reindexTracker, props []string,
) (panicked bool, panicValue interface{}, swapReturned bool, swapErr error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicValue = r
		}
	}()
	swapErr = task.runtimeSwap(ctx, task.logger, shard, rt, props)
	swapReturned = true
	return
}

// midPropTidyRunTidyExpectingPanicError runs tidyBackupBuckets and
// returns the error it surfaces. When the testHookPostPropTidy hook
// panics inside one of the goroutines, the wrapper's deferFunc recovers
// and the error returned from eg.Wait() carries the substring "panic
// occurred" (see [entities/errors/error_group_wrapper.go] line ~92).
func midPropTidyRunTidyExpectingPanicError(ctx context.Context, task *ShardReindexTaskGeneric,
	shard *Shard, rt reindexTracker, props []string,
) error {
	return task.tidyBackupBuckets(ctx, task.logger, shard, rt, props)
}

// TestRecoveryConvergence_MidPropSwapOrTidy_Loop is a table-driven
// symmetric counterpart to TestRecoveryConvergence_MidPropSwap_Loop.
// It pins recovery convergence when EITHER of the per-prop loops in
// the swap/tidy code path is interrupted after K of N props.
//
// Matrix: (phase ∈ {swap, tidy}) × (haltAfter ∈ {0, 1, 2, 3}) on a
// 4-prop class. Each cell:
//
//  1. Drives the migration to the state immediately before the
//     phase-under-test (markMerged for swap, markSwapped for tidy).
//  2. Installs the phase's halt hook to panic after `haltAfter`
//     per-prop completions.
//  3. Runs the phase, expecting either a panic (swap; recovered in
//     the test frame) or a "panic occurred" error from the errgroup
//     wrapper (tidy; the per-goroutine deferFunc recovers and
//     surfaces it via eg.Wait).
//  4. Simulates a process restart and drives recovery to completion.
//  5. Asserts every prop's bucket fingerprint matches the baseline
//     produced by a clean (non-halted) migration.
//
// The haltAfter=0 cell is the no-halt baseline: the hook never fires
// (or in the tidy case, fires for every prop but never panics) and
// the cell exercises the full clean-completion path with the hook
// wired in. Useful for catching regressions where the hook itself
// breaks the happy path (e.g. firing position bug, nil-panic).
//
// For tidy, "halt" is a misnomer: the errgroup wrapper recovers
// panics inside individual goroutines, and the per-prop RemoveAll
// goroutines don't check ctx, so even with SetLimit(1)+FIFO the
// remaining goroutines still launch and run. The visible effect of
// the panic is that eg.Wait() returns the wrapper's recovered
// "panic occurred" error, which short-circuits the final
// markTidied() sentinel write. The post-halt on-disk state has
// IsSwapped=true, IsTidied=false (because markTidied never ran),
// and the per-prop backup dirs MAY all be gone (if the panic fired
// on the last goroutine) OR some MAY remain (if the panic fired
// on goroutine K<N — though even then K+1..N-1 still execute under
// concurrency=1 FIFO, so in practice all backups end up removed
// regardless of haltAfter). What this test actually pins for the
// tidy phase is: convergence from a torn state where the aggregate
// markTidied() write was lost — the same recovery branch as a
// crash between markSwapped() and markTidied().
func TestRecoveryConvergence_MidPropSwapOrTidy_Loop(t *testing.T) {
	// CI integration runs (`test/integration/run.sh:11`) export
	// `DISABLE_RECOVERY_ON_PANIC=true`, which makes the
	// ErrorGroupWrapper's deferred recover a no-op
	// (`entities/errors/error_group_wrapper.go:82-99`). Without that
	// recovery, the panic we deliberately fire from the per-prop
	// `testHookPostPropTidy` hook propagates out of the errgroup
	// goroutine and crashes the test binary, even though we capture
	// it at the test-level via `midPropTidyRunSwapWithRecover` /
	// `midPropTidyRunTidyWithRecover`. Force the env var false for
	// the lifetime of this test so the wrapper recovers panics into
	// `eg.Wait()` errors and the test's own panic-capture frame can
	// observe the halt deterministically — matching local-dev
	// behaviour. The pre-existing PR #11415 `MidPropSwap_Loop` test
	// happens to never hit this because it relies on the swap
	// loop's sequential (non-errgroup) per-prop frame, where the
	// panic IS captured by the test's outer recover.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	const numObjects = 25
	propNames := []string{"title", "subtitle", "description", "keywords"}

	baseline := computeMultiPropBaseline(t, propNames, numObjects)
	for _, propName := range propNames {
		require.NotEmptyf(t, baseline[propName],
			"mid-prop-tidy baseline must have non-empty fingerprint for prop %q", propName)
	}

	cases := []midPropTidyCase{
		{midPropTidyPhaseSwap, 0},
		{midPropTidyPhaseSwap, 1},
		{midPropTidyPhaseSwap, 2},
		{midPropTidyPhaseSwap, 3},
		{midPropTidyPhaseTidy, 0},
		{midPropTidyPhaseTidy, 1},
		{midPropTidyPhaseTidy, 2},
		{midPropTidyPhaseTidy, 3},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("phase=%s_haltAfter=%d", tc.phase, tc.haltAfter), func(t *testing.T) {
			ctx := testCtx()
			className := fmt.Sprintf("MidPropTidy_%s_%d_%s", tc.phase, tc.haltAfter, uuid.NewString()[:8])
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
			// Force sequential tidy so the per-prop hook fires in
			// deterministic propIdx order. Has no effect on swap (Phase
			// 2a is already strictly sequential).
			task.config.concurrency = 1

			// Phase 1: drive iteration to markReindexed, then
			// runtimePrepare to markMerged. After this, the next step
			// is runtimeSwap. For tidy cells, we additionally run
			// runtimeSwap so that markSwapped is set and tidy is the
			// next thing to run.
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

			switch tc.phase {
			case midPropTidyPhaseSwap:
				midPropTidyInstallSwapHook(task, tc.haltAfter)

				panicked, panicValue, swapReturned, swapErr := midPropTidyRunSwapWithRecover(
					ctx, task, shard, rt, props)

				if tc.haltAfter == 0 {
					// Baseline cell: hook never panics, swap must
					// complete cleanly.
					require.Falsef(t, panicked,
						"haltAfter=0 must not panic (got panicValue=%v)", panicValue)
					require.Truef(t, swapReturned, "haltAfter=0 must reach swap-return")
					require.NoErrorf(t, swapErr, "haltAfter=0 must succeed")
				} else {
					require.Falsef(t, swapReturned,
						"runtimeSwap returned without panicking (err=%v); hook did not fire — test harness invalid",
						swapErr)
					require.Truef(t, panicked,
						"expected panic from testHookPostPropSwap; got swapErr=%v", swapErr)
					panicStr, ok := panicValue.(string)
					require.Truef(t, ok && strings.HasPrefix(panicStr, midPropTidyHaltPanicPrefix),
						"recovered panic was not from the hook (want prefix %q; got %T %v)",
						midPropTidyHaltPanicPrefix, panicValue, panicValue)

					// Verify partial state: exactly `haltAfter`
					// markSwappedProp sentinels should be on disk.
					swappedCount := 0
					for _, p := range props {
						if rt.IsSwappedProp(p) {
							swappedCount++
						}
					}
					assert.GreaterOrEqualf(t, swappedCount, tc.haltAfter,
						"after Phase 2a halt, expected ≥%d markSwappedProp sentinels (got %d)",
						tc.haltAfter, swappedCount)
					assert.Lessf(t, swappedCount, len(propNames),
						"after Phase 2a halt, expected <%d markSwappedProp sentinels (got %d) — halt didn't fire",
						len(propNames), swappedCount)
				}

			case midPropTidyPhaseTidy:
				// runtimeSwap inlines markSwapped + markTidied
				// (Phase 2b/2c) and then trimOlderGenerationsLocked
				// removes the per-prop backup dirs. tidyBackupBuckets
				// is therefore dead code on the inline happy path;
				// it's only ever called from the recovery branches
				// in RunSwapOnShard / OnBeforeLsmInit. To exercise
				// the per-prop tidy hook explicitly we drive
				// runtimeSwap to completion, then reset the tidied
				// sentinel (leaving IsSwapped=true, IsTidied=false),
				// then call tidyBackupBuckets directly. The
				// RemoveAll calls land on already-missing dirs and
				// are OS-level no-ops, but the hook still fires
				// per-prop, which is what we need.
				require.NoError(t, task.runtimeSwap(ctx, task.logger, shard, rt, props),
					"runtimeSwap must succeed before tidy phase test")
				require.True(t, rt.IsTidied(),
					"runtimeSwap must have set tidied on completion")

				// Reset the tidied sentinel so tidyBackupBuckets
				// runs the markTidied call again (idempotent on
				// the bucket-removal side because RemoveAll is
				// OS-level idempotent). The hook can then fire.
				//
				// Same pattern as runCrossReplicaMigrationWithCrash
				// (inverted_reindex_recovery_multiprop_test.go:651)
				// — there is no public unmarkTidied so we remove
				// the tidied.mig file directly.
				ftr := rt.(*fileReindexTracker)
				tidiedPath := filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied)
				require.NoError(t, os.Remove(tidiedPath),
					"reset tidied sentinel for tidy-hook exercise")
				require.False(t, rt.IsTidied(), "tidied sentinel must be reset")

				var tidyFireCount atomic.Int64
				midPropTidyInstallTidyHook(task, tc.haltAfter, &tidyFireCount)

				tidyErr := midPropTidyRunTidyExpectingPanicError(
					ctx, task, shard, rt, props)

				if tc.haltAfter == 0 {
					// Baseline cell: hook never panics, tidy must
					// complete cleanly.
					require.NoErrorf(t, tidyErr,
						"haltAfter=0 must succeed (got %v)", tidyErr)
					require.True(t, rt.IsTidied(),
						"haltAfter=0 must end with IsTidied=true")
					// Hook fired exactly len(props) times.
					assert.Equalf(t, int64(len(propNames)), tidyFireCount.Load(),
						"haltAfter=0 hook must fire once per prop (got %d)",
						tidyFireCount.Load())
				} else {
					require.Errorf(t, tidyErr,
						"haltAfter=%d must surface a panic-recovered error", tc.haltAfter)
					require.Containsf(t, tidyErr.Error(), "panic occurred",
						"err must be the wrapper's recovered-panic error; got %v", tidyErr)
					// markTidied must NOT have run.
					require.False(t, rt.IsTidied(),
						"haltAfter=%d: markTidied must not have run", tc.haltAfter)
				}
			}

			// Phase 3: restart, drive recovery.
			shardName := shard.Name()
			shardLSMPath := shard.pathLSM()
			require.NoError(t, shard.Shutdown(ctx))

			// Same orphan-bucket cleanup rationale as
			// TestRecoveryConvergence_MidPropSwap_Loop — see that
			// test's comment for the full reasoning. For tidy cells
			// the call is a no-op (no orphan buckets) but it's
			// harmless and keeps the recovery path identical across
			// cells.
			simulateProcessRestartBucketCleanup(t, shardLSMPath)

			strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task2 := newTestTask(idx.logger, strategy2)
			task2.skipSwapOnFinish.Store(false)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoErrorf(t, err, "mid-prop-tidy shard re-init (phase=%s, haltAfter=%d)",
				tc.phase, tc.haltAfter)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err,
					"mid-prop-tidy recovery OnAfterLsmInitAsync (phase=%s, haltAfter=%d)",
					tc.phase, tc.haltAfter)
				if rerunAt.IsZero() {
					break
				}
			}

			// Phase 4: per-prop convergence — every prop must converge
			// to baseline.
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard2.store.Bucket(bucketName)
				require.NotNilf(t, bucket,
					"mid-prop-tidy bucket %q must exist post-recovery (phase=%s, haltAfter=%d)",
					propName, tc.phase, tc.haltAfter)
				require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
					"mid-prop-tidy bucket %q must be StrategyInverted post-recovery (phase=%s, haltAfter=%d)",
					propName, tc.phase, tc.haltAfter)

				got := fingerprintInvertedBucket(t, bucket)
				expected := baseline[propName]

				assert.Equalf(t, len(expected), len(got),
					"mid-prop-tidy term count for %q diverges (phase=%s, haltAfter=%d)",
					propName, tc.phase, tc.haltAfter)
				for term, expectedIDs := range expected {
					gotIDs, ok := got[term]
					if !ok {
						assert.Failf(t, "mid-prop-tidy missing term",
							"term %q on prop %q present in baseline but missing post-recovery (phase=%s, haltAfter=%d)",
							term, propName, tc.phase, tc.haltAfter)
						continue
					}
					assert.Equalf(t, expectedIDs, gotIDs,
						"mid-prop-tidy term %q on prop %q diverges (phase=%s, haltAfter=%d)\n  baseline (%d): %v\n  got      (%d): %v",
						term, propName, tc.phase, tc.haltAfter, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
				}
			}
		})
	}
}
