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
			panic(fmt.Sprintf("%s (phase=swap, propIdx=%d, haltAfter=%d)",
				midPropTidyHaltPanicPrefix, propIdx, haltAfter))
		}
	}
}

// midPropTidyInstallTidyHook panics after haltAfter completions.
// Concurrency MUST be 1 — otherwise the parallel goroutines race for
// the count and halt order becomes non-deterministic.
// The error-group wrapper recovers panics per-goroutine and surfaces
// them via eg.Wait(); subsequent SetLimit(1)-FIFO goroutines still run
// to completion.
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
			panic(fmt.Sprintf("%s (phase=tidy, propIdx=%d, haltAfter=%d, fireOrdinal=%d)",
				midPropTidyHaltPanicPrefix, propIdx, haltAfter, n))
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

// TestRecoveryConvergence_MidPropSwapOrTidy_Loop pins recovery
// convergence when the per-prop swap or tidy loop is interrupted
// after K of N props. Matrix: (phase ∈ {swap, tidy}) × (haltAfter ∈
// {0..3}) on a 4-prop class; haltAfter=0 is the no-halt baseline.
//
// Tidy "halt" is via the errgroup wrapper surfacing a recovered
// panic as eg.Wait()'s error, which short-circuits the aggregate
// markTidied() write. Recovery branch exercised is identical to a
// crash between markSwapped() and markTidied().
func TestRecoveryConvergence_MidPropSwapOrTidy_Loop(t *testing.T) {
	// CI integration runs (`test/integration/run.sh:11`) export
	// `DISABLE_RECOVERY_ON_PANIC=true`, which makes the
	// ErrorGroupWrapper's deferred recover a no-op
	// (`entities/errors/error_group_wrapper.go:82-99`). Without that
	// recovery, the panic deliberately fired from the per-prop
	// `testHookPostPropTidy` hook propagates out of the error-group
	// goroutine and crashes the test binary — `midPropTidyRunTidyExpectingPanicError`
	// (the tidy helper this test uses) relies on the wrapper turning
	// the panic into an `eg.Wait()` error, not on a test-level
	// recover. Force the env var false for the lifetime of this test
	// so the wrapper recovers panics into errors and the assertion
	// frame can observe the halt deterministically. The swap helper
	// `midPropTidyRunSwapWithRecover` is unaffected (Phase 2a runs
	// in the calling goroutine, where its own defer-recover catches
	// the panic regardless of the env var).
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
