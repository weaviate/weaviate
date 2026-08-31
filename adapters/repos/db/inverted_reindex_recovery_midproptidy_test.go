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
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// midPropSwapHaltPanicPrefix is the sentinel string every hook-driven
// panic carries, so the recover() handler can tell the expected halt
// apart from an unrelated panic that would otherwise be swallowed and
// mask a real bug.
const midPropSwapHaltPanicPrefix = "mid-prop-swap halt: simulated crash"

// midPropSwapInstallFault wraps the production processOneSwapProp
// method so it panics once haltAfter properties have flipped. A
// haltAfter of 0 is a pass-through. Injected via processOneSwapPropFn,
// so the production method stays untouched.
func midPropSwapInstallFault(task *ShardReindexTaskGeneric, haltAfter int) {
	prod := task.processOneSwapProp
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, propIdx int, propName string) (*lsmkv.Bucket, error) {
		bucket, err := prod(ctx, store, propIdx, propName)
		if err != nil {
			return nil, err
		}
		if haltAfter > 0 && propIdx == haltAfter-1 {
			panic(fmt.Sprintf("%s (propIdx=%d, haltAfter=%d)",
				midPropSwapHaltPanicPrefix, propIdx, haltAfter))
		}
		return bucket, nil
	}
}

// midPropSwapRunWithRecover runs runtimeSwap inside a defer-recover
// frame, returning (panicked, panicValue, swapReturned, swapErr).
//
// Phase 2a is a sequential per-prop loop in the calling goroutine, so a
// panic from the injected fault propagates straight up the stack and the
// deferred recover() catches it.
func midPropSwapRunWithRecover(ctx context.Context, task *ShardReindexTaskGeneric,
	shard *Shard, props []string,
) (panicked bool, panicValue interface{}, swapReturned bool, swapErr error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicValue = r
		}
	}()
	swapErr = task.runtimeSwap(ctx, task.logger, shard, props)
	swapReturned = true
	return
}

// TestRecoveryConvergence_MidPropSwap_HaltMatrix pins recovery
// convergence when the per-prop flip loop is interrupted after K of N
// props, for every K on a 4-prop class. K=0 is the no-halt baseline.
//
// The retirement of the displaced directories runs inline right after
// the flip loop, on the same goroutine, so a halt inside the flip loop
// is also a halt before any of that retirement has run.
func TestRecoveryConvergence_MidPropSwap_HaltMatrix(t *testing.T) {
	const numObjects = 25
	propNames := []string{"title", "subtitle", "description", "keywords"}

	baseline := computeMultiPropBaseline(t, propNames, numObjects)
	for _, propName := range propNames {
		require.NotEmptyf(t, baseline[propName],
			"baseline must have a non-empty fingerprint for prop %q", propName)
	}

	for _, haltAfter := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("haltAfter=%d", haltAfter), func(t *testing.T) {
			ctx := testCtx()
			className := fmt.Sprintf("MidPropSwap_%d_%s", haltAfter, uuid.NewString()[:8])
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

			// Drive the rebuild and the prep, so the flip loop is next.
			task.skipSwapOnFinish.Store(true)
			require.NoError(t, task.OnAfterLsmInit(ctx, shard))
			for {
				rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				require.NoError(t, err)
				if rerunAt.IsZero() {
					break
				}
			}
			rec, ok := task.migrationRecord(shard)
			require.True(t, ok, "the rebuild must have left a record")
			props := rec.Subject().Properties
			require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, props))

			midPropSwapInstallFault(task, haltAfter)

			panicked, panicValue, swapReturned, swapErr := midPropSwapRunWithRecover(
				ctx, task, shard, props)

			if haltAfter == 0 {
				require.Falsef(t, panicked,
					"haltAfter=0 must not panic (got panicValue=%v)", panicValue)
				require.Truef(t, swapReturned, "haltAfter=0 must reach swap-return")
				require.NoErrorf(t, swapErr, "haltAfter=0 must succeed")
			} else {
				require.Falsef(t, swapReturned,
					"runtimeSwap returned without panicking (err=%v); the fault did not fire — test harness invalid",
					swapErr)
				require.Truef(t, panicked,
					"expected panic from the injected fault; got swapErr=%v", swapErr)
				panicStr, ok := panicValue.(string)
				require.Truef(t, ok && strings.HasPrefix(panicStr, midPropSwapHaltPanicPrefix),
					"recovered panic was not from the fault (want prefix %q; got %T %v)",
					midPropSwapHaltPanicPrefix, panicValue, panicValue)

				// A flipped property has no ingest-name entry left in the
				// store, which is the same fact the flip loop reads to skip
				// one it already flipped.
				flippedCount := 0
				for _, p := range props {
					if shard.store.Bucket(task.ingestBucketName(p)) == nil {
						flippedCount++
					}
				}
				assert.GreaterOrEqualf(t, flippedCount, haltAfter,
					"after the halt, expected >=%d props flipped (got %d)", haltAfter, flippedCount)
				assert.Lessf(t, flippedCount, len(propNames),
					"after the halt, expected <%d props flipped (got %d) — the fault did not fire",
					len(propNames), flippedCount)
			}

			// Restart and drive recovery.
			shardName := shard.Name()
			shardLSMPath := shard.pathLSM()
			require.NoError(t, shard.Shutdown(ctx))

			// Same orphan-bucket cleanup rationale as
			// TestRecoveryConvergence_MidPropSwap_Loop — see that test's
			// comment for the full reasoning.
			simulateProcessRestartBucketCleanup(t, shardLSMPath)

			strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task2 := newTestTask(idx.logger, strategy2)
			task2.skipSwapOnFinish.Store(false)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoErrorf(t, err, "mid-prop-swap shard re-init (haltAfter=%d)", haltAfter)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err,
					"mid-prop-swap recovery OnAfterLsmInitAsync (haltAfter=%d)", haltAfter)
				if rerunAt.IsZero() {
					break
				}
			}

			// Every prop must converge to baseline.
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard2.store.Bucket(bucketName)
				require.NotNilf(t, bucket,
					"mid-prop-swap bucket %q must exist post-recovery (haltAfter=%d)",
					propName, haltAfter)
				require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
					"mid-prop-swap bucket %q must be StrategyInverted post-recovery (haltAfter=%d)",
					propName, haltAfter)

				got := fingerprintInvertedBucket(t, bucket)
				expected := baseline[propName]

				assert.Equalf(t, len(expected), len(got),
					"mid-prop-swap term count for %q diverges (haltAfter=%d)",
					propName, haltAfter)
				for term, expectedIDs := range expected {
					gotIDs, ok := got[term]
					if !ok {
						assert.Failf(t, "mid-prop-swap missing term",
							"term %q on prop %q present in baseline but missing post-recovery (haltAfter=%d)",
							term, propName, haltAfter)
						continue
					}
					assert.Equalf(t, expectedIDs, gotIDs,
						"mid-prop-swap term %q on prop %q diverges (haltAfter=%d)\n  baseline (%d): %v\n  got      (%d): %v",
						term, propName, haltAfter, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
				}
			}
		})
	}
}
