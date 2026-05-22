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
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// Exhaustive integration test for restart-recovery convergence
// -----------------------------------------------------------------------------
//
// Per weaviate/0-weaviate-issues#240 Symptom B: rolling restart of a 3-node
// cluster mid-change-tokenization-migration can land all three replicas
// on diverged on-disk bucket content despite the task showing FINISHED.
// The acceptance tests rely on random rolling-restart timing and only
// probabilistically hit a few combinations of the recovery cross-product.
//
// Following the test-pyramid principle, the deterministic exhaustive
// coverage of the recovery state machine belongs at integration level
// (single-process, real LSM store, real migration code path), not flaky
// e2e. The baseline assertion that the migration code works end-to-end
// with the per-doc-id fingerprint we compare against is foundational —
// every recovery-from-state case in the staged follow-up compares
// against this baseline. If the baseline itself doesn't reproduce the
// expected post-migration bucket content, no recovery test can.
//
// Coverage matrix (build out in stages):
//   - [stage 1, THIS COMMIT] Baseline only: clean migration to
//     completion, fingerprint the post-state. Asserts the migration
//     produces a non-empty per-term posting list and the fingerprint
//     primitive works.
//   - [stage 2, follow-up] Recovery from each sentinel state: drive
//     the migration to each on-disk state a crashed replica could
//     land in, then restart with a fresh task, then assert the
//     post-recovery fingerprint matches the baseline.
//   - [stage 3, follow-up] Mid-iteration resume from a non-empty
//     lastProcessedKey.
//   - [stage 4, follow-up] Mid-per-prop-loop crashes inside
//     runtimePrepare / runtimeSwap when multiple props are migrating
//     in lock-step.

// fingerprintInvertedBucket reads a searchable bucket using its public
// Cursor and returns a deterministic (term → sorted []docID) snapshot.
// Used to compare post-recovery bucket content against the baseline.
//
// Format: map[term]sortedDocIDs. The frequency-per-doc is NOT compared
// here — per-doc inclusion is sufficient to catch the #11383
// divergence shape (a node returning 0 hits for a query == that term
// has no posting list on that node).
func fingerprintInvertedBucket(t *testing.T, b *lsmkv.Bucket) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	if b == nil {
		return out
	}
	// Inverted-strategy buckets: iterate via MapCursor. Each row key
	// is a term; each map pair under the row is a (docID, frequency)
	// tuple. We collect docIDs only.
	c, err := b.MapCursor()
	require.NoError(t, err)
	defer c.Close()
	for k, pairs := c.First(context.Background()); k != nil; k, pairs = c.Next(context.Background()) {
		term := string(append([]byte(nil), k...))
		ids := make([]uint64, 0, len(pairs))
		for _, p := range pairs {
			if len(p.Key) < 8 {
				continue
			}
			id := uint64(p.Key[0])<<56 |
				uint64(p.Key[1])<<48 |
				uint64(p.Key[2])<<40 |
				uint64(p.Key[3])<<32 |
				uint64(p.Key[4])<<24 |
				uint64(p.Key[5])<<16 |
				uint64(p.Key[6])<<8 |
				uint64(p.Key[7])
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[term] = ids
	}
	return out
}

// makeConvergenceTestObjects builds a deterministic list of test
// objects. Text values cycle through a dictionary so the same word
// appears in multiple docs (replicates the BM25 "alpha appears in N
// docs" fingerprint that the #11383 acceptance test asserts on).
func makeConvergenceTestObjects(t *testing.T, n int, className string) []*storobj.Object {
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
		text := tokens[i%len(tokens)] + " " + tokens[(i+1)%len(tokens)] + " " + tokens[(i+2)%len(tokens)]
		out[i] = createTestObjectWithText(className, text)
	}
	return out
}

// TestRecoveryConvergence_Baseline runs a clean MapToBlockmax migration
// to completion using the production code path (task.OnAfterLsmInit +
// OnAfterLsmInitAsync loop), then fingerprints the post-migration
// searchable bucket. Establishes that:
//
//  1. The migration code path works end-to-end against the test
//     fixture (testShardWithSettings + makeConvergenceTestObjects).
//  2. The fingerprint primitive produces a non-empty (term → []docID)
//     mapping that can be used as the ground truth for the
//     recovery-from-state cases in stage 2.
//
// This test alone does NOT pin the #240 bug; it pins the foundation
// the staged cases will build on. If this test fails, the staged
// follow-ups have no baseline to compare against.
func TestRecoveryConvergence_Baseline(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "ConvergenceBaseline_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := makeConvergenceTestObjects(t, numObjects, className)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Pre-migration: bucket is MapCollection (source strategy).
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, preBucket, "pre-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyMapCollection, preBucket.Strategy(),
		"pre-migration searchable bucket must be StrategyMapCollection")

	// Drive the migration to completion using production code.
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
	require.True(t, strategy.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	// Post-migration: bucket strategy must have flipped to Inverted.
	postBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, postBucket, "post-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-migration searchable bucket must be StrategyInverted")

	// Tracker must show all sentinels in the terminal state.
	rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())

	// Fingerprint: should be non-empty (every test object contributes
	// 3 tokens). Every token in our dictionary should appear at least
	// once because the cycling pattern ensures each token is hit.
	fp := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, fp, "baseline fingerprint must have at least one term")

	// Every dictionary token should be present given numObjects=25
	// and the cycle pattern (each token starts a 3-token window for
	// some doc index, and our dictionary has 25 entries).
	expectedTokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	for _, tok := range expectedTokens {
		docIDs, ok := fp[tok]
		require.Truef(t, ok, "baseline fingerprint missing token %q (post-migration bucket should contain every dictionary word)", tok)
		require.NotEmptyf(t, docIDs, "baseline fingerprint token %q has no docIDs (posting list is empty)", tok)
	}
}
