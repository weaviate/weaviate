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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestReindexTracker_GetProcessedObjectCount_SumsCheckpoints pins the
// contract getProcessedObjectCount relies on: the cumulative processed
// count is the SUM of every checkpoint's per-chunk "all N" count, not the
// last checkpoint's count. markProgress writes one file per chunk with that
// chunk's local count (it resets to 0 each OnAfterLsmInitAsync invocation),
// so summing is the only way to recover cumulative progress after a resume.
// weaviate/0-weaviate-issues#317.
func TestReindexTracker_GetProcessedObjectCount_SumsCheckpoints(t *testing.T) {
	keyParser := &UuidKeyParser{}
	newKey := func() indexKey {
		b, err := uuid.New().MarshalBinary()
		require.NoError(t, err)
		return keyParser.FromBytes(b)
	}

	tr := NewFileReindexTracker(t.TempDir(), "resume_progress_sum", keyParser)
	require.NoError(t, tr.init())

	// Fresh tracker, no checkpoints yet → 0.
	got, err := tr.getProcessedObjectCount()
	require.NoError(t, err)
	require.Equal(t, 0, got, "no checkpoints must report zero processed objects")

	// Chunk 1 processed 40 objects; chunk 2 processed a further 8. Each
	// markProgress records that chunk's LOCAL count (40, then 8).
	require.NoError(t, tr.markProgress(newKey(), 40, 40))
	require.NoError(t, tr.markProgress(newKey(), 8, 8))

	got, err = tr.getProcessedObjectCount()
	require.NoError(t, err)
	require.Equalf(t, 48, got,
		"getProcessedObjectCount must SUM per-chunk checkpoints (40+8=48), not "+
			"return the last chunk's count (8); got %d", got)
}

// TestReindexResumeProgress_AdvancesFromCheckpoint is the regression test
// for weaviate/0-weaviate-issues#317: after a crash mid-reindex, the
// resumed scan restarts from the persisted checkpoint key, but the live
// /v1/tasks progress used to freeze at the pre-crash checkpoint value and
// then jump to completion in one step.
//
// Root cause: the reindex loop reports progress as
// (this-invocation processedCount) / totalObjects, and processedCount
// resets to 0 every time OnAfterLsmInitAsync is (re-)entered. On resume the
// emitted fraction therefore restarts near 0; because the FSM stores unit
// progress monotonically (cluster/distributedtask.Manager.UpdateUnitProgress
// drops regressions), the displayed value sticks at the checkpoint until the
// resumed scan re-crosses it — which, for a late checkpoint, never happens
// before completion. Operators cannot distinguish this freeze from a hang.
//
// The fix makes the numerator cumulative (baseline from prior checkpoints +
// this invocation's processedCount), so the resumed scan reports at the same
// granularity as a fresh scan.
//
// This test drives a real RoaringSetRefresh migration on an in-memory shard:
//
//	Phase 1 processes ~checkpointPct of the objects and stops WITHOUT
//	        finishing (processingDuration=1ns forces a break at the first
//	        checkProcessingEveryNoObjects boundary), persisting a checkpoint
//	        — exactly the on-disk state a SIGKILL mid-build leaves behind.
//	Phase 2 installs a progress callback and drives the resumed scan to
//	        completion, capturing every emitted fraction.
//
// The captured sequence pins the operator-visible invariant: resumed progress
// is monotonic AND never regresses below the pre-crash checkpoint fraction.
// Pre-fix, phase 2's first emission is ~1/N (near zero), so both assertions
// fail — precisely the "frozen then jump" symptom from the issue.
func TestReindexResumeProgress_AdvancesFromCheckpoint(t *testing.T) {
	const numObjects = 100

	cases := []struct {
		name string
		// checkpointEvery is checkProcessingEveryNoObjects for phase 1; the
		// scan breaks at the first multiple of it, so the checkpoint lands at
		// ~checkpointEvery objects. Chosen to mirror the two crash points the
		// issue reported (R2b ≈ 0.48, R2c ≈ 0.89).
		checkpointEvery int
	}{
		{name: "checkpoint_at_~48pct_R2b", checkpointEvery: 48},
		{name: "checkpoint_at_~89pct_R2c", checkpointEvery: 89},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			const propName = "title"
			className := "ResumeProgress_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			// Flush objects to disk so ObjectCountAsync (the progress
			// denominator) sees all of them; it reads on-disk segments only.
			require.NoError(t, shard.store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

			task, _ := newRoaringSetRefreshTask(t, idx)
			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			// --- Phase 1: pre-crash. Process ~checkpointEvery objects, then
			// break without finishing, persisting a checkpoint. No callback
			// installed — we only care about the resumed emissions.
			task.config.checkProcessingEveryNoObjects = tc.checkpointEvery
			task.config.processingDuration = time.Nanosecond
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err)
			require.Falsef(t, rerunAt.IsZero(),
				"phase 1 must break mid-scan (not finish) so there is a resume to test")

			// Read the persisted baseline via the same path the fix uses.
			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			baseline, err := rt.getProcessedObjectCount()
			require.NoError(t, err)
			require.Greaterf(t, baseline, 0,
				"phase 1 must persist a non-zero checkpoint; got baseline=%d", baseline)
			require.Lessf(t, baseline, numObjects,
				"phase 1 must NOT finish the whole scan; got baseline=%d of %d", baseline, numObjects)
			checkpointFraction := float32(baseline) / float32(numObjects)
			t.Logf("phase 1 checkpoint: %d/%d objects (fraction %.4f)",
				baseline, numObjects, checkpointFraction)

			// --- Phase 2: resume. Emit progress for every object and run to
			// completion, capturing the fractions.
			task.config.checkProcessingEveryNoObjects = 1
			task.config.processingDuration = 10 * time.Minute

			var mu sync.Mutex
			var got []float32
			task.SetProgressCallback(func(p float32) {
				mu.Lock()
				got = append(got, p)
				mu.Unlock()
			})

			for {
				rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				require.NoError(t, err)
				if rerunAt.IsZero() {
					break
				}
			}

			require.NotEmpty(t, got, "resumed scan must emit progress")

			// Invariant 1: progress is monotonic non-decreasing — the operator
			// never sees the bar go backwards.
			for i := 1; i < len(got); i++ {
				require.GreaterOrEqualf(t, got[i], got[i-1],
					"progress regressed at emission %d: %v", i, got)
			}

			// Invariant 2 (the fix): the FIRST resumed emission already reflects
			// the pre-crash checkpoint — it does not restart near zero. Pre-fix
			// got[0] ≈ 1/N, far below checkpointFraction, so this fails.
			require.GreaterOrEqualf(t, got[0], checkpointFraction,
				"resumed progress restarted below the pre-crash checkpoint "+
					"(got[0]=%.4f, checkpoint=%.4f) — the /v1/tasks freeze-then-jump bug. "+
					"full sequence: %v", got[0], checkpointFraction, got)

			// Invariant 3 ("verified not a hang"): progress visibly advances
			// during the resumed work and climbs to near-complete. Pre-fix, for
			// the late (89%) checkpoint the emitted max never exceeds ~0.11.
			last := got[len(got)-1]
			require.Greaterf(t, last, got[0],
				"resumed progress did not advance during the rebuild (stuck at %.4f) — "+
					"indistinguishable from a hang", got[0])
			require.GreaterOrEqualf(t, last, float32(0.9),
				"resumed progress did not climb to near-complete; final=%.4f, sequence=%v",
				last, got)

			t.Logf("resume emitted %d fractions: first=%.4f last=%.4f",
				len(got), got[0], last)
		})
	}
}
