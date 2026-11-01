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

// Pins getProcessedObjectCount's contract: sum per-chunk counts, not just the last checkpoint's.
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

	// Two checkpoints (40, then 8) to verify summation, not last-value.
	require.NoError(t, tr.markProgress(newKey(), 40, 40))
	require.NoError(t, tr.markProgress(newKey(), 8, 8))

	got, err = tr.getProcessedObjectCount()
	require.NoError(t, err)
	require.Equalf(t, 48, got,
		"getProcessedObjectCount must SUM per-chunk checkpoints (40+8=48), not "+
			"return the last chunk's count (8); got %d", got)
}

// Regression test for weaviate/0-weaviate-issues#317: resumed reindex
// progress must not restart near 0 / freeze at the pre-crash checkpoint.
func TestReindexResumeProgress_AdvancesFromCheckpoint(t *testing.T) {
	const numObjects = 100

	cases := []struct {
		name string
		// checkProcessingEveryNoObjects for phase 1; checkpoint lands at ~checkpointEvery objects.
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
			// Flush so ObjectCountAsync (reads on-disk segments only) sees all objects.
			require.NoError(t, shard.store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

			task, _ := newRoaringSetRefreshTask(t, idx)
			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			// --- Phase 1: process ~checkpointEvery objects, then break (persists a checkpoint).
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

			// --- Phase 2: resume; emit and capture progress for every object to completion.
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

			// Invariant 1: progress never regresses (monotonic).
			for i := 1; i < len(got); i++ {
				require.GreaterOrEqualf(t, got[i], got[i-1],
					"progress regressed at emission %d: %v", i, got)
			}

			// Invariant 2: the first resumed emission already reflects the pre-crash checkpoint.
			require.GreaterOrEqualf(t, got[0], checkpointFraction,
				"resumed progress restarted below the pre-crash checkpoint "+
					"(got[0]=%.4f, checkpoint=%.4f) — the /v1/tasks freeze-then-jump bug. "+
					"full sequence: %v", got[0], checkpointFraction, got)

			// Invariant 3: progress advances and climbs to near-complete (not a hang).
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
