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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// This file pins the flip-apply re-resolve discriminator for
// weaviate/0-weaviate-issues#220 (QA finding 5, Issue B). The hook
// [Shard.reResolveStagedSwapsOnSchemaFlip] re-drives a merged-only staged gen
// once the schema flips. A gen that reached merged-only by FAILING mid-swap is
// on-disk identical (in every sentinel) to a genuinely boot-reverted gen — so
// without a discriminator the hook re-drives the FAILED swap and trims the only
// recoverable OLD copy. The discriminator is a durable positive revert marker
// (reverted.mig) written only by the boot-revert path [restoreStagedDirsToOld];
// a failed-mid-swap gen lacks it and must be left untouched.

// TestReResolveDiscriminator_MergedOnlyRequiresRevertMarker is the pure
// predicate pin: a merged-only tracker dir is classified as boot-reverted iff
// the durable reverted.mig marker is present. Without the marker (the
// failed-mid-swap shape) the predicate must return false so the flip-apply
// hook leaves the gen alone.
func TestReResolveDiscriminator_MergedOnlyRequiresRevertMarker(t *testing.T) {
	write := func(dir, name string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644))
	}

	t.Run("merged-only without revert marker is NOT reverted (failed-mid-swap shape)", func(t *testing.T) {
		dir := t.TempDir()
		write(dir, "merged.mig")
		require.False(t, isRevertedMergedStagedGen(dir),
			"merged.mig alone must not be treated as boot-reverted — this is the failed-mid-swap shape whose OLD backup must survive (0-weaviate-issues#220)")
	})

	t.Run("merged + revert marker IS reverted (boot-revert shape)", func(t *testing.T) {
		dir := t.TempDir()
		write(dir, "merged.mig")
		write(dir, reindexRevertedMarkerFile)
		require.True(t, isRevertedMergedStagedGen(dir),
			"merged.mig + reverted.mig is the boot-revert shape and must be re-drivable on flip")
	})

	// Any progress/terminal sentinel alongside the revert marker still
	// disqualifies the gen — the predicate stays strict in both directions.
	for _, disqualifier := range []string{"staged.mig", "swapped.mig", "tidied.mig", "unswapped.mig"} {
		disqualifier := disqualifier
		t.Run("revert marker + "+disqualifier+" is NOT reverted", func(t *testing.T) {
			dir := t.TempDir()
			write(dir, "merged.mig")
			write(dir, reindexRevertedMarkerFile)
			write(dir, disqualifier)
			require.False(t, isRevertedMergedStagedGen(dir),
				"%s present must disqualify re-drive even with the revert marker", disqualifier)
		})
	}

	t.Run("revert marker without merged.mig is NOT reverted", func(t *testing.T) {
		dir := t.TempDir()
		write(dir, reindexRevertedMarkerFile)
		require.False(t, isRevertedMergedStagedGen(dir),
			"the merged prepend must have completed; a bare revert marker is not a re-drivable state")
	})
}

// TestReindexStagedSwap_FailedMidSwapMerged_NotReResolvedOnFlip is the
// integration pin (QA finding 5, Issue B). It builds a REAL failed-mid-swap
// gen — runtimeSwap's Phase 2b rename fails with ENOTDIR, the exact fault the
// multinode journey plants — leaving the tracker merged-only. It then applies
// the schema flip and fires the REAL flip-apply hook and asserts the hook does
// NOT re-drive: no commit, and the OLD data survives on disk. This is the
// data-loss amplifier the journey test caught: under the (separately fixed)
// base-protocol bug the flip lands over the failed unit, and without this
// discriminator the hook would then trim the only OLD copy.
func TestReindexStagedSwap_FailedMidSwapMerged_NotReResolvedOnFlip(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25
	className := "FailedMidSwapNoReResolve_" + uuid.NewString()[:8]

	class := newTestClassWithProps(className, []string{propName})
	canonicalBucket := helpers.BucketFromPropNameLSM(propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	lsmPath := shard.pathLSM()

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task := NewRuntimeFilterableRetokenizeTask(idx.logger, propName,
		models.PropertyTokenizationField, className, className, 1)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))

	// Induce a genuine Phase 2b failure: plant a regular FILE at the backup
	// rename target so runtimeSwap's os.Rename(<canonical dir>, <backup>)
	// fails with ENOTDIR (a directory onto a regular file). This mirrors the
	// journey's obstruction exactly and leaves the tracker merged-only.
	backupDir := filepath.Join(lsmPath, task.backupBucketName(propName))
	require.NoError(t, os.WriteFile(backupDir, []byte("obstruction"), 0o644))
	require.Error(t, task.RunSwapOnShard(ctx, shard),
		"the planted obstruction must make the Phase 2b rename fail")

	migDir := filepath.Join(lsmPath, ".migrations", task.strategy.MigrationDirName())

	rt, err := task.newReindexTracker(lsmPath)
	require.NoError(t, err)
	require.True(t, rt.IsMerged(), "failed-mid-swap gen must carry merged.mig (runtimePrepare ran)")
	require.False(t, rt.IsSwapped(), "global swapped.mig must be absent (Phase 2b failed before markSwapped)")
	require.False(t, rt.IsStaged(), "staged.mig must be absent")
	require.False(t, rt.IsTidied(), "tidied.mig must be absent")
	require.False(t, fileExists(filepath.Join(migDir, "unswapped.mig")), "unswapped.mig must be absent")
	require.False(t, fileExists(filepath.Join(migDir, reindexRevertedMarkerFile)),
		"a failed-mid-swap gen must NOT carry the boot-revert marker (it never ran the revert path)")

	// THE DISCRIMINATOR: this merged-only gen has no revert evidence, so it
	// must not be classified as boot-reverted. RED before the fix (returned
	// true → the hook re-drove the failed swap and trimmed the OLD backup).
	require.False(t, isRevertedMergedStagedGen(migDir),
		"a failed-mid-swap-merged gen (no reverted.mig) must NOT be classified as boot-reverted (0-weaviate-issues#220)")

	// Persist the recovery payload the hook consults, and clear the
	// obstruction so a WRONGFUL re-drive would be free to complete and trim —
	// i.e. the bug would be observable here if the discriminator failed.
	rec := reindexRecoveryRecord{
		TaskID: "failed-mid-swap-test", TaskVersion: 1, UnitID: "unit-0",
		Payload: ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenizationFilterable,
			Collection:         className,
			Properties:         []string{propName},
			TargetTokenization: models.PropertyTokenizationField,
		},
	}
	encoded, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, task.SaveRecoveryPayload(lsmPath, encoded))
	require.NoError(t, os.Remove(backupDir))

	// The OLD data still lives at the canonical name (the Phase 2b rename
	// never completed). It must survive the hook.
	canonicalDir := filepath.Join(lsmPath, canonicalBucket)
	require.True(t, dirExists(canonicalDir),
		"precondition: OLD data must be at the canonical dir after the failed rename")

	// Apply the schema flip and fire the REAL flip-apply hook. With the schema
	// now reflecting field, semanticMigrationSchemaFlipped is true — the ONLY
	// thing that must stop the destructive re-drive is the missing marker.
	class.Properties[0].Tokenization = models.PropertyTokenizationField
	shard.reResolveStagedSwapsOnSchemaFlip(ctx)

	rt2, err := task.newReindexTracker(lsmPath)
	require.NoError(t, err)
	require.False(t, rt2.IsTidied(),
		"the flip-apply hook must NOT commit a failed-mid-swap gen — committing trims the only OLD copy (0-weaviate-issues#220)")
	require.True(t, rt2.IsMerged(),
		"the failed gen's tracker must be left untouched (still merged-only) for the restart-time finalize")
	require.True(t, dirExists(canonicalDir),
		"the OLD data must survive — the hook must not have re-driven + trimmed it")
	require.True(t, dirExists(migDir),
		"the tracker dir must be kept in place so the next restart's finalize resolves it via the schema verdict")
}
