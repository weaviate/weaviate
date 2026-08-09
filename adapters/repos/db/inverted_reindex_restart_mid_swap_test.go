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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Restart in the middle of a swap, for a repair-rangeable migration on a
// shard whose range index was healthy to begin with.
//
// The swap renames the canonical dir to backup_<gen> and only then writes
// swapped.mig, so a node killed between those two points and markTidied
// comes back with no dir under the canonical name. Nothing else in shard
// init restores it: createPropertyValueIndex creates an empty bucket
// because the schema says the property has a range index, and the
// recovery-only reindexer's RunBeforeLsmInit is a no-op. If startup
// declines to promote the ingest dir, the property serves zero rows.
//
// This is the reachable half of a repair-rangeable restart: the shards
// that reach it are the ones whose liveness lookup answers Live, which
// after this PR means lazily-loaded and multi-tenant ones. Only the
// "task still running" arm is a receipt for that; the "task already
// gone" arm restarts with a schema that agrees, which the old code
// promoted anyway, so it is a control.

func TestRestartRecovery_MidSwapRepairRangeableKeepsServing(t *testing.T) {
	const (
		numObjects = 25
		taskID     = "mid-swap-repair-task"
		version    = uint64(11)
	)
	propName := filterableToRangeablePropName

	for _, taskLive := range []bool{true, false} {
		name := "task still running"
		if !taskLive {
			name = "task already gone"
		}
		t.Run(name, func(t *testing.T) {
			ctx := testCtx()
			className := "RestartMidSwap_" + uuid.NewString()[:8]
			class := newFilterableToRangeableTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			require.True(t, rt.IsSwapped(), "setup must reach swapped")
			migrationPath := rt.(*fileReindexTracker).config.migrationPath

			// The expected fingerprint is what the swap put in place —
			// this is what a restart must still serve.
			bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
			wantFingerprint := filterableToRangeableFingerprint(t, shard.store.Bucket(bucketName))
			require.NotEmpty(t, wantFingerprint, "setup must leave real postings to serve")

			// Removing tidied.mig is the crash: markTidied is the very
			// next write after the canonical→backup rename, so this is
			// the disk state a kill in that window leaves behind.
			require.NoError(t, os.Remove(filepath.Join(migrationPath, "tidied.mig")))
			writeTrackerPayload(t, migrationPath, ReindexTaskPayload{
				MigrationType: ReindexTypeRepairRangeable,
				Collection:    className,
				Properties:    []string{propName},
			}, taskID, version)

			lsmPath := shard.pathLSM()
			require.False(t, fileExists(filepath.Join(lsmPath, bucketName)),
				"the swap renamed the canonical dir away; that is the premise of this test")

			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			installTaskLiveness(t, idx, taskID, version, taskLive)

			// repair-rangeable runs on a schema that already promises the
			// range index, so queries read the canonical bucket from the
			// first moment the restarted shard serves.
			enabled := true
			restartClass := newFilterableToRangeableTestClass(className)
			restartClass.Properties[0].IndexRangeFilters = &enabled

			shd2, err := idx.initShard(ctx, shardName, restartClass, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed")
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			bucket := shard2.store.Bucket(bucketName)
			require.NotNil(t, bucket, "the canonical rangeable bucket must exist after restart")
			require.Equal(t, wantFingerprint, filterableToRangeableFingerprint(t, bucket),
				"the restarted shard must serve the same rows it served before the crash")

			require.Empty(t, dirsWithPrefix(t, lsmPath, bucketName+"__rangeable_ingest"),
				"the ingest dir was promoted to canonical, so it is gone")
			require.False(t, fileExists(migrationPath), "the tracker has done its job")
			require.True(t, fileExists(migrationFinalizedMarkerPath(lsmPath, task.MigrationDirName())),
				"the promotion must record itself, or the task below cannot tell done from never-ran")

			// The task the cluster is still waiting on now runs its swap
			// phase against a shard whose migration startup already
			// finished. It must ack success: failing here would report
			// the migration as failed cluster-wide on a shard whose data
			// is correct.
			require.NoError(t, task.RunPrepareOnShard(ctx, shard2),
				"the prepare phase must ack work startup already did")
			require.NoError(t, task.RunSwapOnShard(ctx, shard2),
				"the swap phase must ack work startup already did")
			require.Equal(t, wantFingerprint, filterableToRangeableFingerprint(t, shard2.store.Bucket(bucketName)),
				"the ack must not have disturbed the promoted data")
			require.False(t, fileExists(migrationPath),
				"the swap phase must not leave an empty tracker dir behind — it would blind the startup audit")
		})
	}
}

// Restart in the middle of a runtime swap, killed between the in-memory
// bucket-pointer flip (Phase 2a) and the shutdown + rename that makes it
// durable (Phase 2b).
//
// The flip died with the process, but its per-prop sentinel is on disk.
// Trusting that sentinel makes the retry skip the flip while still setting
// the tokenization overlay and marking the migration done, so the shard
// serves the pre-migration bucket under a schema that says otherwise —
// the shape of the per-replica divergence seen in CI.
func TestRestartRecovery_StaleSwapSentinelDoesNotSkipTheFlip(t *testing.T) {
	const (
		numObjects = 25
		propName   = "title"
		taskID     = "stale-swap-sentinel-task"
		version    = uint64(7)
	)

	ctx := testCtx()
	className := "RestartStaleSwapSentinel_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Change-tokenization is the migration the CI divergence was in, and
	// the only type here whose schema can disagree at restart: the
	// content-equivalent ones are accepted unconditionally, so startup
	// would promote and there would be no swap left to retry.
	task, _ := newFilterableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsMerged(), "setup must reach merged, the state the swap starts from")
	require.False(t, rt.IsSwapped(), "the kill lands before the swap is durable")
	migrationPath := rt.(*fileReindexTracker).config.migrationPath

	// The rows the ingest bucket holds are what the swap owes the
	// canonical name.
	wantFingerprint := fingerprintRoaringSetBucket(t,
		shard.store.Bucket(task.ingestBucketName(propName)))
	require.NotEmpty(t, wantFingerprint, "setup must leave real postings to serve")

	// The dead process got as far as Phase 2a: pointer flipped in memory,
	// per-prop sentinel written. Then it died, taking the flip with it.
	require.NoError(t, rt.(*fileReindexTracker).markSwappedProp(propName))

	// A real tracker carries its task identity, so the restart takes the
	// promotion decision instead of defaulting to "promote unverified".
	writeTrackerPayload(t, migrationPath, ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		Collection:         className,
		Properties:         []string{propName},
		TargetTokenization: models.PropertyTokenizationField,
	}, taskID, version)

	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	// Restart with the pre-migration schema (still word tokenization), so
	// startup declines to promote and the swap retry is what has to
	// converge.
	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "shard re-init must succeed")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	require.True(t, fileExists(migrationPath),
		"startup must leave the tracker for the retry; if it promoted instead, "+
			"the retry below never sees the stale sentinel and this test proves nothing")

	// Recovery registers the task as a shard-init callback, so the ingest
	// buckets are warm and the swap phase takes the in-memory path.
	require.NoError(t, task.OnAfterLsmInit(ctx, shard2))
	require.NoError(t, task.RunSwapOnShard(ctx, shard2))

	bucketName := helpers.BucketFromPropNameLSM(propName)
	require.Equal(t, wantFingerprint,
		fingerprintRoaringSetBucket(t, shard2.store.Bucket(bucketName)),
		"the retry must flip the canonical bucket to the migrated rows — a per-prop "+
			"sentinel left by the dead process must not make it skip the flip")
}
