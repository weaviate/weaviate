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
