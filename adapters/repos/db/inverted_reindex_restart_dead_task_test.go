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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Restart-recovery half of the promotion scoping. Drives a real
// migration to the merged-but-untidied state through the production
// task methods, then restarts the shard through idx.initShard — the
// same entry point production uses — with the distributed task list
// answering "this task is gone".
//
// See https://github.com/weaviate/0-weaviate-issues/issues/464.

// installTaskLiveness makes the shard-init liveness lookup answer for
// exactly one task identity, the way a restarted node's distributed
// task list would.
func installTaskLiveness(t *testing.T, idx *Index, taskID string, version uint64, live bool) {
	t.Helper()
	idx.db.SetReindexAuditDeps(context.Background(), func(context.Context) (KnownReindexTaskLookup, error) {
		return func(gotID string, gotVersion uint64) bool {
			return live && gotID == taskID && gotVersion == version
		}, nil
	}, logrus.New())
}

// writeTrackerPayload puts the payload.mig record next to the tracker's
// sentinels, exactly as ReindexProvider.persistRecoveryRecord does when
// a task starts.
func writeTrackerPayload(t *testing.T, migrationPath string, payload ReindexTaskPayload, taskID string, version uint64) {
	t.Helper()
	rec, err := json.Marshal(reindexRecoveryRecord{
		TaskID: taskID, TaskVersion: version, UnitID: "unit-0", Payload: payload,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(migrationPath, reindexRecoveryPayloadFile), rec, 0o644))
}

// dirsWithPrefix lists the shard's LSM dirs starting with prefix, used
// to assert on disk state rather than on query results.
func dirsWithPrefix(t *testing.T, lsmPath, prefix string) []string {
	t.Helper()
	entries, err := os.ReadDir(lsmPath)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			out = append(out, e.Name())
		}
	}
	return out
}

// dirHasSegments reports whether a bucket dir holds any LSM segment,
// i.e. whether data was moved into it. An absent or empty dir is the
// "nothing was promoted here" answer.
func dirHasSegments(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".db") {
			return true
		}
	}
	return false
}

// TestRestartRecovery_MergedResidueOfDeadTask drives an enable-rangeable
// migration to merged-but-untidied, then restarts the shard with the
// task gone from the task list. With the schema disagreeing (the state a
// cancel leaves), the residue must be discarded and nothing promoted.
// With the schema agreeing (a task that finished cluster-wide before
// this node died), the residue must still promote.
func TestRestartRecovery_MergedResidueOfDeadTask(t *testing.T) {
	const (
		numObjects = 25
		taskID     = "restart-recovery-task"
		version    = uint64(3)
	)
	propName := filterableToRangeablePropName

	cases := []struct {
		name         string
		schemaAgrees bool
		taskLive     bool
		wantPromoted bool
		wantResidue  bool
	}{
		{name: "cancelled task, schema disagrees", schemaAgrees: false, taskLive: false},
		{name: "finished task, schema agrees", schemaAgrees: true, wantPromoted: true},
		{name: "running task, schema disagrees", taskLive: true, wantResidue: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RestartDeadTask_" + uuid.NewString()[:8]
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

			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			require.True(t, rt.IsMerged(), "setup must reach merged")
			require.False(t, rt.IsTidied(), "setup must stop before tidied")
			migrationPath := rt.(*fileReindexTracker).config.migrationPath
			writeTrackerPayload(t, migrationPath, ReindexTaskPayload{
				MigrationType: ReindexTypeEnableRangeable,
				Collection:    className,
				Properties:    []string{propName},
			}, taskID, version)

			lsmPath := shard.pathLSM()
			ingestPrefix := helpers.BucketRangeableFromPropNameLSM(propName) + "__rangeable_ingest"
			require.NotEmpty(t, dirsWithPrefix(t, lsmPath, ingestPrefix),
				"setup must leave an ingest dir on disk")

			// Restart. The restarted node reads the schema it is given
			// and asks the task list about the tracker's task.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			installTaskLiveness(t, idx, taskID, version, tc.taskLive)

			restartClass := newFilterableToRangeableTestClass(className)
			if tc.schemaAgrees {
				enabled := true
				restartClass.Properties[0].IndexRangeFilters = &enabled
			}

			shd2, err := idx.initShard(ctx, shardName, restartClass, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed")
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			// On-disk assertions first: they distinguish "wrong answers
			// hidden by an un-flipped schema flag" from "disk repaired".
			trackerStillThere := fileExists(migrationPath)
			ingestDirs := dirsWithPrefix(t, lsmPath, ingestPrefix)
			canonical := filepath.Join(lsmPath, helpers.BucketRangeableFromPropNameLSM(propName))

			require.Equal(t, tc.wantResidue, trackerStillThere,
				"tracker dir presence after restart")
			if tc.wantResidue {
				require.NotEmpty(t, ingestDirs, "a running task keeps its ingest dir")
			} else {
				require.Empty(t, ingestDirs, "ingest dir must not survive a terminal decision")
			}

			require.Equal(t, tc.wantPromoted, dirHasSegments(t, canonical),
				"the canonical rangeable dir must hold segments only when the migration was promoted")

			if tc.wantPromoted {
				require.True(t, fileExists(canonical),
					"an agreeing schema must get the migrated data promoted to canonical")
				bucket := shard2.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
				require.NotNil(t, bucket, "promoted rangeable bucket must load")
				require.NotEmpty(t, filterableToRangeableFingerprint(t, bucket),
					"promoted rangeable bucket must hold the migrated postings")
			}

			// The pre-migration data is never at risk: objects survive
			// every arm.
			objectCount := 0
			require.NoError(t, shard2.store.Bucket(helpers.ObjectsBucketLSM).
				IterateObjects(ctx, func(o *storobj.Object) error {
					objectCount++
					return nil
				}))
			require.Equal(t, numObjects, objectCount, "objects bucket must be intact")
		})
	}
}
