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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// Pins that a tenant set COLD while its swap is owed loses no write taken
// after it is reactivated: the reload arms the mirror, and the late swap
// installs a bucket that holds the write. A mirror that fails to arm holds the
// shard READONLY instead of stamping the record, since a stamp would refuse
// the owed swap for good, and so does a load before RAFT has caught up.
func TestWritesAfterReactivatingATenantThatOwesASwapSurviveTheSwap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		armFails bool
		// The load runs before this node's RAFT has caught up, so no task list is readable yet.
		raftBehind bool
	}{
		{name: "the mirror arms"},
		{name: "the mirror fails to arm", armFails: true},
		{name: "the mirror fails to arm before RAFT has caught up", armFails: true, raftBehind: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const prop, node = "title", "node1"
			ctx := testCtx()
			className := "ColdHot_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			class.Properties[0].IndexFilterable, class.Properties[0].IndexSearchable = boolPtr(true), boolPtr(true)
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			registerIndex(idx, className)
			require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, "seed")))

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(idx.db, nil, nil, logger, node, nil, ctx)
			idx.db.SetReindexUnitSeal(p.ReindexUnitSealBuilder())
			idx.shardReindexer = NewMigrationRecoveryReindexer(true, logger, nil, p)

			unitID, group := shard.migrationUnit(), shard.Name()
			payload := ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{prop},
				TargetTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
				UnitToShard: map[string]string{unitID: group}, UnitToNode: map[string]string{unitID: node},
			}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			task := func(status distributedtask.TaskStatus) *distributedtask.Task {
				return withUnits(&distributedtask.Task{
					Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
					Status: status, Payload: raw, NeedsPreparationBarrier: true,
					SwapDecidedGroups: map[string]time.Time{group: time.Now()}, SwapsOwed: map[string][]string{group: {node}},
				}, map[string]*distributedtask.Unit{unitID: {ID: unitID, GroupID: group, NodeID: node}})
			}
			p.processOneUnit(ctx, task(distributedtask.TaskStatusStarted), &payload, idx, unitID, newFakeRecorder())
			require.NoError(t, p.OnGroupCompleted(task(distributedtask.TaskStatusPreparing), group, []string{unitID}))
			cancelled := task(distributedtask.TaskStatusCancelled)
			if tc.raftBehind {
				idx.db.SetMigrationTaskSources(ctx,
					func(string) ([]distributedtask.TaskStatusEntry, bool) { return nil, false },
					func(context.Context) ([]*distributedtask.Task, error) { return nil, errors.New("RAFT is behind") })
			} else {
				installTestMigrationTaskSources(ctx, idx.db, nil, cancelled)
			}
			require.NoError(t, p.OnTaskCompleted(cancelled))

			mig := &Migrator{db: idx.db, logger: logger}
			setStatus := func(status string) {
				require.NoError(t, mig.UpdateTenants(ctx, class,
					[]*schemaUC.UpdateTenantPayload{{Name: group, Status: status}}, false))
			}
			setStatus(models.TenantActivityStatusCOLD)
			restore := func() {}
			if tc.armFails {
				restore = corruptIngestSegments(t, shard.pathLSM())
			}
			setStatus(models.TenantActivityStatusHOT)
			reloaded, err := unwrapShard(ctx, idx.shards.Load(group))
			require.NoError(t, err)
			defer reloaded.Shutdown(ctx)
			if tc.armFails {
				for _, rec := range reloaded.migrationRecords.Records() {
					require.False(t, rec.Subject().Unmirrored, "a stamp would refuse the owed swap for good")
				}
				require.Equal(t, storagestate.StatusReadOnly, reloaded.GetStatus())
				if tc.raftBehind {
					installTestMigrationTaskSources(ctx, idx.db, nil, cancelled)
				}
				restore()
				require.NoError(t, reloaded.UpdateStatus(storagestate.StatusReady.String(), statusReasonManualUpdate))
				require.Equal(t, storagestate.StatusReady, reloaded.GetStatus())
			}
			require.NotEmpty(t, p.cachedReindexTasks(cancelled.TaskDescriptor, unitID),
				"the owed swap reuses the instances the reload armed, though the task has ended")
			written := createTestObjectWithText(className, "afterhot")
			require.NoError(t, reloaded.PutObject(ctx, written))
			require.NoError(t, p.OnSwapRequested(cancelled, group, []string{unitID}))

			filtered, _, err := reloaded.ObjectSearch(ctx, 10, propEqualsFilter(className, prop, "afterhot"), nil, nil, nil, additional.Properties{}, nil)
			require.NoError(t, err)
			require.Contains(t, objectIDs(filtered), written.ID().String(), "the filter finds the write")
			ranked, _, err := reloaded.ObjectSearch(ctx, 10, nil,
				&searchparams.KeywordRanking{Type: "bm25", Properties: []string{prop}, Query: "afterhot"},
				nil, nil, additional.Properties{}, []string{prop})
			require.NoError(t, err)
			require.Contains(t, objectIDs(ranked), written.ID().String(), "and so does BM25")
		})
	}
}

// corruptIngestSegments puts an unreadable segment in every staged bucket, so
// the next load cannot arm the mirror, and returns what puts them back.
func corruptIngestSegments(t *testing.T, lsm string) func() {
	t.Helper()
	entries, err := os.ReadDir(lsm)
	require.NoError(t, err)
	var restores []func()
	for _, e := range entries {
		if !e.IsDir() || !strings.Contains(e.Name(), "_ingest") {
			continue
		}
		path := filepath.Join(lsm, e.Name(), "segment-1.db")
		original, readErr := os.ReadFile(path)
		existed := readErr == nil
		require.True(t, existed || errors.Is(readErr, os.ErrNotExist), "read %s: %v", path, readErr)
		require.NoError(t, os.WriteFile(path, []byte("not a segment"), 0o600))
		restores = append(restores, func() {
			if existed {
				require.NoError(t, os.WriteFile(path, original, 0o600))
			} else {
				require.NoError(t, os.Remove(path))
			}
		})
	}
	require.NotEmpty(t, restores, "a staged bucket to corrupt")
	return func() {
		for _, r := range restores {
			r()
		}
	}
}
