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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins that a tenant migrated ahead of the schema survives a reload, even
// across a crash between the swap's pointer flips and its record write.
func TestReloadKeepsATenantMigratedAheadOfTheSchema(t *testing.T) {
	const propName = "title"
	for _, tc := range []struct {
		name string
		// swap runs the live swap before the reload; otherwise the reload
		// stands in for a crash right after the pointer flips.
		swap    bool
		decided bool
		// wantMigrated: the reloaded tenant serves field tokenization.
		wantMigrated bool
		wantRecord   MigrationState
	}{
		{name: "swapped, then reloaded", swap: true, decided: true, wantMigrated: true, wantRecord: MigrationStatePromoted},
		{name: "crashed after the flips, tenant decided", decided: true, wantMigrated: true, wantRecord: MigrationStatePromoted},
		{name: "crashed before any decision", wantMigrated: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "Reload511_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			for _, obj := range makeConvergenceTestObjects(t, 25, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			pre := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)).Strategy()

			task, _ := newSearchableRetokenizeTask(t, idx, className, propName, models.PropertyTokenizationField, pre, shard.migrationUnit())
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeChangeTokenization, Properties: []string{propName},
				TargetTokenization: models.PropertyTokenizationField, BucketStrategy: pre,
			}
			desc := distributedtask.TaskDescriptor{ID: "T511", Version: 1}
			task.setMigrationIdentity(desc, shard.migrationUnit(), payload)
			maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task})
			flip := task.swapPropAtomic
			task.swapPropAtomic = func(ctx context.Context, store *lsmkv.Store, propIdx int, prop string) (*lsmkv.Bucket, error) {
				rec, _ := task.migrationRecord(shard)
				require.Equal(t, MigrationStateMerged, rec.State(), "no record write may precede the pointer flip")
				return flip(ctx, store, propIdx, prop)
			}
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			if tc.swap {
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
			}

			failed := withUnits(&distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: desc, Status: distributedtask.TaskStatusFailed,
			}, map[string]*distributedtask.Unit{shard.migrationUnit(): {ID: shard.migrationUnit(), GroupID: shard.Name()}})
			if tc.decided {
				failed.SwapDecidedGroups = map[string]time.Time{shard.Name(): {}}
			}
			idx.db.indices[indexID(idx.Config.ClassName)] = idx
			installTestMigrationTaskSources(ctx, idx.db, nil, failed)

			name := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			idx.shardReindexer = &noRecoveryTaskReindexer{}
			loaded, err := idx.initShard(ctx, name, class, nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(name, loaded)
			re := loaded.(*Shard)
			defer re.Shutdown(ctx)

			rec, present := task.migrationRecord(re)
			if tc.wantRecord == "" {
				require.False(t, present, "an undecided record is discarded")
			} else {
				require.True(t, present)
				require.Equal(t, tc.wantRecord, rec.State())
			}

			bm25 := func(q string) int {
				res, _, err := re.ObjectSearch(ctx, 100, nil,
					&searchparams.KeywordRanking{Type: "bm25", Properties: []string{propName}, Query: q},
					nil, nil, additional.Properties{}, nil)
				require.NoError(t, err)
				return len(res)
			}
			require.NoError(t, re.PutObject(ctx, createTestObjectWithText(className, "zulu one")))
			keys := fingerprintInvertedBucket(t, re.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))
			_, fieldTerm := keys["zulu one"]
			_, wordTerm := keys["zulu"]
			if tc.wantMigrated {
				require.Equal(t, models.PropertyTokenizationField, re.TokenizationFor(propName, models.PropertyTokenizationWord))
				require.True(t, fieldTerm && !wordTerm, "a write after the reload is indexed with the migrated tokenization")
				require.Equal(t, 1, bm25("zulu one"), "and a query is analyzed with it")
				return
			}
			require.Equal(t, models.PropertyTokenizationWord, re.TokenizationFor(propName, models.PropertyTokenizationWord))
			require.True(t, wordTerm && !fieldTerm, "a tenant that never migrated keeps the schema's tokenization")
		})
	}
}

// Pins that an index a tenant's migration turned on stands while its owning
// task may still flip the schema: the bucket survives a reload and the
// index-off sweeps, and takes writes. Once the task finished or is gone, the
// flip landed, so the schema's "off" is a drop and the bucket goes.
func TestReloadKeepsAnIndexATenantEnabledAheadOfTheSchema(t *testing.T) {
	const propName = "title"
	for _, tc := range []struct {
		name   string
		status distributedtask.TaskStatus // empty: the task is gone
		stands bool
	}{
		{name: "a retained failed run keeps it", status: distributedtask.TaskStatusFailed, stands: true},
		{name: "a finished run hands it to the schema", status: distributedtask.TaskStatusFinished},
		{name: "a run that is gone hands it to the schema"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "Enable511_" + uuid.NewString()[:8]
			class := newEnableSearchableTestClass(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			for _, obj := range makeConvergenceTestObjects(t, 25, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			task, _ := newEnableSearchableTask(t, idx, className, propName, models.PropertyTokenizationWord, shard.migrationUnit())
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeEnableSearchable, Properties: []string{propName},
				TargetTokenization: models.PropertyTokenizationWord,
			}
			desc := distributedtask.TaskDescriptor{ID: "T511e", Version: 1}
			task.setMigrationIdentity(desc, shard.migrationUnit(), payload)
			maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task})
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			idx.db.indices[indexID(idx.Config.ClassName)] = idx
			var tasks []*distributedtask.Task
			if tc.status != "" {
				tasks = append(tasks, &distributedtask.Task{Namespace: ReindexNamespace, TaskDescriptor: desc, Status: tc.status})
			}
			installTestMigrationTaskSources(ctx, idx.db, nil, tasks...)
			bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
			sweep := func(shard *Shard) {
				eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
				var counts migrationSweepCounts
				shard.updatePropertyBuckets(ctx, eg, class.Properties[0], &counts)
				require.NoError(t, eg.Wait())
				require.Equal(t, tc.stands, shard.store.Bucket(bucketName) != nil, "after a property update with the flag off")
			}
			sweep(shard)

			name := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			idx.shardReindexer = &noRecoveryTaskReindexer{}
			loaded, err := idx.initShard(ctx, name, class, nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(name, loaded)
			re := loaded.(*Shard)
			defer re.Shutdown(ctx)

			require.Equal(t, tc.stands, re.store.Bucket(bucketName) != nil, "after a reload")
			require.NoError(t, re.PutObject(ctx, createTestObjectWithText(className, "zulu one")))
			if tc.stands {
				rec, ok := task.migrationRecord(re)
				require.True(t, ok)
				require.Equal(t, MigrationStatePromoted, rec.State(), "the staged data is promoted even though the schema flag is off")
				_, indexed := fingerprintInvertedBucket(t, re.store.Bucket(bucketName))["zulu"]
				require.True(t, indexed, "a write after the reload reaches the migrated index")
			}
			sweep(re)
		})
	}
}
