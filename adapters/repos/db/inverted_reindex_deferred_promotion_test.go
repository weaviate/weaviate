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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A restart between the swap ack and the schema flip must keep serving the migrated data across the flip.
func TestRestartBeforeTheSchemaFlipServesTheMigratedData(t *testing.T) {
	const (
		propName   = "title"
		numObjects = 25
		token      = "alpha"
		lateToken  = "zulu"
	)
	want := 0
	for i := 0; i < numObjects; i++ {
		for k := 0; k < 3; k++ {
			if convergenceTokens[(i+k)%len(convergenceTokens)] == token {
				want++
				break
			}
		}
	}

	for i, tc := range []struct {
		name     string
		newClass func(className string) *models.Class
		newTask  func(t *testing.T, idx *Index, className, unitID string) *ShardReindexTaskGeneric
		flip     func(prop *models.Property)
		count    func(ctx context.Context, s *Shard, className, tok string) (int, error)
	}{
		{
			name: "enable filterable",
			newClass: func(className string) *models.Class {
				return newEnableFilterableTestClass(className, propName)
			},
			newTask: func(t *testing.T, idx *Index, className, unitID string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, unitID, propName)
				return task
			},
			flip: func(prop *models.Property) { prop.IndexFilterable = boolPtr(true) },
			count: func(ctx context.Context, s *Shard, className, tok string) (int, error) {
				res, _, err := s.ObjectSearch(ctx, 100, &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On:       &filters.Path{Class: schema.ClassName(className), Property: propName},
					Value:    &filters.Value{Value: tok, Type: schema.DataTypeText},
				}}, nil, nil, nil, additional.Properties{}, nil)
				return len(res), err
			},
		},
		{
			name: "enable searchable",
			newClass: func(className string) *models.Class {
				return newEnableSearchableTestClass(className, []string{propName})
			},
			newTask: func(t *testing.T, idx *Index, className, unitID string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, propName,
					models.PropertyTokenizationWord, unitID)
				return task
			},
			flip: func(prop *models.Property) {
				prop.IndexSearchable = boolPtr(true)
				prop.Tokenization = models.PropertyTokenizationWord
				prop.SearchableBlockmax = boolPtr(true)
			},
			count: func(ctx context.Context, s *Shard, _, tok string) (int, error) {
				res, _, err := s.ObjectSearch(ctx, 100, nil, &searchparams.KeywordRanking{
					Type: "bm25", Properties: []string{propName}, Query: tok,
				}, nil, nil, additional.Properties{}, nil)
				return len(res), err
			},
		},
	} {
		for _, sc := range []struct {
			name string
			// Shard start reads the class before the flip; the after-load hook runs after it.
			flipDuringLoad bool
			// Only an unmirrored record refuses promotion over a canonical dir left on disk.
			unmirrored bool
			// With its task gone, only the record shows the swap; a stamp would block its promotion, so the load holds.
			armFailsTaskGone bool
		}{
			{name: "flip after the load"},
			{name: "flip during the load", flipDuringLoad: true},
			{name: "unmirrored record", unmirrored: true},
			{name: "mirror fails to arm after the task is gone", armFailsTaskGone: true},
		} {
			// The arm-failure path has no per-strategy branch, so one strategy covers it.
			if sc.armFailsTaskGone && i > 0 {
				continue
			}
			t.Run(tc.name+"/"+sc.name, func(t *testing.T) {
				ctx := testCtx()
				className := "DeferredPromotion_" + uuid.NewString()[:8]
				class := tc.newClass(className)
				// Stopwords on: a word-tokenized filter needs a detector.
				shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
				shard := shd.(*Shard)
				for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
					require.NoError(t, shard.PutObject(ctx, obj))
				}

				task := tc.newTask(t, idx, className, shard.migrationUnit())
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
				require.Equal(t, []MigrationState{MigrationStateSwapped}, migrationRecordStates(t, shard.pathLSM()))

				if sc.unmirrored {
					rec, ok := task.migrationRecord(shard)
					require.True(t, ok)
					stamped, ok := migrationRecordStampedUnmirrored(rec)
					require.True(t, ok)
					require.NoError(t, shard.migrationRecords.Put(stamped))
				}

				name, lsm := shard.Name(), shard.pathLSM()
				require.NoError(t, shard.Shutdown(ctx))
				if sc.armFailsTaskGone {
					idx.db.migrationCluster.local = func(string) ([]distributedtask.TaskStatusEntry, bool) { return nil, true }
					corrupt := filepath.Join(lsm, task.ingestBucketName(propName), "segment-1.db")
					require.NoError(t, os.WriteFile(corrupt, []byte("not a segment"), 0o600))
				}
				p := NewReindexProvider(idx.db, nil, nil, idx.logger, idx.getSchema.NodeName(), nil, ctx)
				idx.shardReindexer = NewMigrationRecoveryReindexer(idx.logger, nil, p)
				loadClass := class
				if sc.flipDuringLoad {
					snapshot := *class
					prop := *class.Properties[0]
					snapshot.Properties = []*models.Property{&prop}
					loadClass = &snapshot
					tc.flip(class.Properties[0])
				}
				shard = openShardFromDisk(t, ctx, idx, loadClass, name)
				if sc.armFailsTaskGone {
					defer shard.Shutdown(context.Background())
					require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
					rec, ok := task.migrationRecord(shard)
					require.True(t, ok)
					require.Equal(t, MigrationStateSwapped, rec.State())
					require.False(t, rec.Subject().Unmirrored, "a stamp would block the promotion of a swapped-in record")
					return
				}
				require.Len(t, p.cachedReindexTasks(distributedtask.TaskDescriptor{ID: task.taskID, Version: task.taskVersion},
					task.unitID), 1, "the load must recover the migration")
				require.Equal(t, []MigrationState{MigrationStatePromoted}, migrationRecordStates(t, lsm),
					"a decided swap promotes at load, while the schema has not flipped yet")

				require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, lateToken)))

				if !sc.flipDuringLoad {
					tc.flip(class.Properties[0])
				}
				require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))

				assertServed := func(s *Shard, when string) {
					t.Helper()
					n, err := tc.count(ctx, s, className, token)
					require.NoError(t, err)
					require.Equalf(t, want, n, "%s: objects written before the migration", when)
					n, err = tc.count(ctx, s, className, lateToken)
					require.NoError(t, err)
					require.Equalf(t, 1, n, "%s: the object written after the restart", when)
				}
				assertServed(shard, "after the flip")

				shard = reloadShardFromDisk(t, ctx, idx, shard, class)
				defer shard.Shutdown(context.Background())
				require.Empty(t, migrationRecordStates(t, lsm),
					"the next load retires the record once the schema shows its effect")
				assertServed(shard, "after the next load")
			})
		}
	}
}

// A reload keeps the record of a swap a failed run decided, and a swapped
// tenant keeps serving its migrated tokenization while the schema lags.
func TestReloadKeepsATenantMigratedAheadOfTheSchema(t *testing.T) {
	const propName = "title"
	for _, tc := range []struct {
		name string
		// swap runs the live swap before the reload; otherwise the reload
		// stands in for a crash before the swap began.
		swap       bool
		wantRecord MigrationState
	}{
		{name: "swapped, then reloaded", swap: true, wantRecord: MigrationStatePromoted},
		{name: "crashed before the swap, tenant decided", wantRecord: MigrationStateMerged},
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
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			if tc.swap {
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
			}

			failed := &distributedtask.Task{
				Namespace: ReindexNamespace, TaskDescriptor: desc, Status: distributedtask.TaskStatusFailed,
				SwapDecidedGroups: map[string]time.Time{shard.Name(): {}},
				Units:             map[string]*distributedtask.Unit{shard.migrationUnit(): {ID: shard.migrationUnit(), GroupID: shard.Name()}},
			}
			failed.IndexGroups()
			idx.db.indices[indexID(idx.Config.ClassName)] = idx
			installTestMigrationTaskSources(ctx, idx.db, nil, failed)

			require.NoError(t, shard.Shutdown(ctx))
			idx.shardReindexer = &noRecoveryTaskReindexer{}
			re := openShardFromDisk(t, ctx, idx, class, shard.Name())
			defer re.Shutdown(ctx)

			rec, present := task.migrationRecord(re)
			require.True(t, present)
			require.Equal(t, tc.wantRecord, rec.State())

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
			if tc.swap {
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
			re := openShardFromDisk(t, ctx, idx, class, name)

			require.Equal(t, tc.stands, re.store.Bucket(bucketName) != nil, "after a reload")
			require.NoError(t, re.PutObject(ctx, createTestObjectWithText(className, "zulu one")))
			if tc.stands {
				rec, ok := task.migrationRecord(re)
				require.True(t, ok)
				require.Equal(t, MigrationStatePromoted, rec.State(), "the staged data is promoted even though the schema flag is off")
				_, indexed := fingerprintInvertedBucket(t, re.store.Bucket(bucketName))["zulu"]
				require.True(t, indexed, "a write after the reload reaches the migrated index")
			}

			// The same update reaching the shard unloaded, its data now at the canonical name.
			require.NoError(t, re.Shutdown(ctx))
			lazy := NewLazyLoadShard(ctx, nil, name, idx, class, idx.centralJobQueue, idx.indexCheckpoints,
				idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer, false, idx.bitmapBufPool)
			idx.shards.Store(name, lazy)
			eg := enterrors.NewErrorGroupWrapper(idx.logger)
			lazy.updatePropertyBuckets(ctx, eg, class.Properties[0], &migrationSweepCounts{})
			require.NoError(t, eg.Wait())
			require.NoError(t, lazy.Load(ctx))
			defer lazy.Shutdown(ctx)
			if tc.stands {
				_, kept := fingerprintInvertedBucket(t, lazy.shard.store.Bucket(bucketName))["zulu"]
				require.True(t, kept, "an update while unloaded keeps the migrated index's data")
			}
			sweep(lazy.shard)
		})
	}
}

// Pins that a boot load, which cannot read the task list yet, keeps an enabled
// index only until the list is readable: then a finished run hands it to the
// schema, so the next boot load sweeps it, while a retained failed run keeps it.
func TestBootLoadReDecidesAnEnabledIndexOnceTheTaskListIsReadable(t *testing.T) {
	const propName = "title"
	for _, tc := range []struct {
		name   string
		status distributedtask.TaskStatus
		stands bool
	}{
		{name: "a finished run hands it to the schema", status: distributedtask.TaskStatusFinished},
		{name: "a retained failed run keeps it", status: distributedtask.TaskStatusFailed, stands: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "BootEnable_" + uuid.NewString()[:8]
			class := newEnableSearchableTestClass(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			task, _ := newEnableSearchableTask(t, idx, className, propName, models.PropertyTokenizationWord, shard.migrationUnit())
			payload := &ReindexTaskPayload{
				Collection: className, MigrationType: ReindexTypeEnableSearchable, Properties: []string{propName},
				TargetTokenization: models.PropertyTokenizationWord,
			}
			desc := distributedtask.TaskDescriptor{ID: "TBoot", Version: 1}
			task.setMigrationIdentity(desc, shard.migrationUnit(), payload)
			maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task})
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))
			idx.db.indices[indexID(idx.Config.ClassName)] = idx
			idx.shardReindexer = &noRecoveryTaskReindexer{}
			bucketName := helpers.BucketSearchableFromPropNameLSM(propName)

			bootLoad := func(s *Shard) *Shard {
				idx.db.migrationCluster.mu.Lock()
				idx.db.migrationCluster.local = nil
				idx.db.migrationCluster.mu.Unlock()
				require.NoError(t, s.Shutdown(ctx))
				return openShardFromDisk(t, ctx, idx, class, s.Name())
			}
			forced := func(s *Shard) bool {
				return s.SnapshotPropertyOverlay([]string{propName})[propName].ForceSearchable
			}

			booted := bootLoad(shard)
			require.True(t, forced(booted), "an unreadable task list keeps the index on")
			installTestMigrationTaskSources(ctx, idx.db, nil,
				&distributedtask.Task{Namespace: ReindexNamespace, TaskDescriptor: desc, Status: tc.status})
			_, kept := task.migrationRecord(booted)
			require.Equal(t, tc.stands, kept, "the record once the task list is readable")
			require.Equal(t, tc.stands, forced(booted), "the overlay once the task list is readable")

			rebooted := bootLoad(booted)
			defer rebooted.Shutdown(ctx)
			require.Equal(t, tc.stands, rebooted.store.Bucket(bucketName) != nil, "the bucket after the next boot load")
		})
	}
}
