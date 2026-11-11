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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A restart between the swap ack and the schema flip must keep serving staged data until promotion catches up.
func TestDeferredPromotionServesStagedDataAcrossTheFlip(t *testing.T) {
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
			// A stamp would block the promotion, so the load holds the shard until the mirror arms.
			armFails bool
		}{
			{name: "flip after the load"},
			{name: "flip during the load", flipDuringLoad: true},
			{name: "unmirrored record", unmirrored: true},
			{name: "mirror fails to arm at the load", armFails: true},
		} {
			// The arm-failure path has no per-strategy branch, so one strategy covers it.
			if sc.armFails && i > 0 {
				continue
			}
			t.Run(tc.name+"/"+sc.name, func(t *testing.T) {
				ctx := testCtx()
				className := "DeferredPromotion_" + uuid.NewString()[:8]
				class := tc.newClass(className)
				// Stopwords on: a word-tokenized filter needs a detector.
				shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false)
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
				corrupt := filepath.Join(lsm, task.ingestBucketName(propName), "segment-1.db")
				if sc.armFails {
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
				if sc.armFails {
					require.Equal(t, storagestate.StatusReadOnly, shard.GetStatus())
					rec, ok := task.migrationRecord(shard)
					require.True(t, ok)
					require.False(t, rec.Subject().Unmirrored)
					require.NoError(t, os.Remove(corrupt))
					require.ErrorContains(t, liftReady(shard)(), "a recovery retry started")
					awaitRetry(t, shard)
				}
				require.Equal(t, []MigrationState{MigrationStateSwapped}, migrationRecordStates(t, lsm),
					"the unflipped schema must defer the promotion")

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
				require.Equal(t, []MigrationState{MigrationStatePromoted}, migrationRecordStates(t, lsm),
					"the next load must promote the staged data")
				assertServed(shard, "after the next load")
			})
		}
	}
}
