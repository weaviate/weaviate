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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// An unreadable record elsewhere must not block queries on an already-migrated property.
func TestFrozenBootServesSwappedMigration(t *testing.T) {
	tests := []struct {
		name  string
		class func(string) *models.Class
		task  func(t *testing.T, idx *Index, className, unit string) *ShardReindexTaskGeneric
		query func(ctx context.Context, s *Shard, className string) (int, error)
	}{
		{
			name:  "bm25 after rebuild searchable",
			class: func(c string) *models.Class { return newRebuildSearchableTestClass(c, []string{"title"}) },
			task: func(t *testing.T, idx *Index, c, u string) *ShardReindexTaskGeneric {
				task, _ := newRebuildSearchableTask(t, idx, c, "title", u)
				return task
			},
			query: func(ctx context.Context, s *Shard, _ string) (int, error) {
				found, _, err := s.ObjectSearch(ctx, 100, nil,
					&searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "alpha"},
					nil, nil, additional.Properties{}, []string{"title"})
				return len(found), err
			},
		},
		{
			name:  "equal filter after filterable refresh",
			class: func(c string) *models.Class { return newTestClassWithProps(c, []string{"title"}) },
			task: func(t *testing.T, idx *Index, _, u string) *ShardReindexTaskGeneric {
				task, _ := newRoaringSetRefreshTask(t, idx, u)
				return task
			},
			query: func(ctx context.Context, s *Shard, c string) (int, error) {
				found, _, err := s.ObjectSearch(ctx, 100, propEqualsFilter(c, "title", "alpha"),
					nil, nil, nil, additional.Properties{}, nil)
				return len(found), err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "Frozen_" + uuid.NewString()[:8]
			class := tc.class(className)
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			for _, obj := range makeConvergenceTestObjects(t, 25, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			task := tc.task(t, idx, className, shard.migrationUnit())
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))
			want, err := tc.query(ctx, shard, className)
			require.NoError(t, err)
			require.NotZero(t, want)

			bad := filepath.Join(shard.migrationRecords.Dir(),
				"99_searchable_retokenize_"+shard.migrationUnit()+".json")
			require.NoError(t, os.WriteFile(bad, []byte("{"), 0o600))

			name := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			idx.shardReindexer = &testShardReindexer{task: tc.task(t, idx, className, testMigrationUnitFor(idx, name))}
			reloaded, err := idx.initShard(ctx, name, class, nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(name, reloaded)
			defer reloaded.Shutdown(ctx)
			require.NotEmpty(t, reloaded.(*Shard).migrationRecords.Unreadable())

			got, err := tc.query(ctx, reloaded.(*Shard), className)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}
