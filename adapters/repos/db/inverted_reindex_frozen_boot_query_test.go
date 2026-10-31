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
			require.NotEmpty(t, reloaded.(*Shard).migrationRecords.Unreadable())

			got, err := tc.query(ctx, reloaded.(*Shard), className)
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.NoError(t, reloaded.Shutdown(ctx))

			require.NoError(t, os.Remove(bad))
			idx.shardReindexer = &testShardReindexer{task: tc.task(t, idx, className, testMigrationUnitFor(idx, name))}
			promoted, err := idx.initShard(ctx, name, class, nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(name, promoted)
			defer promoted.Shutdown(ctx)
			rec, ok := promoted.(*Shard).migrationRecords.Get(task.migrationRecordKey())
			require.True(t, ok)
			require.Equal(t, MigrationStatePromoted, rec.State())
			got, err = tc.query(ctx, promoted.(*Shard), className)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// The staged copy is not the latest data, so a frozen boot must keep serving the canonical directory.
func TestFrozenBootKeepsCanonicalData(t *testing.T) {
	bm25 := func(ctx context.Context, s *Shard, query string) int {
		found, _, err := s.ObjectSearch(ctx, 100, nil,
			&searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: query},
			nil, nil, additional.Properties{}, []string{"title"})
		require.NoError(t, err)
		return len(found)
	}
	tests := []struct {
		name  string
		query string
		// arrange returns the hit count the canonical directory alone should produce.
		arrange func(t *testing.T, ctx context.Context, s *Shard, boot func(ShardReindexerV3) *Shard) int
	}{
		{
			name:  "unmirrored writes reached only the canonical directory",
			query: "betaword",
			arrange: func(t *testing.T, ctx context.Context, s *Shard, boot func(ShardReindexerV3) *Shard) int {
				stamped, ok := migrationRecordStampedUnmirrored(s.migrationRecords.Records()[0])
				require.True(t, ok)
				require.NoError(t, s.migrationRecords.Put(stamped))
				require.NoError(t, s.Shutdown(ctx))

				unmirrored := boot(NewShardReindexerV3Noop())
				require.NoError(t, unmirrored.PutObject(ctx, createTestObjectWithText(s.index.Config.ClassName.String(), "betaword")))
				require.NoError(t, unmirrored.Shutdown(ctx))
				return 1
			},
		},
		{
			name:  "a newer migration already promoted into the canonical directory",
			query: "alpha",
			arrange: func(t *testing.T, ctx context.Context, s *Shard, _ func(ShardReindexerV3) *Shard) int {
				want := bm25(ctx, s, "alpha")
				older := s.migrationRecords.Records()[0].Subject()
				dirs := older.Props["title"]
				newer := older
				newer.Key.TaskVersion++
				newer.TaskID += "-newer"
				newer.Props = map[string]MigrationPropertyDirs{"title": {
					Staged: dirs.Canonical + "__newer_ingest", Sidecar: dirs.Canonical + "__newer_reindex", Canonical: dirs.Canonical,
				}}
				require.NoError(t, s.migrationRecords.Put(NewMigrationRecordPromoted(newer, []string{"title"},
					map[string]string{"title": dirs.Canonical})))
				require.NoError(t, s.Shutdown(ctx))

				canonical := filepath.Join(s.pathLSM(), dirs.Canonical)
				require.NoError(t, os.RemoveAll(canonical))
				require.NoError(t, os.CopyFS(canonical, os.DirFS(filepath.Join(s.pathLSM(), dirs.Staged))))
				return want
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "Frozen_" + uuid.NewString()[:8]
			class := newRebuildSearchableTestClass(className, []string{"title"})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			for _, obj := range makeConvergenceTestObjects(t, 25, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			name := shard.Name()
			newTask := func() *ShardReindexTaskGeneric {
				task, _ := newRebuildSearchableTask(t, idx, className, "title", testMigrationUnitFor(idx, name))
				return task
			}
			task := newTask()
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))
			canonical := shard.migrationRecords.Records()[0].Subject().Props["title"].Canonical
			boot := func(reindexer ShardReindexerV3) *Shard {
				idx.shardReindexer = reindexer
				reloaded, err := idx.initShard(ctx, name, class, nil, true, true)
				require.NoError(t, err)
				idx.shards.Store(name, reloaded)
				return reloaded.(*Shard)
			}

			bad := filepath.Join(shard.migrationRecords.Dir(), "99_searchable_retokenize_"+shard.migrationUnit()+".json")
			require.NoError(t, os.WriteFile(bad, []byte("{"), 0o600))
			want := tc.arrange(t, ctx, shard, boot)

			frozen := boot(&testShardReindexer{task: newTask()})
			require.NotEmpty(t, frozen.migrationRecords.Unreadable())
			require.Equal(t, want, bm25(ctx, frozen, tc.query))
			require.Equal(t, canonical, filepath.Base(frozen.store.Bucket(canonical).GetDir()))
			require.NoError(t, frozen.Shutdown(ctx))

			require.NoError(t, os.Remove(bad))
			cleared := boot(&testShardReindexer{task: newTask()})
			defer cleared.Shutdown(ctx)
			require.Equal(t, want, bm25(ctx, cleared, tc.query))
		})
	}
}
