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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const legacyObjects = 20

// The marker set a v1.39 node leaves once its migration's task FINISHED,
// taken from a real run; mergedOnly is a swap that never completed.
var (
	legacyFinishedMarkers = []string{
		"started.mig", "reindexed.mig", "prepended.mig", "merged.mig", "swapped.mig", "tidied.mig",
	}
	legacyMergedOnlyMarkers = []string{"started.mig", "reindexed.mig", "prepended.mig", "merged.mig"}
)

// Each row plants the on-disk state a v1.39 node leaves after a FINISHED
// migration: the data under the staged name, no canonical directory, and the
// tracker. Verified against a real v1.39 run for every code except map to
// blockmax, which that run could not start.
func TestFinalizeLegacyMigrations(t *testing.T) {
	tests := []struct {
		name    string
		code    MigrationStrategyCode
		props   []string
		markers []string
		// alreadyMoved are properties a crashed earlier pass already renamed.
		alreadyMoved []string
		// disables turns the migrated index off, as before a cluster commit.
		disables  bool
		wantMoved bool
	}{
		{"map to blockmax", StrategyCodeSearchableMapToBlockmax, []string{"title"}, legacyFinishedMarkers, nil, false, true},
		{"repair filterable", StrategyCodeFilterableRoaringsetRefresh, []string{"title"}, legacyFinishedMarkers, nil, false, true},
		{"enable or repair rangeable", StrategyCodeFilterableToRangeable, []string{"score"}, legacyFinishedMarkers, nil, false, true},
		{"retokenize searchable", StrategyCodeSearchableRetokenize, []string{"title"}, legacyFinishedMarkers, nil, false, true},
		{"retokenize filterable", StrategyCodeFilterableRetokenize, []string{"title"}, legacyFinishedMarkers, nil, false, true},
		{"enable filterable", StrategyCodeEnableFilterable, []string{"title"}, legacyFinishedMarkers, nil, false, true},
		{"enable searchable", StrategyCodeEnableSearchable, []string{"title"}, legacyFinishedMarkers, nil, false, true},
		{"rebuild searchable", StrategyCodeRebuildSearchable, []string{"title"}, legacyFinishedMarkers, nil, false, true},
		{"crash after the first of two renames", StrategyCodeEnableFilterable, []string{"body", "title"}, legacyFinishedMarkers, []string{"body"}, false, true},
		{"swap still in progress", StrategyCodeFilterableRetokenize, []string{"title"}, legacyMergedOnlyMarkers, nil, false, false},
		{"index not yet enabled by the schema", StrategyCodeEnableFilterable, []string{"title"}, legacyFinishedMarkers, nil, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			class := newLegacyFinalizeClass("LegacyFinalize_" + uuid.NewString()[:8])
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
			shard := shd.(*Shard)
			for i := 0; i < legacyObjects; i++ {
				require.NoError(t, shard.PutObject(ctx, newLegacyFinalizeObject(class.Class, i)))
			}
			require.NoError(t, shard.Shutdown(ctx))

			lsm := shard.pathLSM()
			layout := legacyMigrationLayouts[tt.code]
			tracker := filepath.Join(lsm, migrationsDir, fmt.Sprintf("%s_%s_7", tt.code, strings.Join(tt.props, "_")))
			require.NoError(t, os.MkdirAll(tracker, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(tracker, "properties.mig"), []byte(strings.Join(tt.props, ",")), 0o644))
			for _, marker := range tt.markers {
				require.NoError(t, os.WriteFile(filepath.Join(tracker, marker), nil, 0o644))
			}
			staged := map[string]string{}
			for _, prop := range tt.props {
				canonical := layout.canonical(prop)
				staged[prop] = canonical + layout.staged + "_7"
				if !slices.Contains(tt.alreadyMoved, prop) {
					require.NoError(t, os.Rename(filepath.Join(lsm, canonical), filepath.Join(lsm, staged[prop])))
				}
			}
			if tt.disables {
				class.Properties[0].IndexFilterable = boolPtr(false)
			}

			reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
			require.NoError(t, err)
			shard = reloaded.(*Shard)
			t.Cleanup(func() { shard.Shutdown(context.Background()) })

			_, err = os.Stat(tracker)
			if !tt.wantMoved {
				require.NoError(t, err, "a migration that may not have finished must keep its tracker")
				for _, dir := range staged {
					require.DirExists(t, filepath.Join(lsm, dir), "and its staged data")
				}
				return
			}
			require.ErrorIs(t, err, os.ErrNotExist, "the markers go once the move is done, so it runs once")
			for _, prop := range tt.props {
				require.NoDirExists(t, filepath.Join(lsm, staged[prop]))
				require.Lenf(t, legacyFinalizeSearch(t, ctx, shard, class.Class, tt.code, prop), legacyObjects,
					"property %q must serve every object", prop)
			}
		})
	}
}

func newLegacyFinalizeClass(className string) *models.Class {
	text := func(name string) *models.Property {
		return &models.Property{
			Name: name, DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord,
			IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true),
		}
	}
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			UsingBlockMaxWAND:      true,
		},
		Properties: []*models.Property{
			text("title"), text("body"),
			{
				Name: "score", DataType: schema.DataTypeInt.PropString(),
				IndexFilterable: boolPtr(true), IndexRangeFilters: boolPtr(true),
			},
		},
	}
}

func newLegacyFinalizeObject(className string, i int) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]any{
				"title": fmt.Sprintf("apple %d", i), "body": fmt.Sprintf("apple %d", i), "score": int64(i),
			},
		},
	}
}

// Queries through the index the migration rebuilt, so an empty canonical
// bucket answers with nothing.
func legacyFinalizeSearch(t *testing.T, ctx context.Context, shard *Shard, className string,
	code MigrationStrategyCode, prop string,
) []*storobj.Object {
	t.Helper()
	var (
		filter  *filters.LocalFilter
		keyword *searchparams.KeywordRanking
	)
	on := &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(prop)}
	switch legacyMigrationLayouts[code].canonical(prop) {
	case helpers.BucketSearchableFromPropNameLSM(prop):
		keyword = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{prop}, Query: "apple"}
	case helpers.BucketRangeableFromPropNameLSM(prop):
		filter = &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorGreaterThanEqual, On: on,
			Value: &filters.Value{Value: 0, Type: schema.DataTypeInt},
		}}
	default:
		filter = &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual, On: on,
			Value: &filters.Value{Value: "apple", Type: schema.DataTypeText},
		}}
	}
	found, _, err := shard.ObjectSearch(ctx, 2*legacyObjects, filter, keyword, nil, nil,
		additional.Properties{}, nil)
	require.NoError(t, err)
	return found
}
