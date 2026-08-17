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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestTheFirstWriteAfterARestartInTheWindowIsFoundOnceTheFlipLands is the
// case that decides where the routing has to come from.
//
// A node that restarts inside the window has already finished this shard's
// unit, so no task is dispatched to it and no scheduler tick is owed. The
// write here is issued against the shard the load returned, with nothing in
// between — so anything that armed the routing on a later event, however
// prompt, would miss it. Reading the record during shard init is what leaves
// no such gap.
//
// It ends on a query rather than a bucket dump because that is the promise:
// once the flip lands the object is findable by the value it was written
// with.
func TestTheFirstWriteAfterARestartInTheWindowIsFoundOnceTheFlipLands(t *testing.T) {
	const (
		propName   = "title"
		windowTerm = "zerogapterm"
	)
	ctx := testCtx()
	className := "ZeroGapWindowWrite_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)

	// Stopword detector wired: this test ends on a query, and the query path
	// tokenizes the filter value through it.
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		true, false, false)
	shard := shd.(*Shard)
	shardName := shard.Name()

	for _, obj := range makeConvergenceTestObjects(t, 10, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	task, _ := newEnableFilterableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.NoError(t, shard.Shutdown(ctx))

	// Nothing runs a reindex task on this load: the local unit is complete
	// and only the cluster-wide flag is outstanding.
	idx.shardReindexer = NewShardReindexerV3Noop()
	loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shardName, loaded)
	live := loaded.(*Shard)
	defer live.Shutdown(context.Background())

	windowID := strfmt.UUID(uuid.NewString())
	require.NoError(t, live.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         windowID,
			Class:      className,
			Properties: map[string]interface{}{propName: windowTerm},
		},
	}))

	require.False(t, *class.Properties[0].IndexFilterable,
		"the write above has to happen while the schema still hides the index")
	class.Properties[0].IndexFilterable = boolPtr(true)

	found, _, err := live.ObjectSearch(ctx, 10,
		equalsFilter(className, propName, windowTerm), nil, nil, nil,
		additional.Properties{}, []string{propName})
	require.NoError(t, err)
	require.Len(t, found, 1,
		"the object written in the window is not findable by its own value")
	assert.Equal(t, windowID, found[0].ID())
}

// TestARetokenizedShardKeepsAnalyzingUnderItsOwnKeysAcrossARestart covers the
// other half of the window: a migration that changes no index flag but
// rewrites a property's keys under a new tokenization.
//
// Nothing on disk says which tokenization the bucket holds — the schema still
// names the old one, and after a restart the in-memory overlay that bridged
// that is gone. A write then lands under keys the flipped schema will never
// look for, and a query looks under keys the bucket does not hold. The record
// is what carries the answer across the restart.
//
// Both halves of the migration are covered: they rewrite different buckets and
// are read back by different query paths, and the tokenization is what both of
// them resolve the keys with.
func TestARetokenizedShardKeepsAnalyzingUnderItsOwnKeysAcrossARestart(t *testing.T) {
	const (
		propName = "title"
		// Two words, so word and field tokenization disagree about the keys
		// this object lands under.
		windowText = "alpha bravo"
	)

	halves := []struct {
		name    string
		newTask func(t *testing.T, idx *Index, className string) (*ShardReindexTaskGeneric, MigrationStrategy)
		// search reads the window write back the way that half is queried.
		search func(t *testing.T, ctx context.Context, shard *Shard, className string) []*storobj.Object
	}{
		{
			name: "filterable",
			newTask: func(t *testing.T, idx *Index, className string) (*ShardReindexTaskGeneric, MigrationStrategy) {
				task, wrapped := newFilterableRetokenizeTask(t, idx, className, propName,
					models.PropertyTokenizationField)
				return task, wrapped
			},
			search: func(t *testing.T, ctx context.Context, shard *Shard, className string) []*storobj.Object {
				found, _, err := shard.ObjectSearch(ctx, 10,
					equalsFilter(className, propName, windowText), nil, nil, nil,
					additional.Properties{}, []string{propName})
				require.NoError(t, err)
				return found
			},
		},
		{
			name: "searchable",
			newTask: func(t *testing.T, idx *Index, className string) (*ShardReindexTaskGeneric, MigrationStrategy) {
				task, wrapped := newSearchableRetokenizeTask(t, idx, className, propName,
					models.PropertyTokenizationField, lsmkv.StrategyMapCollection)
				return task, wrapped
			},
			search: func(t *testing.T, ctx context.Context, shard *Shard, className string) []*storobj.Object {
				found, _, err := shard.ObjectSearch(ctx, 10, nil,
					&searchparams.KeywordRanking{
						Type: "bm25", Properties: []string{propName}, Query: windowText,
					}, nil, nil, additional.Properties{}, []string{propName})
				require.NoError(t, err)
				return found
			},
		},
	}

	for _, half := range halves {
		t.Run(half.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RetokenizeWindowWrite_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			require.Equal(t, models.PropertyTokenizationWord, class.Properties[0].Tokenization)

			// Stopword detector wired: this test ends on a query, and the
			// query path tokenizes its input through it.
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				true, false, false)
			shard := shd.(*Shard)
			shardName := shard.Name()
			lsmPath := shard.pathLSM()

			for _, obj := range makeConvergenceTestObjects(t, 10, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			task, strategy := half.newTask(t, idx, className)
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))
			// What the provider persists per shard before it dispatches the
			// task; the test drives the task directly, so it stands in for
			// that write.
			writeRecoveryPayload(t, lsmPath, strategy.MigrationDirName(),
				[]string{propName}, models.PropertyTokenizationField)
			require.NoError(t, shard.Shutdown(ctx))

			idx.shardReindexer = NewShardReindexerV3Noop()
			loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(shardName, loaded)
			live := loaded.(*Shard)
			defer live.Shutdown(context.Background())

			assert.Equal(t, models.PropertyTokenizationField,
				live.TokenizationFor(propName, class.Properties[0].Tokenization),
				"the shard has to answer from its own keys, not from the schema that has yet to catch up")

			windowID := strfmt.UUID(uuid.NewString())
			require.NoError(t, live.PutObject(ctx, &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:         windowID,
					Class:      className,
					Properties: map[string]interface{}{propName: windowText},
				},
			}))

			class.Properties[0].Tokenization = models.PropertyTokenizationField
			found := half.search(t, ctx, live, className)
			require.Len(t, found, 1,
				"the object written in the window is not findable by its own value")
			assert.Equal(t, windowID, found[0].ID())
		})
	}
}

func equalsFilter(className, propName, value string) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On: &filters.Path{
			Class:    schema.ClassName(className),
			Property: schema.PropertyName(propName),
		},
		Value: &filters.Value{Value: value, Type: schema.DataTypeText},
	}}
}
