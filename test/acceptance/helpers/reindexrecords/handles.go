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

package reindexrecords

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

type Handles struct {
	Canonical string
	Staged    string
	Sidecar   string
}

func HandlesFor(t *testing.T, code db.MigrationStrategyCode, propName string, generation int) Handles {
	t.Helper()

	recipe, ok := handleRecipes[code]
	require.Truef(t, ok, "no directory recipe for migration strategy %q", code)

	main := recipe.bucket(propName)
	gen := "_" + strconv.Itoa(generation)
	return Handles{
		Canonical: main,
		Staged:    main + recipe.ingestSuffix + gen,
		Sidecar:   main + recipe.reindexSuffix + gen,
	}
}

type handleRecipe struct {
	bucket        func(propName string) string
	ingestSuffix  string
	reindexSuffix string
}

var handleRecipes = map[db.MigrationStrategyCode]handleRecipe{
	db.StrategyCodeSearchableMapToBlockmax: {
		helpers.BucketSearchableFromPropNameLSM, "__blockmax_ingest", "__blockmax_reindex",
	},
	db.StrategyCodeFilterableRoaringsetRefresh: {
		helpers.BucketFromPropNameLSM, "__roaringset_ingest", "__roaringset_reindex",
	},
	db.StrategyCodeFilterableToRangeable: {
		helpers.BucketRangeableFromPropNameLSM, "__rangeable_ingest", "__rangeable_reindex",
	},
	db.StrategyCodeSearchableRetokenize: {
		helpers.BucketSearchableFromPropNameLSM, "__retokenize_ingest", "__retokenize_reindex",
	},
	db.StrategyCodeFilterableRetokenize: {
		helpers.BucketFromPropNameLSM, "__filt_retokenize_ingest", "__filt_retokenize_reindex",
	},
	db.StrategyCodeEnableFilterable: {
		helpers.BucketFromPropNameLSM, "__enable_filterable_ingest", "__enable_filterable_reindex",
	},
	db.StrategyCodeEnableSearchable: {
		helpers.BucketSearchableFromPropNameLSM, "__enable_searchable_ingest", "__enable_searchable_reindex",
	},
	db.StrategyCodeRebuildSearchable: {
		helpers.BucketSearchableFromPropNameLSM, "__rebuild_searchable_ingest", "__rebuild_searchable_reindex",
	},
}
