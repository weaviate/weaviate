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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// Handles are the directories one migration owns for one property.
type Handles struct {
	Canonical string
	Staged    string
	Sidecar   string
}

// HandlesFor names the directories the writer emits for propName under the
// strategy that code identifies, at the given migration generation.
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

// TrackerDir names the directory under .migrations/ that tracks a migration of
// the given strategy over propNames. A class-level strategy ignores propNames.
func TrackerDir(t *testing.T, code db.MigrationStrategyCode, propNames []string, generation int) string {
	t.Helper()

	recipe, ok := handleRecipes[code]
	require.Truef(t, ok, "no directory recipe for migration strategy %q", code)

	name := string(code)
	switch recipe.tracker {
	case trackerNamesOneProperty:
		require.Lenf(t, propNames, 1, "%q tracks exactly one property", code)
		name += "_" + propNames[0]
	case trackerNamesPropertyList:
		if len(propNames) > 0 {
			name += "_" + strings.Join(slices.Sorted(slices.Values(propNames)), "_")
		}
	case trackerNamesNoProperty:
	}
	return name + "_" + strconv.Itoa(generation)
}

type trackerNaming int

const (
	trackerNamesPropertyList trackerNaming = iota
	trackerNamesNoProperty
	trackerNamesOneProperty
)

type handleRecipe struct {
	bucket        func(propName string) string
	ingestSuffix  string
	reindexSuffix string
	tracker       trackerNaming
}

var handleRecipes = map[db.MigrationStrategyCode]handleRecipe{
	db.StrategyCodeSearchableMapToBlockmax: {
		helpers.BucketSearchableFromPropNameLSM, "__blockmax_ingest", "__blockmax_reindex", trackerNamesNoProperty,
	},
	db.StrategyCodeFilterableRoaringsetRefresh: {
		helpers.BucketFromPropNameLSM, "__roaringset_ingest", "__roaringset_reindex", trackerNamesNoProperty,
	},
	db.StrategyCodeFilterableToRangeable: {
		helpers.BucketRangeableFromPropNameLSM, "__rangeable_ingest", "__rangeable_reindex", trackerNamesPropertyList,
	},
	db.StrategyCodeSearchableRetokenize: {
		helpers.BucketSearchableFromPropNameLSM, "__retokenize_ingest", "__retokenize_reindex", trackerNamesOneProperty,
	},
	db.StrategyCodeFilterableRetokenize: {
		helpers.BucketFromPropNameLSM, "__filt_retokenize_ingest", "__filt_retokenize_reindex", trackerNamesOneProperty,
	},
	db.StrategyCodeEnableFilterable: {
		helpers.BucketFromPropNameLSM, "__enable_filterable_ingest", "__enable_filterable_reindex", trackerNamesPropertyList,
	},
	db.StrategyCodeEnableSearchable: {
		helpers.BucketSearchableFromPropNameLSM, "__enable_searchable_ingest", "__enable_searchable_reindex", trackerNamesPropertyList,
	},
	db.StrategyCodeRebuildSearchable: {
		helpers.BucketSearchableFromPropNameLSM, "__rebuild_searchable_ingest", "__rebuild_searchable_reindex", trackerNamesPropertyList,
	},
}
