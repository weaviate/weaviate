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
	// Canonical is the property's own bucket, the name promotion moves onto.
	Canonical string
	// Staged holds the migration's copy of the index until promotion.
	Staged string
	// Sidecar holds what the backfill scan rebuilt from the objects store.
	Sidecar string
}

// HandlesFor names the directories the writer emits for propName under the
// strategy that code identifies, at the given migration generation.
//
// A fixture planting on-disk state has to name what a writer would have
// written. A staged or sidecar handle is refused outright unless it is shaped
// like a sidecar of a property bucket, and a canonical handle that names the
// wrong bucket points promotion at another property's index — neither state a
// crashed run can leave behind, so a fixture that builds one pins behavior
// against something no server ever meets.
//
// [TestHandlesMatchTheStrategies] pins every recipe against the strategy that
// owns it, so a renamed suffix or bucket fails there rather than in an
// acceptance run.
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
// the given strategy over propNames, at the given generation. A strategy that
// tracks the whole class names no property, and ignores propNames.
//
// The strategy code is the directory name, so the only thing to get right is
// how the property names and the generation hang off it — and a tracker whose
// generation disagrees with the sidecars beside it is another state no writer
// produces. [TestTrackerDirsMatchTheStrategies] pins that against the
// strategies.
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
			// Sorted, so the name is a function of the set rather than of the
			// caller's slice order.
			name += "_" + strings.Join(slices.Sorted(slices.Values(propNames)), "_")
		}
	case trackerNamesNoProperty:
		// The strategy code alone, whatever properties the migration covers.
	}
	return name + "_" + strconv.Itoa(generation)
}

// trackerNaming is how a strategy's tracker directory carries property names.
// Which one a strategy uses shows in the directory-name constant it reaches
// for: db.MigrationDirPrefix* is a prefix properties hang off, db.MigrationDir*
// is a whole name.
type trackerNaming int

const (
	// trackerNamesPropertyList joins the migration's sorted property names onto
	// the strategy code, and leaves them out when the migration names none.
	trackerNamesPropertyList trackerNaming = iota
	// trackerNamesNoProperty tracks the whole class under the strategy code
	// alone, whatever properties the migration covers.
	trackerNamesNoProperty
	// trackerNamesOneProperty always carries exactly one property name, since
	// the strategy takes it at construction.
	trackerNamesOneProperty
)

// handleRecipe is how one strategy composes its directory names: the property
// bucket it works on, plus the suffix of each sidecar role. The generation tail
// is not part of a suffix here — [HandlesFor] appends it, the way each
// strategy's own suffix method does.
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
