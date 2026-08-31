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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
)

// namingStrategy is the part of a migration strategy that decides directory
// names. Declared here rather than using db.MigrationStrategy so this test
// depends on the four methods it actually pins.
type namingStrategy interface {
	StrategyCode() db.MigrationStrategyCode
	MigrationDirName() string
	SourceBucketName(propName string) string
	IngestSuffix() string
	ReindexSuffix() string
}

// strategiesUnderTest is every migration strategy, at the zero value. Building
// them by type rather than by code is what makes a strategy that is renamed or
// removed a compile error here.
func strategiesUnderTest() []namingStrategy {
	return []namingStrategy{
		&db.MapToBlockmaxStrategy{},
		&db.RoaringSetRefreshStrategy{},
		&db.FilterableToRangeableStrategy{},
		&db.SearchableRetokenizeStrategy{},
		&db.FilterableRetokenizeStrategy{},
		&db.EnableFilterableStrategy{},
		&db.EnableSearchableStrategy{},
		&db.RebuildSearchableStrategy{},
	}
}

// TestHandlesMatchTheStrategies pins [handleRecipes] against the strategies
// themselves, so a suffix or a bucket namer that moves fails here rather than
// in an acceptance run that plants a directory no writer emits.
func TestHandlesMatchTheStrategies(t *testing.T) {
	strategies := strategiesUnderTest()
	require.Len(t, handleRecipes, len(strategies),
		"every strategy needs a recipe, and every recipe a strategy")

	for _, strategy := range strategies {
		t.Run(string(strategy.StrategyCode()), func(t *testing.T) {
			recipe, ok := handleRecipes[strategy.StrategyCode()]
			require.Truef(t, ok, "%T has no recipe", strategy)

			// The empty property name, because two strategies take theirs at
			// construction and ignore the argument. It still separates the
			// three bucket shapes: "property_", "property__searchable" and
			// "property__rangeable".
			require.Equalf(t, strategy.SourceBucketName(""), recipe.bucket(""),
				"%T works on a different property bucket than its recipe", strategy)

			// A zero-value strategy is at generation 0, and each suffix method
			// appends its own generation tail.
			require.Equalf(t, strategy.IngestSuffix(), recipe.ingestSuffix+"_0",
				"%T stages under a different suffix than its recipe", strategy)
			require.Equalf(t, strategy.ReindexSuffix(), recipe.reindexSuffix+"_0",
				"%T rebuilds under a different suffix than its recipe", strategy)
		})
	}
}

// TestTrackerDirsMatchTheStrategies pins [TrackerDir] the same way, against
// each strategy at the zero value: no property names and generation 0.
func TestTrackerDirsMatchTheStrategies(t *testing.T) {
	for _, strategy := range strategiesUnderTest() {
		t.Run(string(strategy.StrategyCode()), func(t *testing.T) {
			// The one-property strategies hold their property rather than
			// taking it per call, so at the zero value it is the empty string
			// — which their directory name carries all the same.
			var props []string
			if handleRecipes[strategy.StrategyCode()].tracker == trackerNamesOneProperty {
				props = []string{""}
			}
			require.Equalf(t, strategy.MigrationDirName(),
				TrackerDir(t, strategy.StrategyCode(), props, 0),
				"%T tracks under a different directory than its recipe", strategy)
		})
	}
}

// TestHandlesAreAcceptedByTheRecordWriter is the end-to-end direction: the
// record store refuses a staged or sidecar handle that is not shaped like a
// sidecar of a property bucket, so every name [HandlesFor] hands a fixture has
// to survive that check.
func TestHandlesAreAcceptedByTheRecordWriter(t *testing.T) {
	// Property names carrying the separators the shape rule reads: "__" and a
	// trailing role word of their own.
	props := []string{"body", "a__b", "x_ingest"}

	for _, strategy := range strategiesUnderTest() {
		for _, prop := range props {
			for _, generation := range []int{1, 2, 11} {
				t.Run(string(strategy.StrategyCode())+"/"+prop, func(t *testing.T) {
					handles := HandlesFor(t, strategy.StrategyCode(), prop, generation)
					Encode(t, db.NewMigrationRecordIterating(db.MigrationSubject{
						Key: db.MigrationRecordKey{
							TaskVersion:  1,
							StrategyCode: strategy.StrategyCode(),
							UnitID:       "u0",
						},
						TaskID:          "handles-are-accepted",
						MigrationType:   db.ReindexTypeRepairFilterable,
						Properties:      []string{prop},
						IterationCutoff: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
						TrackerDir:      TrackerDir(t, strategy.StrategyCode(), []string{prop}, generation),
						StagedDirs:      map[string]string{prop: handles.Staged},
						CanonicalDirs:   map[string]string{prop: handles.Canonical},
						SidecarDirs:     map[string]string{prop: handles.Sidecar},
					}, db.MigrationCheckpoint{}))
				})
			}
		}
	}
}
