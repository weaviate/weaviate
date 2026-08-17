//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//   \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// completedSentinels are what a migration that finished its in-process swap
// leaves in its tracker dir.
var completedSentinels = []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"}

// plantCompletedMigration lays out the on-disk state finalize finds after a
// migration completed in-process: the tracker with its sentinels and property
// list, the ingest dir holding the backfilled data, and the backup copy of the
// bucket it replaced.
func plantCompletedMigration(t *testing.T, lsmPath, migName string, props ...string) {
	t.Helper()
	mkTrackerDir(t, lsmPath, migName, completedSentinels...)
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, ".migrations", migName, "properties.mig"),
		[]byte(strings.Join(props, ",")), 0o644))
	suffixes := migrationSuffixes(migName)
	require.NotNil(t, suffixes, "test fixture names a strategy finalize does not know")
	_, gen, ok := parseMigrationDirName(migName)
	require.True(t, ok)
	for _, propName := range props {
		main := suffixes.sourceBucketName(propName)
		mkSidecarDir(t, lsmPath, main+suffixes.ingestSuffix+genSuffix(gen))
		mkSidecarDir(t, lsmPath, main+suffixes.backupSuffix+genSuffix(gen))
		require.NoError(t, os.WriteFile(
			filepath.Join(lsmPath, main+suffixes.ingestSuffix+genSuffix(gen), "promoted.marker"),
			[]byte(propName), 0o644))
	}
}

// promotedMarkerOf reads the file that rode along inside a property's ingest
// dir, so a canonical dir the promotion produced can be told apart from one
// something else created.
func promotedMarkerOf(t *testing.T, lsmPath, bucketName string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(lsmPath, bucketName, "promoted.marker"))
	require.NoError(t, err)
	return string(data)
}

func classWithProperty(prop *models.Property) *models.Class {
	return &models.Class{Class: "Retention", Properties: []*models.Property{prop}}
}

// The record exists for exactly one window: an index this node already
// rebuilt, which the schema still advertises as disabled because the cluster
// has not agreed to flip the flag yet. Every other outcome — flag already on,
// flag never set, a strategy that flips nothing — must finalize the way it
// always did and leave nothing behind.
func TestFinalizeKeepsARecordOnlyWhileTheSchemaHidesAPromotedIndex(t *testing.T) {
	tests := []struct {
		name       string
		migName    string
		props      []string
		mainBucket string
		class      *models.Class
		wantKept   bool
	}{
		{
			name:       "enable-filterable, flag still disabled",
			migName:    "enable_filterable_category_1",
			props:      []string{"category"},
			mainBucket: "property_category",
			class: classWithProperty(&models.Property{
				Name: "category", IndexFilterable: boolPtr(false),
			}),
			wantKept: true,
		},
		{
			name:       "enable-filterable, flag already flipped",
			migName:    "enable_filterable_category_1",
			props:      []string{"category"},
			mainBucket: "property_category",
			class: classWithProperty(&models.Property{
				Name: "category", IndexFilterable: boolPtr(true),
			}),
		},
		{
			name:       "enable-filterable, flag never set",
			migName:    "enable_filterable_category_1",
			props:      []string{"category"},
			mainBucket: "property_category",
			class:      classWithProperty(&models.Property{Name: "category"}),
		},
		{
			name:       "enable-filterable, property no longer in the class",
			migName:    "enable_filterable_category_1",
			props:      []string{"category"},
			mainBucket: "property_category",
			class:      classWithProperty(&models.Property{Name: "other", IndexFilterable: boolPtr(false)}),
		},
		{
			name:       "enable-filterable, no class to answer from",
			migName:    "enable_filterable_category_1",
			props:      []string{"category"},
			mainBucket: "property_category",
		},
		{
			name:       "enable-filterable, one of two properties still disabled",
			migName:    "enable_filterable_alpha_beta_1",
			props:      []string{"alpha", "beta"},
			mainBucket: "property_alpha",
			class: &models.Class{Class: "Retention", Properties: []*models.Property{
				{Name: "alpha", IndexFilterable: boolPtr(true)},
				{Name: "beta", IndexFilterable: boolPtr(false)},
			}},
			wantKept: true,
		},
		{
			name:       "enable-searchable, flag still disabled",
			migName:    "enable_searchable_title_1",
			props:      []string{"title"},
			mainBucket: "property_title_searchable",
			class: classWithProperty(&models.Property{
				Name: "title", IndexSearchable: boolPtr(false),
			}),
			wantKept: true,
		},
		{
			name:       "enable-searchable, flag already flipped",
			migName:    "enable_searchable_title_1",
			props:      []string{"title"},
			mainBucket: "property_title_searchable",
			class: classWithProperty(&models.Property{
				Name: "title", IndexSearchable: boolPtr(true),
			}),
		},
		// enable-rangeable and repair-rangeable share one dir prefix. The
		// predicate is deliberately blind to which of the two ran: repair runs
		// with the flag already true, so it never satisfies it.
		{
			name:       "enable-rangeable, flag still disabled",
			migName:    "filterable_to_rangeable_score_1",
			props:      []string{"score"},
			mainBucket: "property_score_rangeable",
			class: classWithProperty(&models.Property{
				Name: "score", IndexRangeFilters: boolPtr(false),
			}),
			wantKept: true,
		},
		{
			name:       "repair-rangeable, which runs with the flag already on",
			migName:    "filterable_to_rangeable_score_1",
			props:      []string{"score"},
			mainBucket: "property_score_rangeable",
			class: classWithProperty(&models.Property{
				Name: "score", IndexRangeFilters: boolPtr(true),
			}),
		},
		// The remaining strategies flip no flag, so their promotion is never
		// ahead of the schema and their tracker has nothing left to say. The
		// two retokenize halves join them here on purpose: the startup sweep
		// deletes only on an explicit false, and a retokenized property keeps
		// its index flag true throughout.
		{
			name:       "searchable-retokenize, whose index flag stays on throughout",
			migName:    "searchable_retokenize_title_1",
			props:      []string{"title"},
			mainBucket: "property_title_searchable",
			class: classWithProperty(&models.Property{
				Name: "title", IndexSearchable: boolPtr(true),
				Tokenization: models.PropertyTokenizationField,
			}),
		},
		{
			name:       "filterable-retokenize, whose index flag stays on throughout",
			migName:    "filterable_retokenize_title_1",
			props:      []string{"title"},
			mainBucket: "property_title",
			class: classWithProperty(&models.Property{
				Name: "title", IndexFilterable: boolPtr(true),
				Tokenization: models.PropertyTokenizationField,
			}),
		},
		{
			name:       "searchable-map-to-blockmax, a write-strategy change",
			migName:    "searchable_map_to_blockmax_1",
			props:      []string{"title"},
			mainBucket: "property_title_searchable",
			class: classWithProperty(&models.Property{
				Name: "title", IndexSearchable: boolPtr(false),
			}),
		},
		{
			name:       "filterable-roaringset-refresh, a rebuild of an enabled index",
			migName:    "filterable_roaringset_refresh_1",
			props:      []string{"category"},
			mainBucket: "property_category",
			class: classWithProperty(&models.Property{
				Name: "category", IndexFilterable: boolPtr(false),
			}),
		},
		{
			name:       "rebuild-searchable, a rebuild of an enabled index",
			migName:    "rebuild_searchable_title_1",
			props:      []string{"title"},
			mainBucket: "property_title_searchable",
			class: classWithProperty(&models.Property{
				Name: "title", IndexSearchable: boolPtr(false),
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			plantCompletedMigration(t, lsmPath, tc.migName, tc.props...)

			logger, _ := test.NewNullLogger()
			FinalizeCompletedMigrations(lsmPath, tc.class, logger)

			// Whatever the verdict, the promotion itself must have run: the
			// record decides who cleans up, never whether the data lands.
			assert.Equal(t, tc.props[0], promotedMarkerOf(t, lsmPath, tc.mainBucket),
				"the ingest dir must have been promoted to the canonical name")

			migDir := filepath.Join(lsmPath, ".migrations", tc.migName)
			if !tc.wantKept {
				assert.NoDirExists(t, migDir, "nothing is waiting on this record")
				return
			}
			assert.DirExists(t, migDir,
				"the schema still hides this index, so the record is the only note that "+
					"the bucket on disk is a finished index rather than garbage")
			assert.FileExists(t, filepath.Join(migDir, finalizedSentinel),
				"a kept record must say that its promotion already ran")
		})
	}
}

// The marker's claim is "the promotion already ran", and the next start reads
// it instead of retrying. A start that promoted only part of a multi-property
// migration must therefore not write it — the tracker's tidied.mig keeps the
// promoted buckets shielded meanwhile, and the next start finishes the job.
func TestFinalizeLeavesAFailedPromotionUnmarked(t *testing.T) {
	const migName = "enable_filterable_alpha_beta_1"

	class := &models.Class{Class: "Retention", Properties: []*models.Property{
		{Name: "alpha", IndexFilterable: boolPtr(false)},
		{Name: "beta", IndexFilterable: boolPtr(false)},
	}}

	t.Run("a rename that fails for one property", func(t *testing.T) {
		lsmPath := t.TempDir()
		plantCompletedMigration(t, lsmPath, migName, "alpha", "beta")
		// A dangling symlink is a directory entry finalize cannot stat and
		// cannot rename a directory over, so beta's promotion fails while
		// alpha's succeeds.
		blocked := filepath.Join(lsmPath, "property_beta")
		require.NoError(t, os.Symlink(filepath.Join(lsmPath, "gone"), blocked))

		logger, _ := test.NewNullLogger()
		FinalizeCompletedMigrations(lsmPath, class, logger)

		migDir := filepath.Join(lsmPath, ".migrations", migName)
		assert.Equal(t, "alpha", promotedMarkerOf(t, lsmPath, "property_alpha"))
		assert.DirExists(t, lsmPath+"/property_beta__enable_filterable_ingest_1",
			"beta's data must still be under the ingest name for the next start to promote")
		assert.DirExists(t, migDir)
		assert.NoFileExists(t, filepath.Join(migDir, finalizedSentinel),
			"a marker here would tell the next start that beta was promoted, and it never was")

		// The next start, with the obstruction gone, must finish the job.
		require.NoError(t, os.Remove(blocked))
		FinalizeCompletedMigrations(lsmPath, class, logger)
		assert.Equal(t, "beta", promotedMarkerOf(t, lsmPath, "property_beta"))
		assert.FileExists(t, filepath.Join(migDir, finalizedSentinel))
	})

	t.Run("a property list that cannot be read", func(t *testing.T) {
		lsmPath := t.TempDir()
		plantCompletedMigration(t, lsmPath, migName, "alpha", "beta")
		migDir := filepath.Join(lsmPath, ".migrations", migName)
		require.NoError(t, os.Remove(filepath.Join(migDir, "properties.mig")))

		logger, _ := test.NewNullLogger()
		FinalizeCompletedMigrations(lsmPath, class, logger)

		assert.DirExists(t, migDir,
			"removing the tracker would strand both properties' data under their ingest names")
		assert.NoFileExists(t, filepath.Join(migDir, finalizedSentinel))
	})

	t.Run("a marker that cannot be written", func(t *testing.T) {
		lsmPath := t.TempDir()
		plantCompletedMigration(t, lsmPath, migName, "alpha", "beta")
		migDir := filepath.Join(lsmPath, ".migrations", migName)
		// A directory of the marker's name is not the marker, and it is what
		// makes the exclusive create fail for any user, root included.
		require.NoError(t, os.MkdirAll(filepath.Join(migDir, finalizedSentinel), 0o755))

		logger, _ := test.NewNullLogger()
		FinalizeCompletedMigrations(lsmPath, class, logger)

		assert.DirExists(t, migDir,
			"a marker that could not be written must never fall through to removing the record")
		assert.Equal(t, "alpha", promotedMarkerOf(t, lsmPath, "property_alpha"))
	})
}

// A start that crashes between the promotion and the record leaves the ingest
// dir already renamed and no marker, so the next start re-runs the promotion
// on a tracker whose work is done. That re-run has to be a no-op: it is the
// only thing standing between the crash window and a canonical bucket
// overwritten by an empty one.
func TestFinalizeMigrationDirReRunsAsANoOp(t *testing.T) {
	const migName = "enable_filterable_category_1"
	lsmPath := t.TempDir()
	plantCompletedMigration(t, lsmPath, migName, "category")

	class := classWithProperty(&models.Property{Name: "category", IndexFilterable: boolPtr(false)})
	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, class, logger)

	migDir := filepath.Join(lsmPath, ".migrations", migName)
	require.FileExists(t, filepath.Join(migDir, finalizedSentinel))
	// Simulate the crash: the promotion landed, the record did not.
	require.NoError(t, os.Remove(filepath.Join(migDir, finalizedSentinel)))

	FinalizeCompletedMigrations(lsmPath, class, logger)

	assert.Equal(t, "category", promotedMarkerOf(t, lsmPath, "property_category"),
		"the canonical bucket the first start promoted must be untouched")
	assert.FileExists(t, filepath.Join(migDir, finalizedSentinel),
		"and the second start must record what the first one could not")
}

// From the second start onward the record answers the question, so nothing
// re-promotes and nothing rewrites the marker. Without that, a sidecar dir
// that reappears under the same name — a re-submitted migration claiming the
// same generation after operator surgery — would be renamed over a canonical
// bucket that is already correct.
func TestFinalizeDoesNotPromoteTwiceOnTopOfARecord(t *testing.T) {
	const migName = "enable_filterable_category_1"
	lsmPath := t.TempDir()
	plantCompletedMigration(t, lsmPath, migName, "category")

	class := classWithProperty(&models.Property{Name: "category", IndexFilterable: boolPtr(false)})
	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, class, logger)

	migDir := filepath.Join(lsmPath, ".migrations", migName)
	require.FileExists(t, filepath.Join(migDir, finalizedSentinel))

	intruder := "property_category__enable_filterable_ingest_1"
	mkSidecarDir(t, lsmPath, intruder)
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, intruder, "promoted.marker"), []byte("intruder"), 0o644))

	FinalizeCompletedMigrations(lsmPath, class, logger)

	assert.Equal(t, "category", promotedMarkerOf(t, lsmPath, "property_category"),
		"a recorded promotion is not repeated")
	assert.DirExists(t, filepath.Join(lsmPath, intruder))
	assert.DirExists(t, migDir)

	// The flip is what retires the record, at the first start after it lands.
	class.Properties[0].IndexFilterable = boolPtr(true)
	FinalizeCompletedMigrations(lsmPath, class, logger)
	assert.NoDirExists(t, migDir, "the schema now advertises the index; the record is spent")
	assert.Equal(t, "category", promotedMarkerOf(t, lsmPath, "property_category"))
}

// A re-submitted migration supersedes the record it started from: the older
// generation still carries tidied.mig, so the existing older-gen arm removes
// it once the newer generation is promoted.
func TestFinalizeRetiresARecordSupersededByANewerGeneration(t *testing.T) {
	lsmPath := t.TempDir()
	plantCompletedMigration(t, lsmPath, "enable_filterable_category_1", "category")

	class := classWithProperty(&models.Property{Name: "category", IndexFilterable: boolPtr(false)})
	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, class, logger)
	require.FileExists(t, filepath.Join(lsmPath, ".migrations", "enable_filterable_category_1", finalizedSentinel))

	plantCompletedMigration(t, lsmPath, "enable_filterable_category_2", "category")
	FinalizeCompletedMigrations(lsmPath, class, logger)

	assert.NoDirExists(t, filepath.Join(lsmPath, ".migrations", "enable_filterable_category_1"),
		"the newer generation's data replaced what the older record named")
	assert.FileExists(t, filepath.Join(lsmPath, ".migrations", "enable_filterable_category_2", finalizedSentinel))
	assert.Equal(t, "category", promotedMarkerOf(t, lsmPath, "property_category"))
}

// Which index a strategy switches on decides whether its record is kept, so a
// strategy added without an answer here would silently join the "keeps
// nothing" side. Both directions are pinned: the list below must name every
// prefix the build knows, and every prefix must be answered on purpose.
func TestEveryMigrationDirPrefixHasARetentionVerdict(t *testing.T) {
	verdicts := map[string]string{
		MigrationDirPrefixEnableFilterable:      "filterable",
		MigrationDirPrefixEnableSearchable:      "searchable",
		MigrationDirPrefixFilterableToRangeable: "rangeable",
		MigrationDirPrefixSearchableRetokenize:  "",
		MigrationDirPrefixFilterableRetokenize:  "",
		MigrationDirPrefixRebuildSearchable:     "",
		MigrationDirFilterableRoaringsetRefresh: "",
		MigrationDirSearchableMapToBlockmax:     "",
	}

	answered := make([]string, 0, len(verdicts))
	for prefix := range verdicts {
		answered = append(answered, prefix)
	}
	assert.ElementsMatch(t, allMigrationDirPrefixes, answered,
		"every migration dir prefix needs an explicit retention verdict")

	for prefix, want := range verdicts {
		assert.Equalf(t, want, awaitingFlipIndexType(prefix+"_someprop_1"),
			"retention verdict for %q", prefix)
	}
}

// allMigrationDirPrefixes is the completeness argument for readers that decide
// per strategy and cannot be enumerated from their own switch. It only carries
// that argument while it stays in step with the maps that route an index type
// to its strategies, and while every prefix on it is one those readers know.
func TestAllMigrationDirPrefixesCoversEveryStrategy(t *testing.T) {
	var routed []string
	for _, indexType := range []string{"filterable", "searchable", "rangeable"} {
		routed = append(routed, migrationDirPrefixesForIndexType(indexType)...)
		if classDir, ok := classLevelMigrationDirForIndexType(indexType); ok {
			routed = append(routed, classDir)
		}
	}
	sort.Strings(routed)
	routed = slices.Compact(routed)

	want := slices.Clone(allMigrationDirPrefixes)
	sort.Strings(want)
	assert.Equal(t, want, routed,
		"a strategy reachable from an index type but missing here, or the other way round")

	for _, prefix := range allMigrationDirPrefixes {
		migName := prefix + "_someprop_1"
		assert.NotEmptyf(t, reindexSuffixForFinalize(migName),
			"finalize cannot identify %q's reindex sidecars", prefix)
		assert.NotNilf(t, migrationSuffixes(migName),
			"finalize cannot name %q's buckets", prefix)
	}
}

// Every reader of a tracker dir decides from the sentinel files in it, so a
// new sentinel is a new state each of them has to be taught. The source scan
// makes adding one fail here rather than pass unnoticed.
//
// Scoped to the reindex sources on purpose: .migrations/ is shared with other
// subsystems that keep their own flag files under it.
func TestMigrationSentinelVocabularyIsEnumerated(t *testing.T) {
	known := []string{
		"started.mig", "tidied.mig", "merged.mig", "swapped.mig", "reindexed.mig",
		"prepended.mig", "progress.mig", "paused.mig", "reset.mig", "rollback.mig",
		"start.mig", "properties.mig", "payload.mig", "overrides.mig",
		"audit_quarantined.mig", finalizedSentinel,
	}

	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, sources)

	literal := regexp.MustCompile(`"([a-z_]+\.mig)"`)
	found := map[string]bool{}
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		data, err := os.ReadFile(source)
		require.NoError(t, err)
		for _, match := range literal.FindAllStringSubmatch(string(data), -1) {
			found[match[1]] = true
		}
	}

	seen := make([]string, 0, len(found))
	for name := range found {
		seen = append(seen, name)
	}
	assert.ElementsMatch(t, known, seen,
		"a sentinel name appeared or disappeared; every reader of a tracker dir needs to agree on the set")
}
