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
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// completedSentinels are what a migration that finished its in-process swap
// leaves in its tracker dir.
var completedSentinels = []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"}

// recordedSentinels add the marker a load writes once it has promoted what the
// migration produced.
var recordedSentinels = append(append([]string{}, completedSentinels...), finalizedSentinel)

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

// A record survives only while the schema hides an index this node already
// rebuilt. Every other outcome — flag on, flag unset, a strategy that flips
// nothing — must finalize as before and leave nothing behind.
func TestFinalizeKeepsARecordOnlyWhileTheSchemaHidesAPromotedIndex(t *testing.T) {
	tests := []struct {
		name       string
		migName    string
		props      []string
		mainBucket string
		class      *models.Class
		// payloadTokenization plants the target a change-tokenization
		// migration recorded, which is the only thing that says which
		// tokenization the promoted keys are under.
		payloadTokenization string
		wantKept            bool
		// noOutput strips every dir the promotion could have produced or
		// promoted from, so absent ingest and backup dirs are the only thing
		// left to read the outcome off.
		noOutput bool
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
			// A record would have the next load open a bucket that is gone, and
			// shield the empty dir it then creates from every sweep.
			name:    "enable-filterable, nothing left on disk to promote",
			migName: "enable_filterable_category_1",
			props:   []string{"category"},
			class: classWithProperty(&models.Property{
				Name: "category", IndexFilterable: boolPtr(false),
			}),
			noOutput: true,
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
		// The two retokenize halves flip no index flag — their property keeps
		// its index on throughout. What the schema is behind on is the
		// tokenization the promoted keys were written under, which both the
		// write and the query path answer from.
		{
			name:                "searchable-retokenize, schema still names the old tokenization",
			migName:             "searchable_retokenize_title_1",
			props:               []string{"title"},
			mainBucket:          "property_title_searchable",
			payloadTokenization: models.PropertyTokenizationField,
			class: classWithProperty(&models.Property{
				Name: "title", IndexSearchable: boolPtr(true),
				Tokenization: models.PropertyTokenizationWord,
			}),
			wantKept: true,
		},
		{
			name:                "filterable-retokenize, schema still names the old tokenization",
			migName:             "filterable_retokenize_title_1",
			props:               []string{"title"},
			mainBucket:          "property_title",
			payloadTokenization: models.PropertyTokenizationField,
			class: classWithProperty(&models.Property{
				Name: "title", IndexFilterable: boolPtr(true),
				Tokenization: models.PropertyTokenizationWord,
			}),
			wantKept: true,
		},
		{
			name:                "filterable-retokenize, schema has caught up",
			migName:             "filterable_retokenize_title_1",
			props:               []string{"title"},
			mainBucket:          "property_title",
			payloadTokenization: models.PropertyTokenizationField,
			class: classWithProperty(&models.Property{
				Name: "title", IndexFilterable: boolPtr(true),
				Tokenization: models.PropertyTokenizationField,
			}),
		},
		{
			// Nothing names the tokenization to wait for, and the index flag
			// stays true either way, so the startup sweep never reaches this
			// bucket and the record protects nothing.
			name:       "filterable-retokenize, no payload to name the target",
			migName:    "filterable_retokenize_title_1",
			props:      []string{"title"},
			mainBucket: "property_title",
			class: classWithProperty(&models.Property{
				Name: "title", IndexFilterable: boolPtr(true),
				Tokenization: models.PropertyTokenizationWord,
			}),
		},
		// The remaining strategies flip no flag and change no tokenization, so
		// their promotion is never ahead of the schema and their tracker has
		// nothing left to say.
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
			if tc.payloadTokenization != "" {
				writeRecoveryPayload(t, lsmPath, tc.migName, tc.props, tc.payloadTokenization)
			}
			if tc.noOutput {
				buckets, err := filepath.Glob(filepath.Join(lsmPath, "property_*"))
				require.NoError(t, err)
				for _, dir := range buckets {
					require.NoError(t, os.RemoveAll(dir))
				}
			}

			logger, _ := test.NewNullLogger()
			FinalizeCompletedMigrations(lsmPath, tc.class, logger)

			// Whatever the verdict, the promotion itself must have run: the
			// record decides who cleans up, never whether the data lands.
			if !tc.noOutput {
				assert.Equal(t, tc.props[0], promotedMarkerOf(t, lsmPath, tc.mainBucket),
					"the ingest dir must have been promoted to the canonical name")
			}

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

// A start that only partially promotes a multi-property migration must not
// mark it: the marker claims "the promotion already ran", and the next start
// trusts it instead of retrying.
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

	// A tracker whose property list is gone names nothing to promote, so no
	// later start can get further than this one.
	for _, damage := range []struct {
		name string
		to   func(t *testing.T, propsFile string)
	}{
		{
			name: "a property list that cannot be read",
			to:   func(t *testing.T, propsFile string) { require.NoError(t, os.Remove(propsFile)) },
		},
		{
			name: "a property list a kill mid-write left empty",
			to:   func(t *testing.T, propsFile string) { require.NoError(t, os.Truncate(propsFile, 0)) },
		},
	} {
		t.Run(damage.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			plantCompletedMigration(t, lsmPath, migName, "alpha", "beta")
			migDir := filepath.Join(lsmPath, ".migrations", migName)
			damage.to(t, filepath.Join(migDir, "properties.mig"))

			logger, hook := test.NewNullLogger()
			FinalizeCompletedMigrations(lsmPath, class, logger)

			assert.DirExists(t, migDir,
				"removing the tracker would strand both properties' data under their ingest names")
			assert.NoFileExists(t, filepath.Join(migDir, finalizedSentinel))
			for _, entry := range hook.AllEntries() {
				assert.NotEqual(t, logrus.ErrorLevel, entry.Level,
					"an Error every load asks an operator to retry work no start can finish")
			}
		})
	}

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

// A crash between the promotion and the record leaves the ingest dir already
// renamed and no marker, so the next start re-runs a promotion whose work is
// done — that re-run must be a no-op, or it overwrites the canonical bucket
// with an empty one.
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

// Once a record exists, nothing re-promotes or rewrites the marker — a
// sidecar dir that reappears under the same name (e.g. after operator
// surgery) must not be renamed over an already-correct canonical bucket.
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

// What a strategy leaves the schema behind on decides whether its record is
// kept: an index flag the cluster has yet to flip, or the tokenization its
// promoted keys are under. A strategy added without an answer to both would
// silently join the "keeps nothing" side. Both directions are pinned: the list
// below must name every prefix the build knows, and every prefix must be
// answered on purpose.
func TestEveryMigrationDirPrefixHasARetentionVerdict(t *testing.T) {
	type verdict struct {
		indexType  string
		retokenize bool
	}
	verdicts := map[string]verdict{
		MigrationDirPrefixEnableFilterable:      {indexType: "filterable"},
		MigrationDirPrefixEnableSearchable:      {indexType: "searchable"},
		MigrationDirPrefixFilterableToRangeable: {indexType: "rangeable"},
		MigrationDirPrefixSearchableRetokenize:  {retokenize: true},
		MigrationDirPrefixFilterableRetokenize:  {retokenize: true},
		MigrationDirPrefixRebuildSearchable:     {},
		MigrationDirFilterableRoaringsetRefresh: {},
		MigrationDirSearchableMapToBlockmax:     {},
	}

	answered := make([]string, 0, len(verdicts))
	for prefix := range verdicts {
		answered = append(answered, prefix)
	}
	assert.ElementsMatch(t, allMigrationDirPrefixes, answered,
		"every migration dir prefix needs an explicit retention verdict")

	for prefix, want := range verdicts {
		assert.Equalf(t, want.indexType, awaitingFlipIndexType(prefix+"_someprop_1"),
			"index-flag retention verdict for %q", prefix)
		assert.Equalf(t, want.retokenize, isRetokenizeMigrationDir(prefix+"_someprop_1"),
			"tokenization retention verdict for %q", prefix)
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

// A new sentinel is a new state every reader of a tracker dir has to be
// taught; this source scan makes adding one fail here rather than pass
// unnoticed. Scoped to the reindex sources: .migrations/ is shared with other
// subsystems that keep their own flag files under it.
func TestMigrationSentinelVocabularyIsEnumerated(t *testing.T) {
	known := []string{
		"started.mig", "tidied.mig", "merged.mig", "swapped.mig", "reindexed.mig",
		"prepended.mig", "progress.mig", "properties.mig", "payload.mig",
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
