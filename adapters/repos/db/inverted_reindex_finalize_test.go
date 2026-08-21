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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// Tests for the per-migration generation helpers added for
// https://github.com/weaviate/weaviate/issues/10675. The functions under test live in
// inverted_reindex_finalize.go and inverted_reindex_strategy_dir_names.go.

func TestParseMigrationDirName(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantPrefix string
		wantGen    int
		wantOK     bool
	}{
		{"per-prop retokenize gen 1", "searchable_retokenize_text_1", "searchable_retokenize_text", 1, true},
		{"per-prop retokenize gen 42", "filterable_retokenize_email_42", "filterable_retokenize_email", 42, true},
		{"per-prop with underscore-y prop name", "enable_filterable_prop_with_underscores_3", "enable_filterable_prop_with_underscores", 3, true},
		{"class-level gen", "searchable_map_to_blockmax_1", "searchable_map_to_blockmax", 1, true},
		{"class-level roaringset gen", "filterable_roaringset_refresh_5", "filterable_roaringset_refresh", 5, true},
		{"missing gen suffix", "searchable_retokenize_text", "", 0, false},
		{"trailing underscore (no digit)", "searchable_retokenize_text_", "", 0, false},
		{"non-integer suffix", "searchable_retokenize_text_abc", "", 0, false},
		{"zero gen rejected", "searchable_retokenize_text_0", "", 0, false},
		{"negative gen rejected", "searchable_retokenize_text_-1", "", 0, false},
		{"empty string", "", "", 0, false},
		{"single token", "foo", "", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prefix, gen, ok := parseMigrationDirName(c.input)
			require.Equal(t, c.wantOK, ok, "ok mismatch")
			if c.wantOK {
				require.Equal(t, c.wantPrefix, prefix, "prefix mismatch")
				require.Equal(t, c.wantGen, gen, "gen mismatch")
			}
		})
	}
}

func TestGenSuffix(t *testing.T) {
	require.Equal(t, "_1", genSuffix(1))
	require.Equal(t, "_42", genSuffix(42))
	require.Equal(t, "_0", genSuffix(0)) // 0 is reserved (canonical) but genSuffix still emits — callers don't pass 0
}

// fakeMigrationsDir creates a temp .migrations/ tree with the given dir
// names and returns the parent lsmPath.
func fakeMigrationsDir(t *testing.T, dirs []string) string {
	t.Helper()
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))
	for _, d := range dirs {
		require.NoError(t, os.MkdirAll(filepath.Join(migsDir, d), 0o755))
	}
	return lsmPath
}

func TestNextMigrationGeneration_EmptyDisk(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, nil)
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 1, got, "fresh disk should pick gen 1")
}

func TestNextMigrationGeneration_NoMatchingPrefix(t *testing.T) {
	// Existing dirs for a DIFFERENT prop / strategy don't bump the
	// counter for ours.
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_otherprop_1",
		"filterable_retokenize_text_2",
		"enable_filterable_text_5",
	})
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 1, got, "no matching prefix means fresh gen 1")
}

func TestNextMigrationGeneration_ContiguousGens(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_1",
		"searchable_retokenize_text_2",
		"searchable_retokenize_text_3",
	})
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 4, got, "max+1 across contiguous gens")
}

func TestNextMigrationGeneration_NonContiguousGens(t *testing.T) {
	// If gens have gaps (e.g. trim removed some but not the highest), we
	// still pick max+1 — never reuse a gap.
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_1",
		"searchable_retokenize_text_5",
		"searchable_retokenize_text_7",
	})
	got := nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text")
	require.Equal(t, 8, got, "non-contiguous gens still pick max+1")
}

func TestNextMigrationGeneration_MixedPrefixesScopedCorrectly(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_3",
		"searchable_retokenize_other_7", // different prop in same prefix
		"filterable_retokenize_text_10", // different prefix, same prop
	})
	require.Equal(t, 4, nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text"))
	require.Equal(t, 8, nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_other"))
	require.Equal(t, 11, nextMigrationGeneration(lsmPath, MigrationDirPrefixFilterableRetokenize, "_text"))
	require.Equal(t, 1, nextMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_neverused"))
}

func TestMaxMigrationGeneration_NoExisting(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, nil)
	require.Equal(t, 0, maxMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text"))
}

func TestMaxMigrationGeneration_Existing(t *testing.T) {
	lsmPath := fakeMigrationsDir(t, []string{
		"searchable_retokenize_text_2",
		"searchable_retokenize_text_5",
	})
	require.Equal(t, 5, maxMigrationGeneration(lsmPath, MigrationDirPrefixSearchableRetokenize, "_text"))
}

// plantedMigration is one migration on the shard under reconciliation. The
// generation is the record key's task version, so a row can order two
// migrations without naming a directory.
type plantedMigration struct {
	generation uint64
	code       MigrationStrategyCode
	prop       string
	state      MigrationState
	// canonical overrides the bucket this migration promotes onto. Two
	// migrations on one property only stay independent while they name
	// different ones.
	canonical string
	// running keeps the owning task in the applied map, which is what holds an
	// uncommitted migration where it is.
	running bool
}

func (p plantedMigration) subject() MigrationSubject {
	subject := testMigrationSubject(p.generation, p.code, p.prop)
	if p.canonical != "" {
		subject.CanonicalDirs[p.prop] = p.canonical
	}
	return subject
}

func (p plantedMigration) record(t *testing.T) MigrationRecord {
	t.Helper()
	subject := p.subject()
	flipped, displaced := []string{p.prop}, map[string]string{p.prop: subject.CanonicalDirs[p.prop]}
	switch p.state {
	case MigrationStateIterating:
		return NewMigrationRecordIterating(subject, MigrationCheckpoint{})
	case MigrationStateIterated:
		return NewMigrationRecordIterated(subject)
	case MigrationStateMerged:
		return NewMigrationRecordMerged(subject)
	case MigrationStateSwapped:
		return NewMigrationRecordSwapped(subject, flipped, displaced)
	case MigrationStatePromoted:
		return NewMigrationRecordPromoted(subject, flipped, displaced)
	}
	require.FailNowf(t, "unknown migration state", "%q", p.state)
	return nil
}

// TestReconcileConvergesEveryMigrationOnAShard pins what the startup finalizer
// used to own: one shard's disk can carry several migrations at once, and a
// load has to settle each of them. The suites next to this one plant records
// that interact — one supersedes another, or answers for the same property —
// so nothing there would catch a pass that stopped after the first record.
func TestReconcileConvergesEveryMigrationOnAShard(t *testing.T) {
	tests := []struct {
		name  string
		plant []plantedMigration
		// wantCanonical maps each canonical bucket to the directory whose data
		// it must hold once the load is done. Every key is planted before the
		// load, so a bucket naming itself is one nothing moved.
		wantCanonical map[string]string
		// wantStaged names the staged directories that must still be there.
		wantStaged []string
	}{
		{
			// The two indexes of one change-tokenization task run as separate
			// migrations and land in separate buckets.
			name: "two strategies on one property settle into their own buckets",
			plant: []plantedMigration{
				{generation: 10, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateMerged},
				{
					generation: 11, code: StrategyCodeFilterableRetokenize, prop: "title",
					canonical: "property_title_filterable", state: MigrationStateMerged,
				},
			},
			wantCanonical: map[string]string{
				"property_title":            "m_10_title",
				"property_title_filterable": "m_11_title",
			},
		},
		{
			name: "three properties on three strategies settle together",
			plant: []plantedMigration{
				{generation: 20, code: StrategyCodeSearchableRetokenize, prop: "alpha", state: MigrationStateMerged},
				{generation: 21, code: StrategyCodeEnableFilterable, prop: "beta", state: MigrationStateSwapped},
				{generation: 22, code: StrategyCodeFilterableToRangeable, prop: "gamma", state: MigrationStateMerged},
			},
			wantCanonical: map[string]string{
				"property_alpha": "m_20_alpha",
				"property_beta":  "m_21_beta",
				"property_gamma": "m_22_gamma",
			},
		},
		{
			name: "a committed migration settles beside one still rebuilding",
			plant: []plantedMigration{
				{generation: 30, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateMerged},
				{
					generation: 31, code: StrategyCodeSearchableRetokenize, prop: "body",
					state: MigrationStateIterating, running: true,
				},
			},
			wantCanonical: map[string]string{
				"property_title": "m_30_title",
				"property_body":  "property_body",
			},
			wantStaged: []string{"m_31_body"},
		},
		{
			// The #10675 shape: the newer data is complete but its flip never
			// happened, while an older migration already flipped. Handing the
			// bucket to the older one serves data the cluster has moved past.
			name: "the newer migration wins the bucket even though the older one already flipped",
			plant: []plantedMigration{
				{generation: 40, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateSwapped},
				{generation: 41, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateMerged},
			},
			wantCanonical: map[string]string{"property_title": "m_41_title"},
		},
		{
			name:          "a bucket no record names is left alone",
			wantCanonical: map[string]string{"property_title": "property_title"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			var props []string
			for canonical := range tt.wantCanonical {
				f.mkdirs(canonical)
			}
			for _, planted := range tt.plant {
				subject := planted.subject()
				f.mkdirs(append(migrationOwnedDirs(subject), subject.CanonicalDirs[planted.prop])...)
				f.put(planted.record(t))
				props = append(props, planted.prop)
				if planted.running {
					f.tasks = append(f.tasks,
						testTask(subject.TaskID, planted.generation, distributedtask.TaskStatusStarted))
				}
			}
			// Every task that is not held running is gone from the map, so the
			// schema showing the effect is what commits it.
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, props...)

			f.reconcile()
			// Twice: a load is re-run on every restart, and the second pass
			// must find nothing left to move.
			f.reconcile()

			for canonical, want := range tt.wantCanonical {
				require.Truef(t, f.exists(canonical), "canonical bucket %q", canonical)
				require.Equalf(t, want, f.contentOf(canonical), "canonical bucket %q", canonical)
			}
			for _, staged := range tt.wantStaged {
				require.Truef(t, f.exists(staged), "staged directory %q", staged)
			}
			f.requireMigrationDirsTrackRecords()
		})
	}
}
