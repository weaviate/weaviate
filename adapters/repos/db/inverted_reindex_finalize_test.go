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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// Tests for the migration dir-name helpers added for
// https://github.com/weaviate/weaviate/issues/10675. The functions under test
// live in inverted_reindex_strategy_dir_names.go.

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

type plantedMigration struct {
	taskVersion uint64
	code        MigrationStrategyCode
	prop        string
	state       MigrationState
	canonical   string
	running     bool
}

func (p plantedMigration) subject() MigrationSubject {
	subject := testMigrationSubject(p.taskVersion, p.code, p.prop)
	if p.canonical != "" {
		dirs := subject.Props[p.prop]
		dirs.Canonical = p.canonical
		subject.Props[p.prop] = dirs
	}
	return subject
}

func (p plantedMigration) record(t *testing.T) MigrationRecord {
	t.Helper()
	return newMigrationRecordAt(t, p.subject(), p.state)
}

func TestReconcileConvergesEveryMigrationOnAShard(t *testing.T) {
	tests := []struct {
		name          string
		plant         []plantedMigration
		wantCanonical map[string]string
		wantStaged    []string
	}{
		{
			name: "two strategies on one property settle into their own buckets",
			plant: []plantedMigration{
				{taskVersion: 10, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateMerged},
				{taskVersion: 11, code: StrategyCodeFilterableRetokenize, prop: "title", state: MigrationStateMerged},
			},
			wantCanonical: map[string]string{
				"property_title_searchable": "property_title__g10_ingest",
				"property_title":            "property_title__g11_ingest",
			},
		},
		{
			name: "three properties on three strategies settle together",
			plant: []plantedMigration{
				{taskVersion: 20, code: StrategyCodeSearchableRetokenize, prop: "alpha", state: MigrationStateMerged},
				{taskVersion: 21, code: StrategyCodeEnableFilterable, prop: "beta", state: MigrationStateSwapped},
				{taskVersion: 22, code: StrategyCodeFilterableToRangeable, prop: "gamma", state: MigrationStateMerged},
			},
			wantCanonical: map[string]string{
				"property_alpha_searchable": "property_alpha__g20_ingest",
				"property_beta":             "property_beta__g21_ingest",
				"property_gamma_rangeable":  "property_gamma__g22_ingest",
			},
		},
		{
			name: "a committed migration settles beside one still rebuilding",
			plant: []plantedMigration{
				{taskVersion: 30, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateMerged},
				{
					taskVersion: 31, code: StrategyCodeSearchableRetokenize, prop: "body",
					state: MigrationStateIterating, running: true,
				},
			},
			wantCanonical: map[string]string{
				"property_title_searchable": "property_title__g30_ingest",
				"property_body_searchable":  "property_body_searchable",
			},
			wantStaged: []string{"property_body__g31_ingest"},
		},
		{
			name: "the newer migration wins the bucket even though the older one already flipped",
			plant: []plantedMigration{
				{taskVersion: 40, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateSwapped},
				{taskVersion: 41, code: StrategyCodeSearchableRetokenize, prop: "title", state: MigrationStateMerged},
			},
			wantCanonical: map[string]string{"property_title_searchable": "property_title__g41_ingest"},
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
				f.mkdirs(append(migrationOwnedDirs(subject), subject.Props[planted.prop].Canonical)...)
				f.put(planted.record(t))
				props = append(props, planted.prop)
				if planted.running {
					f.tasks = append(f.tasks,
						testTask(subject.TaskID, planted.taskVersion, distributedtask.TaskStatusStarted))
				}
			}
			f.class = testClassWithTokenization(models.PropertyTokenizationLowercase, props...)

			f.reconcile()
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
