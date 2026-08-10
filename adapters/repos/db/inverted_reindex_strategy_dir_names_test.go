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
)

// TestMigrationDirName pins the wire-format dir names each strategy produces.
// Changing any string here would silently leave existing on-disk migration
// state in a directory that finalize/debug no longer recognise, so these
// assertions guard against accidental renames.
//
// Every strategy carries a per-migration generation (`_<N>`) appended to
// the dir name. The tests below use generation 7 to exercise the suffix
// composition; the per-strategy base (everything before `_7`) is what
// pins the wire format.
func TestMigrationDirName(t *testing.T) {
	cases := []struct {
		name     string
		got      string
		expected string
	}{
		{
			name:     "MapToBlockmax",
			got:      (&MapToBlockmaxStrategy{generation: 7}).MigrationDirName(),
			expected: "searchable_map_to_blockmax_7",
		},
		{
			name:     "RoaringSetRefresh",
			got:      (&RoaringSetRefreshStrategy{generation: 7}).MigrationDirName(),
			expected: "filterable_roaringset_refresh_7",
		},
		{
			name:     "FilterableToRangeable_noProps",
			got:      (&FilterableToRangeableStrategy{generation: 7}).MigrationDirName(),
			expected: "filterable_to_rangeable_7",
		},
		{
			name:     "FilterableToRangeable_withProps",
			got:      (&FilterableToRangeableStrategy{propNames: []string{"a", "b"}, generation: 7}).MigrationDirName(),
			expected: "filterable_to_rangeable_a_b_7",
		},
		{
			name:     "SearchableRetokenize",
			got:      (&SearchableRetokenizeStrategy{propName: "title", generation: 7}).MigrationDirName(),
			expected: "searchable_retokenize_title_7",
		},
		{
			name:     "FilterableRetokenize",
			got:      (&FilterableRetokenizeStrategy{propName: "title", generation: 7}).MigrationDirName(),
			expected: "filterable_retokenize_title_7",
		},
		{
			name:     "EnableFilterable_noProps",
			got:      (&EnableFilterableStrategy{generation: 7}).MigrationDirName(),
			expected: "enable_filterable_7",
		},
		{
			name:     "EnableFilterable_withProps",
			got:      (&EnableFilterableStrategy{propNames: []string{"a", "b"}, generation: 7}).MigrationDirName(),
			expected: "enable_filterable_a_b_7",
		},
		{
			name:     "EnableSearchable_noProps",
			got:      (&EnableSearchableStrategy{generation: 7}).MigrationDirName(),
			expected: "enable_searchable_7",
		},
		{
			name:     "EnableSearchable_withProps",
			got:      (&EnableSearchableStrategy{propNames: []string{"a", "b"}, generation: 7}).MigrationDirName(),
			expected: "enable_searchable_a_b_7",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.expected {
				t.Fatalf("MigrationDirName mismatch: got %q, want %q", tc.got, tc.expected)
			}
		})
	}
}

// TestFinalizeMigrationSuffixesRecognisesAllStrategies asserts that
// migrationSuffixes returns a non-nil recipe for the dir name produced by
// each strategy's MigrationDirName(). If a new strategy is added and its
// constant is registered in inverted_reindex_strategy_dir_names.go but the
// finalize switch isn't updated, this test fails.
func TestFinalizeMigrationSuffixesRecognisesAllStrategies(t *testing.T) {
	cases := []struct {
		name string
		dir  string
	}{
		{"MapToBlockmax", (&MapToBlockmaxStrategy{}).MigrationDirName()},
		{"RoaringSetRefresh", (&RoaringSetRefreshStrategy{}).MigrationDirName()},
		{"FilterableToRangeable_noProps", (&FilterableToRangeableStrategy{}).MigrationDirName()},
		{"FilterableToRangeable_withProps", (&FilterableToRangeableStrategy{propNames: []string{"p"}}).MigrationDirName()},
		{"SearchableRetokenize", (&SearchableRetokenizeStrategy{propName: "p"}).MigrationDirName()},
		{"FilterableRetokenize", (&FilterableRetokenizeStrategy{propName: "p"}).MigrationDirName()},
		{"EnableFilterable_noProps", (&EnableFilterableStrategy{}).MigrationDirName()},
		{"EnableFilterable_withProps", (&EnableFilterableStrategy{propNames: []string{"p"}}).MigrationDirName()},
		{"EnableSearchable_noProps", (&EnableSearchableStrategy{}).MigrationDirName()},
		{"EnableSearchable_withProps", (&EnableSearchableStrategy{propNames: []string{"p"}}).MigrationDirName()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := migrationSuffixes(tc.dir); got == nil {
				t.Fatalf("migrationSuffixes(%q) = nil, want a recipe", tc.dir)
			}
		})
	}
}

// TestFinalizeMigrationSuffixesUnknown asserts that an unknown dir name
// returns nil, preserving the existing default-branch behaviour.
func TestFinalizeMigrationSuffixesUnknown(t *testing.T) {
	if got := migrationSuffixes("unknown_migration"); got != nil {
		t.Fatalf("migrationSuffixes(unknown) = %+v, want nil", got)
	}
}

// TestMigrationDirsForPropertyIndex_OmitsClassLevelMapToBlockmax pins the
// per-property contract: the class-level MapToBlockmax tracker must NOT be
// returned here (cleanStaleMigrationDirsAt + CleanStalePartialReindexState
// would corrupt the class-level dir on single-property cleanup). The
// blockmax tracker is matched directly in LocalCallbacksDone instead.
func TestMigrationDirsForPropertyIndex_OmitsClassLevelMapToBlockmax(t *testing.T) {
	got := migrationDirsForPropertyIndex("text", "searchable")
	for _, p := range got {
		if p == MigrationDirSearchableMapToBlockmax {
			t.Fatalf("migrationDirsForPropertyIndex(text, searchable) = %v, must NOT include class-level %q",
				got, MigrationDirSearchableMapToBlockmax)
		}
	}
}

// Pins isMigrationDirOf against cross-property prefix collisions (a property
// name may itself be a prefix of another property's tracker dir).
func TestIsMigrationDirOf(t *testing.T) {
	catPrefixes := migrationDirsForPropertyIndex("cat", "filterable")
	require.Contains(t, catPrefixes, "enable_filterable_cat")

	tests := []struct {
		name     string
		dir      string
		prefixes []string
		want     bool
	}{
		{
			name:     "this property's tracker",
			dir:      "enable_filterable_cat_1",
			prefixes: catPrefixes,
			want:     true,
		},
		{
			name:     "a later generation of this property's tracker",
			dir:      "enable_filterable_cat_12",
			prefixes: catPrefixes,
			want:     true,
		},
		{
			name:     "a tracker dir from before generations existed",
			dir:      "enable_filterable_cat",
			prefixes: catPrefixes,
			want:     true,
		},
		{
			name:     "a property whose name extends this one",
			dir:      "enable_filterable_cat_x_1",
			prefixes: catPrefixes,
			want:     false,
		},
		{
			name:     "a property whose name this one extends",
			dir:      "enable_filterable_ca_1",
			prefixes: catPrefixes,
			want:     false,
		},
		{
			name:     "a tracker of the same property under another strategy",
			dir:      "filterable_retokenize_cat_1",
			prefixes: catPrefixes,
			want:     true,
		},
		{
			name:     "another index type's tracker for this property",
			dir:      "enable_searchable_cat_1",
			prefixes: catPrefixes,
			want:     false,
		},
		{
			name:     "the class-level tracker every property shares",
			dir:      "filterable_roaringset_refresh_1",
			prefixes: catPrefixes,
			want:     false,
		},
		{
			name:     "no prefixes to match against",
			dir:      "enable_filterable_cat_1",
			prefixes: nil,
			want:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isMigrationDirOf(tc.dir, tc.prefixes))
		})
	}
}
