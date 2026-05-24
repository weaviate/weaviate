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

	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
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
			got:      (&reindex.MapToBlockmaxStrategy{generation: 7}).MigrationDirName(),
			expected: "searchable_map_to_blockmax_7",
		},
		{
			name:     "RoaringSetRefresh",
			got:      (&reindex.RoaringSetRefreshStrategy{generation: 7}).MigrationDirName(),
			expected: "filterable_roaringset_refresh_7",
		},
		{
			name:     "FilterableToRangeable_noProps",
			got:      (&reindex.FilterableToRangeableStrategy{generation: 7}).MigrationDirName(),
			expected: "filterable_to_rangeable_7",
		},
		{
			name:     "FilterableToRangeable_withProps",
			got:      (&reindex.FilterableToRangeableStrategy{propNames: []string{"a", "b"}, generation: 7}).MigrationDirName(),
			expected: "filterable_to_rangeable_a_b_7",
		},
		{
			name:     "SearchableRetokenize",
			got:      (&reindex.SearchableRetokenizeStrategy{propName: "title", generation: 7}).MigrationDirName(),
			expected: "searchable_retokenize_title_7",
		},
		{
			name:     "FilterableRetokenize",
			got:      (&reindex.FilterableRetokenizeStrategy{propName: "title", generation: 7}).MigrationDirName(),
			expected: "filterable_retokenize_title_7",
		},
		{
			name:     "EnableFilterable_noProps",
			got:      (&reindex.EnableFilterableStrategy{generation: 7}).MigrationDirName(),
			expected: "enable_filterable_7",
		},
		{
			name:     "EnableFilterable_withProps",
			got:      (&reindex.EnableFilterableStrategy{propNames: []string{"a", "b"}, generation: 7}).MigrationDirName(),
			expected: "enable_filterable_a_b_7",
		},
		{
			name:     "EnableSearchable_noProps",
			got:      (&reindex.EnableSearchableStrategy{generation: 7}).MigrationDirName(),
			expected: "enable_searchable_7",
		},
		{
			name:     "EnableSearchable_withProps",
			got:      (&reindex.EnableSearchableStrategy{propNames: []string{"a", "b"}, generation: 7}).MigrationDirName(),
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
		{"MapToBlockmax", (&reindex.MapToBlockmaxStrategy{}).MigrationDirName()},
		{"RoaringSetRefresh", (&reindex.RoaringSetRefreshStrategy{}).MigrationDirName()},
		{"FilterableToRangeable_noProps", (&reindex.FilterableToRangeableStrategy{}).MigrationDirName()},
		{"FilterableToRangeable_withProps", (&reindex.FilterableToRangeableStrategy{propNames: []string{"p"}}).MigrationDirName()},
		{"SearchableRetokenize", (&reindex.SearchableRetokenizeStrategy{propName: "p"}).MigrationDirName()},
		{"FilterableRetokenize", (&reindex.FilterableRetokenizeStrategy{propName: "p"}).MigrationDirName()},
		{"EnableFilterable_noProps", (&reindex.EnableFilterableStrategy{}).MigrationDirName()},
		{"EnableFilterable_withProps", (&reindex.EnableFilterableStrategy{propNames: []string{"p"}}).MigrationDirName()},
		{"EnableSearchable_noProps", (&reindex.EnableSearchableStrategy{}).MigrationDirName()},
		{"EnableSearchable_withProps", (&reindex.EnableSearchableStrategy{propNames: []string{"p"}}).MigrationDirName()},
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
