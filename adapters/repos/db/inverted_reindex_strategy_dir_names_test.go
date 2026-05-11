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
)

// TestMigrationDirName pins the wire-format dir names each strategy produces.
// Changing any string here would silently leave existing on-disk migration
// state in a directory that finalize/debug no longer recognise, so these
// assertions guard against accidental renames.
func TestMigrationDirName(t *testing.T) {
	cases := []struct {
		name     string
		got      string
		expected string
	}{
		{
			name:     "MapToBlockmax",
			got:      (&MapToBlockmaxStrategy{}).MigrationDirName(),
			expected: "searchable_map_to_blockmax",
		},
		{
			name:     "RoaringSetRefresh",
			got:      (&RoaringSetRefreshStrategy{}).MigrationDirName(),
			expected: "filterable_roaringset_refresh",
		},
		{
			name:     "FilterableToRangeable_noProps",
			got:      (&FilterableToRangeableStrategy{}).MigrationDirName(),
			expected: "filterable_to_rangeable",
		},
		{
			name:     "FilterableToRangeable_withProps",
			got:      (&FilterableToRangeableStrategy{propNames: []string{"a", "b"}}).MigrationDirName(),
			expected: "filterable_to_rangeable_a_b",
		},
		{
			name:     "SearchableRetokenize",
			got:      (&SearchableRetokenizeStrategy{propName: "title"}).MigrationDirName(),
			expected: "searchable_retokenize_title",
		},
		{
			name:     "FilterableRetokenize",
			got:      (&FilterableRetokenizeStrategy{propName: "title"}).MigrationDirName(),
			expected: "filterable_retokenize_title",
		},
		{
			name:     "EnableFilterable_noProps",
			got:      (&EnableFilterableStrategy{}).MigrationDirName(),
			expected: "enable_filterable",
		},
		{
			name:     "EnableFilterable_withProps",
			got:      (&EnableFilterableStrategy{propNames: []string{"a", "b"}}).MigrationDirName(),
			expected: "enable_filterable_a_b",
		},
		{
			name:     "EnableSearchable_noProps",
			got:      (&EnableSearchableStrategy{}).MigrationDirName(),
			expected: "enable_searchable",
		},
		{
			name:     "EnableSearchable_withProps",
			got:      (&EnableSearchableStrategy{propNames: []string{"a", "b"}}).MigrationDirName(),
			expected: "enable_searchable_a_b",
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
