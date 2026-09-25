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

func TestMigrationDirsForPropertyIndex_OmitsClassLevelMapToBlockmax(t *testing.T) {
	got := migrationDirPrefixesForIndexType("searchable")
	for _, p := range got {
		if p == MigrationDirSearchableMapToBlockmax {
			t.Fatalf("migrationDirPrefixesForIndexType(searchable) = %v, must NOT include class-level %q",
				got, MigrationDirSearchableMapToBlockmax)
		}
	}
}

// Pins which tracker dirs a (property, index type) cleanup owns: the ones a
// multi-property task named it in, and none of the ones that merely share an
// underscore-joined prefix with it.
func TestMigrationDirScopeMatches(t *testing.T) {
	tests := []struct {
		name string
		dir  string
		// propName is the property being swept; "cat" unless set.
		propName  string
		indexType string
		want      bool
	}{
		{
			name: "this property's tracker",
			dir:  "enable_filterable_cat_1",
			want: true,
		},
		{
			name: "a later generation of this property's tracker",
			dir:  "enable_filterable_cat_12",
			want: true,
		},
		{
			name: "a tracker dir with no generation suffix is its own base",
			dir:  "enable_filterable_cat",
			want: true,
		},
		// The [migrationDirScope] ambiguity kept deliberately: with no payload
		// this is either "cat" generation 2 or property "cat_2"'s tracker.
		{
			name: "a generation suffix with no payload to tell it from a property name",
			dir:  "enable_filterable_cat_2",
			want: true,
		},
		{
			name:     "a two-property task that does not name this property",
			dir:      "enable_filterable_a_b_1",
			propName: "c", want: false,
		},
		// Without a payload the name is all there is, and it is ambiguous. Not
		// matching is the end that cannot delete another property's state.
		{
			name: "a property whose name extends this one, with no payload",
			dir:  "enable_filterable_cat_x_1",
			want: false,
		},
		{
			name:     "a two-property task with no payload",
			dir:      "enable_filterable_a_b_1",
			propName: "a", want: false,
		},
		{
			name:     "another property's task with no payload",
			dir:      "enable_filterable_other_1",
			propName: "cat", want: false,
		},
		{
			name:     "a property whose name this one extends across the join character",
			dir:      "enable_filterable_b_a_1",
			propName: "a", want: false,
		},
		{
			name:     "a single property carrying this property mid-token, with no payload",
			dir:      "enable_filterable_x_a_y_1",
			propName: "a", want: false,
		},
		{
			name:     "the middle property of a three-property task, with no payload",
			dir:      "enable_filterable_a_b_c_1",
			propName: "b", want: false,
		},
		{
			name: "a property whose name this one extends",
			dir:  "enable_filterable_ca_1",
			want: false,
		},
		{
			name: "a tracker of the same property under another strategy",
			dir:  "filterable_retokenize_cat_1",
			want: true,
		},
		{
			name: "another index type's tracker for this property",
			dir:  "enable_searchable_cat_1",
			want: false,
		},
		{
			name: "the class-level tracker every property shares",
			dir:  "filterable_roaringset_refresh_1",
			want: false,
		},
		{
			name:      "an index type with no strategies",
			dir:       "enable_filterable_cat_1",
			indexType: "an-index-type-this-build-does-not-know", want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			propName := tc.propName
			if propName == "" {
				propName = "cat"
			}
			indexType := tc.indexType
			if indexType == "" {
				indexType = "filterable"
			}
			lsm := t.TempDir()
			dir := filepath.Join(lsm, ".migrations", tc.dir)
			require.NoError(t, os.MkdirAll(dir, 0o755))

			require.Equal(t, tc.want,
				migrationDirsOf(lsm, propName, indexType).inScope(tc.dir))
		})
	}
}
