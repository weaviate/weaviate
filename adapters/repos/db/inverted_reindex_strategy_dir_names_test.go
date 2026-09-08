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
	"encoding/json"
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
	got := migrationDirPrefixesForIndexType("searchable")
	for _, p := range got {
		if p == MigrationDirSearchableMapToBlockmax {
			t.Fatalf("migrationDirPrefixesForIndexType(searchable) = %v, must NOT include class-level %q",
				got, MigrationDirSearchableMapToBlockmax)
		}
	}
	require.Equal(t, MigrationDirSearchableMapToBlockmax,
		migrationDirsOf("", nil, "text", "searchable").preserving("searchable").classDir,
		"the preserve set must still span it: a completed class-level migration owns live sidecars")
}

// Pins which tracker dirs a (property, index type) cleanup owns: the ones a
// multi-property task named it in, and none of the ones that merely share an
// underscore-joined prefix with it.
func TestMigrationDirScopeMatches(t *testing.T) {
	tests := []struct {
		name string
		dir  string
		// props is what the task recorded in payload.mig. Empty writes no
		// payload, which is what a tracker from before payload.mig looks like.
		props []string
		// emptyPayload writes a payload.mig that parses but names no property,
		// which a truncated recovery record looks like.
		emptyPayload bool
		// corruptPayload writes a payload.mig that does not parse at all.
		corruptPayload bool
		// propName is the property being swept; "cat" unless set.
		propName  string
		indexType string
		preserve  bool
		want      bool
	}{
		{
			name: "this property's tracker",
			dir:  "enable_filterable_cat_1", props: []string{"cat"},
			want: true,
		},
		{
			name: "a later generation of this property's tracker",
			dir:  "enable_filterable_cat_12", props: []string{"cat"},
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
			name: "a two-property task, swept by its first property",
			dir:  "enable_filterable_a_b_1", props: []string{"a", "b"},
			propName: "a", want: true,
		},
		{
			name: "a two-property task, swept by its second property",
			dir:  "enable_filterable_a_b_1", props: []string{"a", "b"},
			propName: "b", want: true,
		},
		{
			name: "a two-property task that does not name this property",
			dir:  "enable_filterable_a_b_1", props: []string{"a", "b"},
			propName: "c", want: false,
		},
		// The same name shape read the other way: one property called "a_b".
		// Only the payload tells the two apart.
		{
			name: "a single property whose name contains the join character",
			dir:  "enable_filterable_a_b_1", props: []string{"a_b"},
			propName: "a", want: false,
		},
		// The payload names this property, but the dir name does not rebuild
		// from that payload's list. Only a writer that skipped
		// [migrationDirWithProps], which sorts, leaves this shape; a dir this
		// cleanup cannot account for could be another property's tracker.
		{
			name: "a payload naming this property, in a dir name it does not rebuild",
			dir:  "enable_filterable_b_a_1", props: []string{"a", "b"},
			propName: "a", want: false,
		},
		// An intact payload decides on its own, so the name-token fallback that
		// preserves this same dir without a payload does not apply here.
		{
			name: "a payload naming this property, in a dir name it does not rebuild, in the preserve set",
			dir:  "enable_filterable_b_a_1", props: []string{"a", "b"},
			propName: "a", preserve: true, want: false,
		},
		{
			name: "a property whose name extends this one",
			dir:  "enable_filterable_cat_x_1", props: []string{"cat_x"},
			want: false,
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
		// #10675: sidecar deletion is not payload-gated, so preservation must
		// still catch this tracker even without a payload.
		{
			name:     "a two-property task with no payload, in the preserve set",
			dir:      "enable_filterable_a_b_1",
			propName: "a", preserve: true, want: true,
		},
		// Preserve guessing stays scoped to this property; not the whole prefix.
		{
			name:     "another property's task with no payload, in the preserve set",
			dir:      "enable_filterable_other_1",
			propName: "cat", preserve: true, want: false,
		},
		// Over-preserved on purpose: ambiguous between "cat"+"x" and "cat_x".
		{
			name:     "a property whose name extends this one, with no payload, in the preserve set",
			dir:      "enable_filterable_cat_x_1",
			propName: "cat", preserve: true, want: true,
		},
		{
			name:     "a property whose name this one extends across the join character, in the preserve set",
			dir:      "enable_filterable_b_a_1",
			propName: "a", preserve: true, want: true,
		},
		// The swept property as a middle "_"-token of a payload-less name. No
		// sorted multi-property name has this shape, but a single property
		// named "x_a_y" does; only the middle-token clause of
		// namesPropertyToken catches it.
		{
			name:     "a single property carrying this property mid-token, with no payload, in the preserve set",
			dir:      "enable_filterable_x_a_y_1",
			propName: "a", preserve: true, want: true,
		},
		// [migrationDirWithProps] sorts, so the only name a writer leaves that
		// matches mid-list is the task's own middle property.
		{
			name:     "the middle property of a three-property task, with no payload, in the preserve set",
			dir:      "enable_filterable_a_b_c_1",
			propName: "b", preserve: true, want: true,
		},
		{
			name:     "the middle property of a three-property task, with no payload",
			dir:      "enable_filterable_a_b_c_1",
			propName: "b", want: false,
		},
		// An empty payload decides nothing, so falls back to the name like a
		// missing payload does.
		{
			name:         "a payload that names no property at all",
			dir:          "enable_filterable_cat_1",
			emptyPayload: true, want: true,
		},
		// An unparseable payload keeps the narrow fallback for deletion —
		// deleting on a guess could remove another property's tracker. The
		// unloaded-shard gate fails open on it instead; see
		// [migrationDirScope.inScopeFailingOpen].
		{
			name:           "a two-property shape with an unparseable payload",
			dir:            "enable_filterable_a_b_1",
			corruptPayload: true, propName: "a", want: false,
		},
		{
			name:           "a two-property shape with an unparseable payload, in the preserve set",
			dir:            "enable_filterable_a_b_1",
			corruptPayload: true, propName: "a", preserve: true, want: true,
		},
		{
			name: "a property whose name this one extends",
			dir:  "enable_filterable_ca_1", props: []string{"ca"},
			want: false,
		},
		{
			name: "a tracker of the same property under another strategy",
			dir:  "filterable_retokenize_cat_1", props: []string{"cat"},
			want: true,
		},
		{
			name: "another index type's tracker for this property",
			dir:  "enable_searchable_cat_1", props: []string{"cat"},
			want: false,
		},
		{
			name: "the class-level tracker every property shares",
			dir:  "filterable_roaringset_refresh_1",
			want: false,
		},
		{
			name:     "the class-level tracker, in the preserve set",
			dir:      "filterable_roaringset_refresh_1",
			preserve: true, want: true,
		},
		{
			name: "an index type with no strategies",
			dir:  "enable_filterable_cat_1", props: []string{"cat"},
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
			if len(tc.props) > 0 || tc.emptyPayload {
				payload, err := json.Marshal(map[string]any{
					"payload": map[string]any{"properties": tc.props},
				})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, reindexRecoveryPayloadFile), payload, 0o644))
			}
			if tc.corruptPayload {
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, reindexRecoveryPayloadFile),
					[]byte("not a recovery record"), 0o644))
			}

			scope := migrationDirsOf(lsm, nil, propName, indexType)
			if tc.preserve {
				scope = scope.preserving(indexType)
			}
			require.Equal(t, tc.want, scope.inScope(tc.dir))
		})
	}
}
