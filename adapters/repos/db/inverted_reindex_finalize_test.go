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
