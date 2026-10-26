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

package lsmkv

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGlobalBucketRegistry_RemoveByPrefixes(t *testing.T) {
	// Absolute, separator-correct paths so the boundary check is exercised the
	// same way it is in production (registry keys are absolute bucket paths).
	base := filepath.Join(string(filepath.Separator)+"var", "lib", "weaviate", "MyClass")
	t1lsm := filepath.Join(base, "t1", "lsm")
	t10lsm := filepath.Join(base, "t10", "lsm")
	lsm := func(tenant string) string { return filepath.Join(base, tenant, "lsm") }
	id := func(tenant string) string { return filepath.Join(lsm(tenant), "property__id") }

	tests := []struct {
		name     string
		seed     []string
		prefixes []string
		wantOut  []string // must remain registered after RemoveByPrefix
		wantIn   []string // must be purged (redundant with seed\wantOut, kept explicit for clarity)
	}{
		{
			name: "separator boundary: t1 (the dir itself and its children) must not purge t10",
			seed: []string{
				t1lsm,
				filepath.Join(t1lsm, "property__id"),
				filepath.Join(t10lsm, "property__id"),
				t10lsm,
			},
			prefixes: []string{t1lsm},
			wantOut:  []string{filepath.Join(t10lsm, "property__id"), t10lsm},
			wantIn:   []string{t1lsm, filepath.Join(t1lsm, "property__id")},
		},
		{
			name: "does not purge a string-prefix sibling that is not a path child",
			// "<t1lsm>_bak" shares the textual prefix t1lsm but is a different
			// path (no separator boundary), so it must survive.
			seed: []string{
				filepath.Join(t1lsm, "property__id"),
				t1lsm + "_bak",
			},
			prefixes: []string{t1lsm},
			wantOut:  []string{t1lsm + "_bak"},
			wantIn:   []string{filepath.Join(t1lsm, "property__id")},
		},
		{
			name:     "purges the union of several tenants in one call",
			seed:     []string{id("t1"), id("t2"), id("t3"), id("t10")},
			prefixes: []string{lsm("t1"), lsm("t2"), lsm("t3")},
			wantIn:   []string{id("t1"), id("t2"), id("t3")},
			wantOut:  []string{id("t10")}, // t1 !⊑ t10 boundary holds across the batch
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Balance every seed on all paths so this test cannot pollute the
			// process-global registry that sibling tests share.
			for _, p := range tt.seed {
				require.NoError(t, GlobalBucketRegistry.TryAdd(p))
			}
			t.Cleanup(func() {
				for _, p := range tt.seed {
					GlobalBucketRegistry.Remove(p)
				}
			})

			GlobalBucketRegistry.RemoveByPrefixes(tt.prefixes...)

			// A purged key is re-addable (TryAdd succeeds); a surviving key is
			// still claimed (TryAdd fails). TryAdd of a purged key re-registers
			// it, but the Cleanup above removes every seeded key regardless.
			for _, p := range tt.wantIn {
				require.NoError(t, GlobalBucketRegistry.TryAdd(p),
					"%s should have been purged by RemoveByPrefixes(%v)", p, tt.prefixes)
			}
			for _, p := range tt.wantOut {
				require.ErrorIs(t, GlobalBucketRegistry.TryAdd(p), ErrBucketAlreadyRegistered,
					"%s must survive RemoveByPrefixes(%v) (over-match)", p, tt.prefixes)
			}
		})
	}
}
