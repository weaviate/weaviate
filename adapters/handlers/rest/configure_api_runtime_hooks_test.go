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

package rest

import (
	"maps"
	"slices"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/cron"
)

func TestMergeRuntimeHooks(t *testing.T) {
	hook := func() error { return nil }
	// The keys postInitRuntimeOverrides registers ahead of the cron merge.
	registered := func() map[string]func() error {
		return map[string]func() error{
			"OIDC":                     hook,
			"AsyncReplicationDisabled": hook,
			"DisableGraphQL":           hook,
			"AllowedVectorIndexTypes":  hook,
			"AllowedCompressionTypes":  hook,
			"DefaultVectorIndexType":   hook,
			"DefaultQuantization":      hook,
		}
	}
	// The real cron keys, so the row follows hookKey() rather than a literal
	// that can go stale.
	logger, _ := test.NewNullLogger()
	crons, err := cron.NewCrons(t.Context(), logger, func() config.Config { return config.Config{} })
	require.NoError(t, err)

	tests := []struct {
		name    string
		dst     map[string]func() error
		src     map[string]func() error
		wantErr string
	}{
		{
			name: "the cron keys against the keys registered ahead of them",
			dst:  registered(),
			src:  crons.RuntimeConfigHooks(),
		},
		{
			// mergeRuntimeHooks walks src, so the keys written by hand only
			// reach the guard from this side.
			name: "the keys registered by hand ahead of the merge",
			dst:  map[string]func() error{},
			src:  registered(),
		},
		{
			name:    "a cron key colliding with a key registered ahead of it",
			dst:     registered(),
			src:     map[string]func() error{"OIDC": hook},
			wantErr: "already registered",
		},
		{
			name:    "a key registered ahead of the merge colliding with a cron key",
			dst:     map[string]func() error{"NamespaceCleanup": hook},
			src:     map[string]func() error{"NamespaceCleanup": hook},
			wantErr: "already registered",
		},
		{
			name:    "a key matching no field at all",
			dst:     registered(),
			src:     map[string]func() error{"NoSuchKnob": hook},
			wantErr: "prefixes no runtime config field",
		},
		{
			// NamespaceCleanupInterval is the field; the match is by prefix,
			// so naming its tail matches nothing.
			name:    "a key contained in a field name but not prefixing it",
			dst:     registered(),
			src:     map[string]func() error{"CleanupInterval": hook},
			wantErr: "prefixes no runtime config field",
		},
		{
			// ObjectsTTLz prefixes no field and sorts after the good key, so
			// a merge that wrote as it walked would leave the good one behind.
			name:    "a good key merged alongside a typo'd one",
			dst:     registered(),
			src:     map[string]func() error{"NamespaceCleanup": hook, "ObjectsTTLz": hook},
			wantErr: "prefixes no runtime config field",
		},
		{
			name:    "an empty key",
			dst:     registered(),
			src:     map[string]func() error{"": hook},
			wantErr: "prefixes every field",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := slices.Sorted(maps.Keys(tt.dst))

			err := mergeRuntimeHooks(tt.dst, tt.src)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Equal(t, before, slices.Sorted(maps.Keys(tt.dst)),
					"a refused merge must leave the hook map as it was")
				return
			}
			require.NoError(t, err)
			for key := range tt.src {
				assert.Contains(t, tt.dst, key)
			}
		})
	}
}
