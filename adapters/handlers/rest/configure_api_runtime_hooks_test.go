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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/cron"
)

// appStateWithOIDC carries the fields runtimeConfigHooks reads while it builds
// the map. The hooks it stores are closures nothing here calls, so the
// collaborators they capture may stay nil.
func appStateWithOIDC(logger *logrus.Logger, crons *cron.Crons, enabled bool) *state.State {
	return &state.State{
		Logger: logger,
		Crons:  crons,
		ServerConfig: &config.WeaviateConfig{Config: config.Config{
			Authentication: config.Authentication{OIDC: config.OIDC{Enabled: enabled}},
		}},
	}
}

func TestRuntimeConfigHooks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	crons, err := cron.NewCrons(t.Context(), logger, func() config.Config { return config.Config{} })
	require.NoError(t, err)

	tests := []struct {
		name     string
		oidc     bool
		wantKeys []string
	}{
		{
			name: "the keys a deployment without OIDC registers",
			wantKeys: []string{
				"AllowedCompressionTypes", "AllowedVectorIndexTypes", "AsyncReplicationDisabled",
				"DefaultQuantization", "DefaultVectorIndexType", "DisableGraphQL",
				"NamespaceCleanup", "ObjectsTTL",
			},
		},
		{
			// OIDC is the one key configuration turns on, so a map asserted
			// without this row would miss it going stale.
			name: "OIDC joins the map once it is enabled",
			oidc: true,
			wantKeys: []string{
				"AllowedCompressionTypes", "AllowedVectorIndexTypes", "AsyncReplicationDisabled",
				"DefaultQuantization", "DefaultVectorIndexType", "DisableGraphQL",
				"NamespaceCleanup", "OIDC", "ObjectsTTL",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hooks, err := runtimeConfigHooks(appStateWithOIDC(logger, crons, tt.oidc), t.Context())

			require.NoError(t, err, "the map a real deployment registers must pass the merge guard")
			assert.Equal(t, tt.wantKeys, slices.Sorted(maps.Keys(hooks)))
		})
	}
}

func TestMergeRuntimeHooks(t *testing.T) {
	hook := func() error { return nil }
	// The real cron keys, so the rows follow hookKey() rather than a literal
	// that can go stale.
	logger, _ := test.NewNullLogger()
	crons, err := cron.NewCrons(t.Context(), logger, func() config.Config { return config.Config{} })
	require.NoError(t, err)

	// The keys postInitRuntimeOverrides registers ahead of the cron merge,
	// taken from the builder itself and stripped of what the merge adds, so a
	// key renamed there cannot leave this test asserting against a stale one.
	registered := func() map[string]func() error {
		hooks, err := runtimeConfigHooks(appStateWithOIDC(logger, crons, true), t.Context())
		require.NoError(t, err)
		for key := range crons.RuntimeConfigHooks() {
			delete(hooks, key)
		}
		return hooks
	}

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
