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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entschema "github.com/weaviate/weaviate/entities/schema"
)

func TestEnforceNamespaceStartupInvariants(t *testing.T) {
	qualified := func(ns, class string) string {
		return ns + entschema.NamespaceSeparator + class
	}

	tests := []struct {
		name       string
		enabled    bool
		classNames []string
		nsCount    int
		wantErr    bool
		errSubstr  string
	}{
		{
			name:       "enabled, fresh cluster",
			enabled:    true,
			classNames: nil,
			nsCount:    0,
		},
		{
			name:       "enabled, only namespaced classes",
			enabled:    true,
			classNames: []string{qualified("tenant1", "Movie")},
			nsCount:    1,
		},
		{
			name:       "enabled, non-namespaced class present",
			enabled:    true,
			classNames: []string{"Movie"},
			nsCount:    0,
			wantErr:    true,
			errSubstr:  "non-namespaced collection",
		},
		{
			name:       "enabled, mixed but contains legacy class",
			enabled:    true,
			classNames: []string{qualified("tenant1", "Movie"), "Legacy"},
			nsCount:    1,
			wantErr:    true,
			errSubstr:  "Legacy",
		},
		{
			name:       "disabled, fresh cluster",
			enabled:    false,
			classNames: nil,
			nsCount:    0,
		},
		{
			name:       "disabled, legacy classes only",
			enabled:    false,
			classNames: []string{"Movie", "Article"},
			nsCount:    0,
		},
		{
			name:       "disabled, namespace entities exist",
			enabled:    false,
			classNames: nil,
			nsCount:    3,
			wantErr:    true,
			errSubstr:  "namespace entities",
		},
		{
			name:       "disabled, namespace-qualified class exists (defense in depth)",
			enabled:    false,
			classNames: []string{qualified("tenant1", "Movie")},
			nsCount:    0,
			wantErr:    true,
			errSubstr:  "namespace-qualified collection",
		},
		{
			name:       "disabled, mixed classes where qualified branch wins",
			enabled:    false,
			classNames: []string{"Movie", qualified("tenant1", "X")},
			nsCount:    0,
			wantErr:    true,
			errSubstr:  "namespace-qualified collection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := enforceNamespaceStartupInvariants(tt.enabled, tt.classNames, tt.nsCount)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				return
			}
			require.NoError(t, err)
		})
	}
}
