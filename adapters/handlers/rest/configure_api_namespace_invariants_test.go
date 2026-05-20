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
		name                         string
		enabled                      bool
		lsmSkipWriteClassNameEnabled bool
		maxReplicationFac            int
		classNames                   []string
		nsCount                      int
		wantErr                      bool
		errSubstr                    string
	}{
		{
			name:                         "enabled, fresh cluster",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: true,
			maxReplicationFac:            1,
		},
		{
			name:                         "enabled, only namespaced classes",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: true,
			maxReplicationFac:            1,
			classNames:                   []string{qualified("tenant1", "Movie")},
			nsCount:                      1,
		},
		{
			name:                         "enabled, non-namespaced class present",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: true,
			maxReplicationFac:            1,
			classNames:                   []string{"Movie"},
			wantErr:                      true,
			errSubstr:                    "non-namespaced collection",
		},
		{
			name:                         "enabled, mixed but contains legacy class",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: true,
			maxReplicationFac:            1,
			classNames:                   []string{qualified("tenant1", "Movie"), "Legacy"},
			nsCount:                      1,
			wantErr:                      true,
			errSubstr:                    "Legacy",
		},
		{
			name:                         "enabled but lsm skip write classname disabled",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: false,
			maxReplicationFac:            1,
			wantErr:                      true,
			errSubstr:                    "internal invariant violated",
		},
		{
			name:                         "enabled, MaximumFactor unset",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: true,
			maxReplicationFac:            0,
			wantErr:                      true,
			errSubstr:                    "REPLICATION_MAXIMUM_FACTOR",
		},
		{
			name:                         "enabled, MaximumFactor=3",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: true,
			maxReplicationFac:            3,
			wantErr:                      true,
			errSubstr:                    "REPLICATION_MAXIMUM_FACTOR",
		},
		{
			name:    "disabled, fresh cluster",
			enabled: false,
		},
		{
			name:              "disabled, MaximumFactor>1 is allowed",
			enabled:           false,
			maxReplicationFac: 3,
		},
		{
			name:       "disabled, legacy classes only",
			enabled:    false,
			classNames: []string{"Movie", "Article"},
		},
		{
			name:      "disabled, namespace entities exist",
			enabled:   false,
			nsCount:   3,
			wantErr:   true,
			errSubstr: "namespace entities",
		},
		{
			name:       "disabled, namespace-qualified class exists (defense in depth)",
			enabled:    false,
			classNames: []string{qualified("tenant1", "Movie")},
			wantErr:    true,
			errSubstr:  "namespace-qualified collection",
		},
		{
			name:       "disabled, mixed classes where qualified branch wins",
			enabled:    false,
			classNames: []string{"Movie", qualified("tenant1", "X")},
			wantErr:    true,
			errSubstr:  "namespace-qualified collection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := enforceNamespaceStartupInvariants(tt.enabled, tt.lsmSkipWriteClassNameEnabled, tt.maxReplicationFac, tt.classNames, tt.nsCount)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				return
			}
			require.NoError(t, err)
		})
	}
}
