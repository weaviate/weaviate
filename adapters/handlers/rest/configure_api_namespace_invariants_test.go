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
		roleNames                    []string
		policyResources              []string
		groupingSubjects             []string
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
		{
			name:             "disabled, clean RBAC rows",
			enabled:          false,
			roleNames:        []string{"admin", "viewer", "editor"},
			policyResources:  []string{"data/collections/Movies/shards/*/objects/*", "roles/editor"},
			groupingSubjects: []string{"db:alice", "oidc:bob", "group:engineers"},
		},
		{
			name:      "disabled, namespace-qualified role exists",
			enabled:   false,
			roleNames: []string{"admin", qualified("tenant1", "editor")},
			wantErr:   true,
			errSubstr: "namespace-qualified role",
		},
		{
			name:            "disabled, namespace-qualified policy resource exists",
			enabled:         false,
			policyResources: []string{"data/collections/tenant1:Movies/shards/*/objects/*"},
			wantErr:         true,
			errSubstr:       "namespace-qualified role permission",
		},
		{
			// An OIDC username may legitimately contain ':', so a users/<id> or
			// groups/<type>/<name> permission resource carrying a colon is not a
			// namespace qualifier and must not fail startup.
			name:            "disabled, colon-bearing opaque-id permission resources are fine",
			enabled:         false,
			policyResources: []string{"users/oidc:alice", "groups/oidc/team:eng"},
		},
		{
			// A colon-bearing users/<id> or groups/<type>/<name> is a global
			// opaque id, not namespaced state, so it must not fail startup.
			name:            "disabled, namespace-looking opaque-id resources are exempt",
			enabled:         false,
			policyResources: []string{"users/customer1:alice", "groups/db/customer1:team"},
		},
		{
			// An exempt opaque-id must not mask a qualified resource in the same role.
			name:            "disabled, qualified resource alongside exempt opaque-id still caught",
			enabled:         false,
			policyResources: []string{"users/customer1:alice", "roles/tenant1:editor"},
			wantErr:         true,
			errSubstr:       "namespace-qualified role permission",
		},
		{
			// A namespace-qualified roles/<role> permission resource is still caught;
			// only opaque-id (users/groups) resources are exempt from the colon check.
			name:            "disabled, namespace-qualified roles permission resource still caught",
			enabled:         false,
			policyResources: []string{"roles/tenant1:editor"},
			wantErr:         true,
			errSubstr:       "namespace-qualified role permission",
		},
		{
			name:             "disabled, grouping subject for a namespaced principal",
			enabled:          false,
			groupingSubjects: []string{"db:alice", "db:tenant1:bob"},
			wantErr:          true,
			errSubstr:        "namespace-qualified principal",
		},
		{
			name:             "disabled, bare and group subjects are fine",
			enabled:          false,
			groupingSubjects: []string{"db:alice", "oidc:bob", "group:tenant1"},
		},
		{
			// A group is global even when its name carries a colon, so it must
			// not be read as a namespaced principal.
			name:             "disabled, namespace-named group is fine",
			enabled:          false,
			groupingSubjects: []string{"group:tenant1:team"},
		},
		{
			// OIDC usernames may legitimately contain ':', so an oidc: subject
			// cannot be distinguished from a namespaced one and must not fail
			// startup — only db: subjects (whose names forbid ':') are inspected.
			name:             "disabled, colon-bearing oidc subject is fine",
			enabled:          false,
			groupingSubjects: []string{"oidc:foo:bar"},
		},
		{
			name:       "disabled, multiple violation kinds still fails",
			enabled:    false,
			roleNames:  []string{qualified("tenant1", "editor")},
			classNames: []string{qualified("tenant1", "Movie")},
			wantErr:    true,
		},
		{
			name:                         "enabled, qualified RBAC rows are normal",
			enabled:                      true,
			lsmSkipWriteClassNameEnabled: true,
			maxReplicationFac:            1,
			roleNames:                    []string{qualified("tenant1", "editor")},
			policyResources:              []string{"data/collections/tenant1:Movies/shards/*/objects/*"},
			groupingSubjects:             []string{"db:tenant1:bob"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := enforceNamespaceStartupInvariants(tt.enabled, tt.lsmSkipWriteClassNameEnabled, tt.maxReplicationFac, tt.classNames, tt.nsCount, tt.roleNames, tt.policyResources, tt.groupingSubjects)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				return
			}
			require.NoError(t, err)
		})
	}
}
