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

package namespacing

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func namespaced(ns string) *models.Principal { return &models.Principal{Namespace: ns} }

func global() *models.Principal { return &models.Principal{IsGlobalOperator: true} }

func TestRoleResolverQualifyNameForCreate(t *testing.T) {
	tests := []struct {
		name      string
		principal *models.Principal
		nsEnabled bool
		raw       string
		want      string
		wantErr   bool
	}{
		{"NS-disabled passthrough", namespaced("customer1"), false, "editor", "editor", false},
		{"NS-disabled passthrough keeps colon", global(), false, "weird:name", "weird:name", false},
		{"namespaced short qualifies", namespaced("customer1"), true, "editor", "customer1:editor", false},
		{"namespaced colon input rejected", namespaced("customer1"), true, "customer2:editor", "", true},
		// Built-in names qualify here; the handler rejects them separately.
		{"namespaced built-in name qualifies", namespaced("customer1"), true, "admin", "customer1:admin", false},
		{"global short passes", global(), true, "editor", "editor", false},
		{"global colon input rejected", global(), true, "customer1:editor", "", true},
		{"nil principal NS-enabled passthrough", nil, true, "editor", "editor", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := QualifyRoleNameForCreate(tt.principal, tt.nsEnabled, tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRoleResolverResolveRoleName(t *testing.T) {
	// Both customer1:editor and global editor exist, so local-precedence is exercised.
	existing := map[string]bool{
		"customer1:editor": true,
		"editor":           true,
		"viewer":           true,
	}
	exists := func(name string) (bool, error) { return existing[name], nil }

	tests := []struct {
		name         string
		principal    *models.Principal
		nsEnabled    bool
		raw          string
		wantStored   string
		wantNotFound bool
		wantErr      bool
	}{
		{name: "NS-disabled passthrough", principal: namespaced("customer1"), nsEnabled: false, raw: "editor", wantStored: "editor"},
		{name: "global short stays global", principal: global(), nsEnabled: true, raw: "viewer", wantStored: "viewer"},
		{name: "global colon addresses local", principal: global(), nsEnabled: true, raw: "customer1:editor", wantStored: "customer1:editor"},
		{name: "global absent name returned without existence check", principal: global(), nsEnabled: true, raw: "phantom", wantStored: "phantom"},
		{name: "global absent local addressed without existence check", principal: global(), nsEnabled: true, raw: "customer1:ghost", wantStored: "customer1:ghost"},
		{name: "namespaced short prefers local over existing global", principal: namespaced("customer1"), nsEnabled: true, raw: "editor", wantStored: "customer1:editor"},
		{name: "namespaced short falls through to global", principal: namespaced("customer1"), nsEnabled: true, raw: "viewer", wantStored: "viewer"},
		{name: "namespaced unknown not found", principal: namespaced("customer1"), nsEnabled: true, raw: "ghost", wantNotFound: true},
		{name: "namespaced colon input rejected", principal: namespaced("customer1"), nsEnabled: true, raw: "customer2:editor", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored, err := ResolveRoleName(tt.principal, tt.nsEnabled, tt.raw, exists)
			switch {
			case tt.wantNotFound:
				require.ErrorIs(t, err, ErrRoleNotFound)
			case tt.wantErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.wantStored, stored)
			}
		})
	}

	t.Run("propagates existence-callback error", func(t *testing.T) {
		boom := errors.New("store unavailable")
		failing := func(string) (bool, error) { return false, boom }
		_, err := ResolveRoleName(namespaced("customer1"), true, "editor", failing)
		require.ErrorIs(t, err, boom)
		require.NotErrorIs(t, err, ErrRoleNotFound)
	})
}

func TestRoleResolverQualifyPoliciesForCreate(t *testing.T) {
	tests := []struct {
		name      string
		principal *models.Principal
		nsEnabled bool
		in        []authorization.Policy
		want      []string // expected Resource per policy
		wantErr   bool
	}{
		{
			name:      "NS-disabled no-op",
			principal: namespaced("customer1"),
			nsEnabled: false,
			in:        []authorization.Policy{{Resource: "data/collections/Movies/shards/.*/objects/.*"}},
			want:      []string{"data/collections/Movies/shards/.*/objects/.*"},
		},
		{
			name:      "global no-op",
			principal: global(),
			nsEnabled: true,
			in:        []authorization.Policy{{Resource: "data/collections/Movies/shards/.*/objects/.*"}},
			want:      []string{"data/collections/Movies/shards/.*/objects/.*"},
		},
		{
			name:      "namespaced prefixes collection + role + user segments",
			principal: namespaced("customer1"),
			nsEnabled: true,
			in: []authorization.Policy{
				{Resource: "data/collections/Movies/shards/.*/objects/.*"},
				{Resource: "roles/*"},
				{Resource: "users/bob"},
				{Resource: "cluster/*"}, // not namespaceable
			},
			want: []string{
				"data/collections/customer1:Movies/shards/.*/objects/.*",
				"roles/customer1:*",
				"users/customer1:bob",
				"cluster/*",
			},
		},
		{
			name:      "namespaced prefixes both alias segments",
			principal: namespaced("customer1"),
			nsEnabled: true,
			in:        []authorization.Policy{{Resource: "aliases/collections/Movies/aliases/Films"}},
			want:      []string{"aliases/collections/customer1:Movies/aliases/customer1:Films"},
		},
		{
			name:      "namespaced prefixes wildcard segments",
			principal: namespaced("customer1"),
			nsEnabled: true,
			in: []authorization.Policy{
				{Resource: "users/*"},
				{Resource: "schema/collections/*/shards/#"},
			},
			want: []string{
				"users/customer1:*",
				"schema/collections/customer1:*/shards/#",
			},
		},
		{
			// Write-isolation: a built-in role reference auto-prefixes too.
			name:      "namespaced cannot reference global built-in role",
			principal: namespaced("customer1"),
			nsEnabled: true,
			in:        []authorization.Policy{{Resource: "roles/admin"}},
			want:      []string{"roles/customer1:admin"},
		},
		{
			name:      "namespaced already-qualified collection rejected",
			principal: namespaced("customer1"),
			nsEnabled: true,
			in:        []authorization.Policy{{Resource: "data/collections/customer1:Movies/shards/.*/objects/.*"}},
			wantErr:   true,
		},
		{
			// Atomicity: a later policy's error must leave the earlier,
			// qualifiable policy unmutated.
			name:      "namespaced error leaves earlier policy untouched",
			principal: namespaced("customer1"),
			nsEnabled: true,
			in: []authorization.Policy{
				{Resource: "data/collections/Movies/shards/.*/objects/.*"},
				{Resource: "data/collections/customer1:Other/shards/.*/objects/.*"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := make([]string, len(tt.in))
			for i := range tt.in {
				orig[i] = tt.in[i].Resource
			}
			err := QualifyRolePoliciesForCreate(tt.principal, tt.nsEnabled, tt.in)
			if tt.wantErr {
				require.Error(t, err)
				for i := range tt.in {
					assert.Equal(t, orig[i], tt.in[i].Resource, "policy resource mutated despite error")
				}
				return
			}
			require.NoError(t, err)
			for i := range tt.want {
				assert.Equal(t, tt.want[i], tt.in[i].Resource)
			}
		})
	}
}

func TestRoleResolverFindShortNameConflict(t *testing.T) {
	tests := []struct {
		name      string
		existing  []string
		candidate string
		want      RoleShortNameConflict
	}{
		{"no conflict empty", nil, "customer1:editor", NoRoleConflict},
		{"two namespaces same short coexist", []string{"customer1:editor"}, "customer2:editor", NoRoleConflict},
		{"local duplicate", []string{"customer1:editor"}, "customer1:editor", RoleConflictDuplicate},
		{"local blocked by global reservation", []string{"editor"}, "customer1:editor", RoleConflictGlobal},
		{"global blocked by existing local", []string{"customer1:editor"}, "editor", RoleConflictLocal},
		{"global blocked by multiple locals", []string{"customer1:editor", "customer2:editor"}, "editor", RoleConflictLocal},
		{"global duplicate", []string{"editor"}, "editor", RoleConflictDuplicate},
		{"different short no conflict", []string{"customer1:editor"}, "customer1:viewer", NoRoleConflict},
		// An exact duplicate must win over a cross-type match no matter where in
		// the scan each appears — pin both orderings for each candidate type.
		{"global duplicate wins, dup first", []string{"editor", "customer1:editor"}, "editor", RoleConflictDuplicate},
		{"global duplicate wins, dup last", []string{"customer1:editor", "editor"}, "editor", RoleConflictDuplicate},
		{"local duplicate wins, dup first", []string{"customer1:editor", "editor"}, "customer1:editor", RoleConflictDuplicate},
		{"local duplicate wins, dup last", []string{"editor", "customer1:editor"}, "customer1:editor", RoleConflictDuplicate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FindShortNameConflict(slices.Values(tt.existing), tt.candidate))
		})
	}
}

// TestProjectResourceForNamespace pins assignment-time specialization: a bare
// namespace segment is prefixed with the target namespace, an already-qualified
// segment must already name the target, non-namespaceable resources and the
// empty namespace pass through.
func TestProjectResourceForNamespace(t *testing.T) {
	tests := []struct {
		name      string
		resource  string
		namespace string
		want      string
		wantErr   bool
	}{
		{"bare data collection", "data/collections/Movies/shards/*/objects/*", "customer1", "data/collections/customer1:Movies/shards/*/objects/*", false},
		{"bare schema collection", "schema/collections/Movies/shards/#", "customer1", "schema/collections/customer1:Movies/shards/#", false},
		{"bare role", "roles/editor", "customer1", "roles/customer1:editor", false},
		{"bare user", "users/bob", "customer1", "users/customer1:bob", false},
		{"qualified matching user", "users/customer1:bob", "customer1", "users/customer1:bob", false},
		{"foreign user", "users/customer2:bob", "customer1", "", true},
		{"bare wildcard collection", "data/collections/*/shards/*/objects/*", "customer1", "data/collections/customer1:*/shards/*/objects/*", false},
		{"bare alias both segments", "aliases/collections/Movies/aliases/Myalias", "customer1", "aliases/collections/customer1:Movies/aliases/customer1:Myalias", false},
		{"qualified alias different namespace", "aliases/collections/customer2:Movies/aliases/customer2:Myalias", "customer1", "", true},
		{"non-namespaceable cluster", "99", "customer1", "99", false},
		{"empty namespace passthrough", "data/collections/Movies/shards/*/objects/*", "", "data/collections/Movies/shards/*/objects/*", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProjectResourceForNamespace(tt.resource, tt.namespace)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConfinedNamespace(t *testing.T) {
	tests := []struct {
		name      string
		principal *models.Principal
		want      string
	}{
		{"nil principal is unconfined", nil, ""},
		{"global operator is unconfined", &models.Principal{IsGlobalOperator: true}, ""},
		{"operator with a namespace is still unconfined", &models.Principal{IsGlobalOperator: true, Namespace: "customer1"}, ""},
		{"namespace-less principal is unconfined", &models.Principal{Username: "u"}, ""},
		{"namespaced principal is confined to its namespace", &models.Principal{Namespace: "customer1"}, "customer1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ConfinedNamespace(tt.principal))
		})
	}
}

// TestRoleResolver_OperatorWithNamespaceTreatedAsGlobal pins the defense-in-depth
// contract: should an operator ever carry a namespace, the create/resolve paths
// treat it as unconfined — matching the strip paths via ConfinedNamespace so the
// two predicate families cannot diverge.
func TestRoleResolver_OperatorWithNamespaceTreatedAsGlobal(t *testing.T) {
	op := &models.Principal{Username: "admin", IsGlobalOperator: true, Namespace: "customer1"}

	// No namespace prefix is added on create.
	got, err := QualifyRoleNameForCreate(op, true, "editor")
	require.NoError(t, err)
	assert.Equal(t, "editor", got)

	// A ':'-qualified name passes through (a confined caller's would be rejected),
	// and existence is never consulted for an unconfined caller.
	stored, err := ResolveRoleName(op, true, "customer2:editor", func(string) (bool, error) {
		t.Fatal("exists must not be called for an unconfined caller")
		return false, nil
	})
	require.NoError(t, err)
	assert.Equal(t, "customer2:editor", stored)
}
