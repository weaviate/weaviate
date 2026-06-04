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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// deepCopyPermission returns a fully independent clone of p via JSON
// round-trip. Used by mutation-sensitive tests so the snapshot does not
// share any pointers (sub-struct pointers and *string fields) with the
// input the function under test sees.
func deepCopyPermission(t *testing.T, p *models.Permission) *models.Permission {
	t.Helper()
	if p == nil {
		return nil
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)
	out := &models.Permission{}
	require.NoError(t, json.Unmarshal(b, out))
	return out
}

// deepCopyRoles is the slice variant of deepCopyPermission for role tables.
func deepCopyRoles(t *testing.T, roles []*models.Role) []*models.Role {
	t.Helper()
	if roles == nil {
		return nil
	}
	b, err := json.Marshal(roles)
	require.NoError(t, err)
	out := []*models.Role{}
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

var (
	authnNamespacedPrincipal = &models.Principal{Username: "u", Namespace: "customer1"}
	authnGlobalPrincipal     = &models.Principal{Username: "admin", IsGlobalOperator: true}
	authnEmptyNSPrincipal    = &models.Principal{Username: "u"}
)

// fullPermission returns a permission populated with one own-NS value per
// strip-set field plus a few untouched fields, so a single fixture can
// exercise the whole strip surface in one row.
func fullPermission() *models.Permission {
	return &models.Permission{
		Action:      strPtr("read_collections"),
		Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")},
		Data:        &models.PermissionData{Collection: strPtr("customer1:Movies"), Tenant: strPtr("t1"), Object: strPtr("o1")},
		Nodes:       &models.PermissionNodes{Collection: strPtr("customer1:Movies"), Verbosity: strPtr("minimal")},
		Tenants:     &models.PermissionTenants{Collection: strPtr("customer1:Movies"), Tenant: strPtr("t1")},
		Backups:     &models.PermissionBackups{Collection: strPtr("customer1:Movies")},
		Replicate:   &models.PermissionReplicate{Collection: strPtr("customer1:Movies"), Shard: strPtr("s1")},
		Aliases:     &models.PermissionAliases{Collection: strPtr("customer1:Movies"), Alias: strPtr("customer1:Films")},
		Users:       &models.PermissionUsers{Users: strPtr("customer1:apiuser")},
		Roles:       &models.PermissionRoles{Role: strPtr("customer1:editor")},
		Groups:      &models.PermissionGroups{Group: strPtr("customer1:engineers")},
		Namespaces:  &models.PermissionNamespaces{Namespace: strPtr("customer1")},
	}
}

func fullPermissionStripped() *models.Permission {
	return &models.Permission{
		Action:      strPtr("read_collections"),
		Collections: &models.PermissionCollections{Collection: strPtr("Movies")},
		Data:        &models.PermissionData{Collection: strPtr("Movies"), Tenant: strPtr("t1"), Object: strPtr("o1")},
		Nodes:       &models.PermissionNodes{Collection: strPtr("Movies"), Verbosity: strPtr("minimal")},
		Tenants:     &models.PermissionTenants{Collection: strPtr("Movies"), Tenant: strPtr("t1")},
		Backups:     &models.PermissionBackups{Collection: strPtr("Movies")},
		Replicate:   &models.PermissionReplicate{Collection: strPtr("Movies"), Shard: strPtr("s1")},
		Aliases:     &models.PermissionAliases{Collection: strPtr("Movies"), Alias: strPtr("Films")},
		// Untouched sub-structs are preserved by value.
		Users:      &models.PermissionUsers{Users: strPtr("customer1:apiuser")},
		Roles:      &models.PermissionRoles{Role: strPtr("customer1:editor")},
		Groups:     &models.PermissionGroups{Group: strPtr("customer1:engineers")},
		Namespaces: &models.PermissionNamespaces{Namespace: strPtr("customer1")},
	}
}

func TestStripPermissionForCaller(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		in        *models.Permission
		want      *models.Permission
	}{
		{
			name:      "namespaced principal: every strip-set field stripped, untouched fields preserved",
			principal: authnNamespacedPrincipal,
			in:        fullPermission(),
			want:      fullPermissionStripped(),
		},
		{
			name:      "foreign prefix preserved on Collections",
			principal: authnNamespacedPrincipal,
			in:        &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer2:Movies")}},
			want:      &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer2:Movies")}},
		},
		{
			name:      "wildcard `*` preserved on Aliases",
			principal: authnNamespacedPrincipal,
			in:        &models.Permission{Aliases: &models.PermissionAliases{Collection: strPtr("*"), Alias: strPtr("*")}},
			want:      &models.Permission{Aliases: &models.PermissionAliases{Collection: strPtr("*"), Alias: strPtr("*")}},
		},
		{
			name:      "aliases sub-struct with only Alias set",
			principal: authnNamespacedPrincipal,
			in:        &models.Permission{Aliases: &models.PermissionAliases{Alias: strPtr("customer1:Films")}},
			want:      &models.Permission{Aliases: &models.PermissionAliases{Alias: strPtr("Films")}},
		},
		{
			name:      "action only — all resource sub-structs nil",
			principal: authnNamespacedPrincipal,
			in:        &models.Permission{Action: strPtr("read_collections")},
			want:      &models.Permission{Action: strPtr("read_collections")},
		},
		{
			name:      "sub-struct present but Collection nil — passes through untouched",
			principal: authnNamespacedPrincipal,
			in:        &models.Permission{Collections: &models.PermissionCollections{}},
			want:      &models.Permission{Collections: &models.PermissionCollections{}},
		},
		{
			name:      "global principal: stripping is a no-op even when called directly",
			principal: authnGlobalPrincipal,
			in:        &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
			want:      &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
		},
		{
			name:      "nil principal: stripping is a no-op even when called directly",
			principal: nil,
			in:        &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
			want:      &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
		},
		{
			name:      "principal with empty namespace: stripping is a no-op",
			principal: authnEmptyNSPrincipal,
			in:        &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
			want:      &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
		},
		{
			name:      "nil permission returns nil",
			principal: authnNamespacedPrincipal,
			in:        nil,
			want:      nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Deep-copy snapshot so a mutation through tc.in is observable
			// in the post-call comparison (a pointer copy would be aliased
			// by any in-place edit on sub-structs or *string fields).
			snapshot := deepCopyPermission(t, tc.in)
			got := stripPermissionForCaller(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, snapshot, tc.in, "input must not be mutated")
		})
	}

	// Pointer-identity invariants — verified once each, not table-driven, so
	// the rest of the table can stay value-only.
	t.Run("stripped sub-structs are freshly allocated, not aliases of the input", func(t *testing.T) {
		in := fullPermission()
		got := stripPermissionForCaller(authnNamespacedPrincipal, in)
		require.NotSame(t, in, got)
		assert.NotSame(t, in.Collections, got.Collections)
		assert.NotSame(t, in.Data, got.Data)
		assert.NotSame(t, in.Nodes, got.Nodes)
		assert.NotSame(t, in.Tenants, got.Tenants)
		assert.NotSame(t, in.Backups, got.Backups)
		assert.NotSame(t, in.Replicate, got.Replicate)
		assert.NotSame(t, in.Aliases, got.Aliases)
	})

	t.Run("untouched sub-structs keep their input pointer identity", func(t *testing.T) {
		in := fullPermission()
		got := stripPermissionForCaller(authnNamespacedPrincipal, in)
		// Strip set excludes users/roles/groups/namespaces — reused as-is.
		assert.Same(t, in.Users, got.Users)
		assert.Same(t, in.Roles, got.Roles)
		assert.Same(t, in.Groups, got.Groups)
		assert.Same(t, in.Namespaces, got.Namespaces)
	})

	// Singleton safety: PoliciesToPermission's `*authorization.All` branch
	// points sub-structs at package-level singletons. Mutating them would
	// corrupt every other caller in the process. Verify each strip-set
	// singleton is replaced, not mutated.
	singletonCases := []struct {
		name        string
		mkInput     func() *models.Permission
		originalPtr any
		check       func(t *testing.T, got *models.Permission)
	}{
		{
			name: "AllCollections singleton not mutated",
			mkInput: func() *models.Permission {
				return &models.Permission{Collections: authorization.AllCollections}
			},
			originalPtr: authorization.AllCollections,
			check: func(t *testing.T, got *models.Permission) {
				assert.NotSame(t, authorization.AllCollections, got.Collections)
			},
		},
		{
			name: "AllAliases singleton not mutated",
			mkInput: func() *models.Permission {
				return &models.Permission{Aliases: authorization.AllAliases}
			},
			originalPtr: authorization.AllAliases,
			check: func(t *testing.T, got *models.Permission) {
				assert.NotSame(t, authorization.AllAliases, got.Aliases)
			},
		},
	}
	for _, tc := range singletonCases {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.originalPtr
			got := stripPermissionForCaller(authnNamespacedPrincipal, tc.mkInput())
			tc.check(t, got)
			// Underlying singleton pointer still points at the same address.
			switch tc.name {
			case "AllCollections singleton not mutated":
				assert.Same(t, before, authorization.AllCollections)
			case "AllAliases singleton not mutated":
				assert.Same(t, before, authorization.AllAliases)
			}
		})
	}
}

func TestStripRolesForCaller(t *testing.T) {
	makeRoles := func() []*models.Role {
		name1 := "viewer"
		name2 := "editor"
		return []*models.Role{
			{
				Name: &name1,
				Permissions: []*models.Permission{
					{
						Action:      strPtr("read_collections"),
						Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")},
					},
					{
						Action:  strPtr("read_aliases"),
						Aliases: &models.PermissionAliases{Collection: strPtr("customer1:Movies"), Alias: strPtr("customer1:Films")},
					},
				},
			},
			{Name: &name2}, // role with no permissions
		}
	}

	makeStrippedRoles := func() []*models.Role {
		name1 := "viewer"
		name2 := "editor"
		return []*models.Role{
			{
				Name: &name1,
				Permissions: []*models.Permission{
					{
						Action:      strPtr("read_collections"),
						Collections: &models.PermissionCollections{Collection: strPtr("Movies")},
					},
					{
						Action:  strPtr("read_aliases"),
						Aliases: &models.PermissionAliases{Collection: strPtr("Movies"), Alias: strPtr("Films")},
					},
				},
			},
			{Name: &name2},
		}
	}

	cases := []struct {
		name      string
		principal *models.Principal
		in        []*models.Role
		want      []*models.Role
		wantSame  bool // true when the helper is expected to return the input slice unchanged (pass-through)
	}{
		{
			name:      "namespaced principal strips each permission",
			principal: authnNamespacedPrincipal,
			in:        makeRoles(),
			want:      makeStrippedRoles(),
		},
		{
			name:      "global principal: input slice returned unchanged",
			principal: authnGlobalPrincipal,
			in:        makeRoles(),
			want:      makeRoles(),
			wantSame:  true,
		},
		{
			name:      "nil principal: input slice returned unchanged",
			principal: nil,
			in:        makeRoles(),
			want:      makeRoles(),
			wantSame:  true,
		},
		{
			name:      "empty namespace: input slice returned unchanged",
			principal: authnEmptyNSPrincipal,
			in:        makeRoles(),
			want:      makeRoles(),
			wantSame:  true,
		},
		{
			name:      "nil roles: nil out",
			principal: authnNamespacedPrincipal,
			in:        nil,
			want:      nil,
			wantSame:  true,
		},
		{
			name:      "empty roles: passes through",
			principal: authnNamespacedPrincipal,
			in:        []*models.Role{},
			want:      []*models.Role{},
			wantSame:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := deepCopyRoles(t, tc.in)
			got := stripRolesForCaller(tc.principal, tc.in)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, snapshot, tc.in, "input must not be mutated")
			if tc.wantSame && tc.in != nil {
				// Pass-through path: same backing slice header.
				require.Len(t, got, len(tc.in))
				if len(tc.in) > 0 {
					assert.Same(t, tc.in[0], got[0])
				}
			} else if !tc.wantSame && len(tc.in) > 0 {
				// Strip path: each role is a fresh allocation.
				assert.NotSame(t, tc.in[0], got[0])
			}
		})
	}
}
