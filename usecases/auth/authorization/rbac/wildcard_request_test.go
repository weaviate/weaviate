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

package rbac

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func newWildcardManager(t *testing.T, permission *models.Permission) (*Manager, *models.Principal) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	m, err := New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, logger)
	require.NoError(t, err)
	policies, err := conv.RolesToPolicies(&models.Role{Name: authorization.String("r"), Permissions: []*models.Permission{permission}})
	require.NoError(t, err)
	require.NoError(t, m.CreateRolesPermissions(policies))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("u", authentication.AuthTypeDb), []string{"r"}))
	return m, &models.Principal{Username: "u", UserType: models.UserTypeInputDb}
}

// TestWildcardRequestNeedsWildcardGrant: a request whose segment is the literal
// "*" asks for every name, so only a pattern matching every name may pass it,
// not one that merely matches the character '*'.
func TestWildcardRequestNeedsWildcardGrant(t *testing.T) {
	str := authorization.String
	tests := []struct {
		name       string
		permission func(pattern string) *models.Permission
		verb       string
		request    []string
	}{
		{
			name: "every role",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadRoles), Roles: &models.PermissionRoles{Role: &p, Scope: str(models.PermissionRolesScopeAll)}}
			},
			verb:    authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
			request: authorization.Roles(),
		},
		{
			name: "every user",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadUsers), Users: &models.PermissionUsers{Users: &p}}
			},
			verb:    authorization.READ,
			request: authorization.Users(),
		},
		{
			name: "every group",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadGroups), Groups: &models.PermissionGroups{Group: &p, GroupType: models.GroupTypeOidc}}
			},
			verb:    authorization.READ,
			request: authorization.Groups(authentication.AuthTypeOIDC),
		},
		{
			name: "every collection",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadCollections), Collections: &models.PermissionCollections{Collection: &p}}
			},
			verb:    authorization.READ,
			request: authorization.CollectionsMetadata(),
		},
		{
			name: "every collection's data",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadData), Data: &models.PermissionData{Collection: &p, Tenant: str("*")}}
			},
			verb:    authorization.READ,
			request: authorization.CollectionsData(),
		},
		{
			name: "every tenant's data",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadData), Data: &models.PermissionData{Collection: str("Movies"), Tenant: &p}}
			},
			verb:    authorization.READ,
			request: authorization.CollectionsData("Movies"),
		},
		{
			name: "every tenant",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadTenants), Tenants: &models.PermissionTenants{Collection: str("Movies"), Tenant: &p}}
			},
			verb:    authorization.READ,
			request: authorization.ShardsMetadata("Movies"),
		},
		{
			name: "every alias",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadAliases), Aliases: &models.PermissionAliases{Collection: str("Movies"), Alias: &p}}
			},
			verb:    authorization.READ,
			request: authorization.Aliases("Movies"),
		},
		{
			name: "every backup",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ManageBackups), Backups: &models.PermissionBackups{Collection: &p}}
			},
			verb:    authorization.READ,
			request: authorization.Backups(),
		},
		{
			name: "every namespace",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ManageNamespaces), Namespaces: &models.PermissionNamespaces{Namespace: &p}}
			},
			verb:    authorization.READ,
			request: authorization.Namespaces(),
		},
		{
			name: "every collection's nodes",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadNodes), Nodes: &models.PermissionNodes{Collection: &p, Verbosity: str("verbose")}}
			},
			verb:    authorization.READ,
			request: authorization.Nodes("verbose"),
		},
		{
			name: "every shard's replication",
			permission: func(p string) *models.Permission {
				return &models.Permission{Action: str(authorization.ReadReplicate), Replicate: &models.PermissionReplicate{Collection: str("Movies"), Shard: &p}}
			},
			verb:    authorization.READ,
			request: []string{authorization.Replications("Movies", "")},
		},
	}
	patterns := []struct {
		pattern string
		allowed bool
	}{
		{"*", true},
		{".*", true},
		{"A|*", true},
		{"A|.", false},
		{".", false},
		{".?", false},
		{"[*]", false},
		// KeyMatch5 rewrites "{...}" to "[^/]+": these keep '*' but lose every
		// one-letter name, or every name not starting with "a".
		{`\x2a|(.{1})+`, false},
		{`\x2a|a{1}|*|b{1}`, false},
	}
	for _, tt := range tests {
		for _, p := range patterns {
			t.Run(tt.name+"/"+p.pattern, func(t *testing.T) {
				m, u := newWildcardManager(t, tt.permission(p.pattern))
				err := m.AuthorizeSilent(context.Background(), u, tt.verb, tt.request...)
				if p.allowed {
					require.NoError(t, err)
					return
				}
				var forbidden errors.Forbidden
				require.ErrorAs(t, err, &forbidden, "a grant on %q must not reach %v", p.pattern, tt.request)
			})
		}
	}
}

// TestWildcardRequestQuotedTargets: a "\Q" in one target quotes the group
// conv wraps around it and spills into the next target, so the stored text no
// longer splits where the enforced regex does. This grant names no tenant.
func TestWildcardRequestQuotedTargets(t *testing.T) {
	m, u := newWildcardManager(t, &models.Permission{
		Action:  authorization.String(authorization.ReadTenants),
		Tenants: &models.PermissionTenants{Collection: authorization.String(`X.shards.[*]|\Q`), Tenant: authorization.String(`A|\Q\E*`)},
	})
	var forbidden errors.Forbidden
	require.ErrorAs(t, m.AuthorizeSilent(context.Background(), u, authorization.READ, authorization.ShardsMetadata("X")...), &forbidden)
	require.ErrorAs(t, m.AuthorizeSilent(context.Background(), u, authorization.READ, authorization.ShardsMetadata("X", "tenant1")...), &forbidden)
}

// TestWildcardRequestFilter: a list filter first tries the whole parent with a
// "*" request, which must not hand out every item to a narrow grant.
func TestWildcardRequestFilter(t *testing.T) {
	m, u := newWildcardManager(t, &models.Permission{
		Action:  authorization.String(authorization.ReadAliases),
		Aliases: &models.PermissionAliases{Collection: authorization.String("Movies"), Alias: authorization.String("A|.")},
	})
	got := filter.New[string](m, rbacconf.Config{Enabled: true}).Filter(context.Background(), u,
		[]string{"A", "B", "Other"}, authorization.READ,
		func(alias string) string { return authorization.Aliases("Movies", alias)[0] })
	require.Equal(t, []string{"A", "B"}, got)
}

func TestWildcardsCoveredMatcher(t *testing.T) {
	quoted := `schema/collections/(X.shards.[.*]|\Q)/shards/(A|\Q\E.*)`
	tests := []struct {
		name              string
		namespacesEnabled bool
		ns                string
		request           string
		policy            string
		want              bool
	}{
		{name: "root", request: "roles/*", policy: "*", want: true},
		{name: "every role", request: "roles/*", policy: "roles/.*", want: true},
		{name: "one-letter roles", request: "roles/*", policy: "roles/(A|.)", want: false},
		{name: "a named role is no wildcard", request: "roles/A", policy: "roles/(A|.)", want: true},
		{name: "data of one tenant", request: "data/collections/Movies/shards/t1/objects/*", policy: "data/collections/Movies/shards/t1/objects/.*", want: true},
		{name: "data with a narrow objects segment", request: "data/collections/Movies/shards/t1/objects/*", policy: "data/collections/Movies/shards/t1/objects/.", want: false},
		{name: "a segment that does not line up", request: "users/x/*", policy: "users/.*", want: false},
		{
			name: "global caller, every tenant of a qualified collection", namespacesEnabled: true,
			request: "schema/collections/c1:Movies/shards/*", policy: "schema/collections/Movies/shards/.*", want: true,
		},
		{
			name: "global caller, one-letter tenants of a qualified collection", namespacesEnabled: true,
			request: "schema/collections/c1:Movies/shards/*", policy: "schema/collections/Movies/shards/(A|.)", want: false,
		},
		{
			name: "confined caller, every tenant", namespacesEnabled: true, ns: "c1",
			request: "schema/collections/c1:Movies/shards/*", policy: "schema/collections/Movies/shards/.*", want: true,
		},
		{
			name: "confined caller, one-letter tenants", namespacesEnabled: true, ns: "c1",
			request: "schema/collections/c1:Movies/shards/*", policy: "schema/collections/Movies/shards/(A|.)", want: false,
		},
		{name: "a quoted group spilling into the next segment", request: "schema/collections/X/shards/*", policy: quoted, want: false},
		{
			name: "global caller, a quoted group spilling into the next segment", namespacesEnabled: true,
			request: "schema/collections/c1:X/shards/*", policy: quoted, want: false,
		},
		{
			name: "confined caller, a quoted group spilling into the next segment", namespacesEnabled: true, ns: "c1",
			request: "schema/collections/c1:X/shards/*", policy: quoted, want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeNamespaceAwareMatcherFunc(tt.namespacesEnabled)(tt.request, tt.policy, tt.ns)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestUniversalSegment(t *testing.T) {
	for seg, want := range map[string]bool{
		"*":                   true,
		".*":                  true,
		"..*":                 true,
		".+":                  true,
		".*?":                 true,
		"(.*)":                true,
		"(.)*":                true,
		"(.+)?":               true,
		".*.*":                true,
		"(?s).*":              true,
		"(A|.*)":              true,
		"(.*|A)":              true,
		".":                   false,
		"..":                  false,
		".?":                  false,
		"(A|.)":               false,
		"[*]":                 false,
		`\*`:                  false,
		"A.*":                 false,
		".*A":                 false,
		"[^/]+":               false,
		"#":                   false,
		"(":                   false,
		`(\x2a|(.{1})+)`:      false,
		`(\x2a|a{1}|.*|b{1})`: false,
		".{0,}":               false,
	} {
		require.Equal(t, want, universalSegment(seg), seg)
	}
}

// TestUniversalSegmentNestedOptionals pins that the check stays linear in the
// nesting depth: each "(...)?B" level used to double the work.
func TestUniversalSegmentNestedOptionals(t *testing.T) {
	seg := "AB"
	for range 40 {
		seg = "(" + seg + ")?B"
	}
	require.False(t, universalSegment(seg+"|."))
}
