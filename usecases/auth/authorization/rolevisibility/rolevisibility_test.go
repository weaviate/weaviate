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

package rolevisibility

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func collPolicy(verb, collection string) authorization.Policy {
	return authorization.Policy{Resource: authorization.Collections(collection)[0], Verb: verb, Domain: authorization.SchemaDomain}
}

func TestCallerConfined(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		want              bool
	}{
		{name: "namespaces disabled", namespacesEnabled: false, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, want: false},
		{name: "nil principal", namespacesEnabled: true, principal: nil, want: false},
		{name: "global operator", namespacesEnabled: true, principal: &models.Principal{Username: "op", Namespace: "customer1", IsGlobalOperator: true}, want: false},
		{name: "namespace-less principal", namespacesEnabled: true, principal: &models.Principal{Username: "root"}, want: false},
		{name: "confined non-operator", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, CallerConfined(tt.namespacesEnabled, tt.principal))
		})
	}
}

func TestRoleHiddenFromCaller(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		storedRoleName    string
		want              bool
	}{
		{name: "unconfined caller hides nothing", namespacesEnabled: false, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer2:editor", want: false},
		{name: "global operator hides nothing", namespacesEnabled: true, principal: &models.Principal{Username: "op", IsGlobalOperator: true}, storedRoleName: "customer2:editor", want: false},
		{name: "confined caller: foreign namespace hidden", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer2:editor", want: true},
		{name: "confined caller: own namespace visible", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer1:editor", want: false},
		{name: "confined caller: global role visible", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "editor", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, RoleHiddenFromCaller(tt.namespacesEnabled, tt.principal, tt.storedRoleName))
		})
	}
}

func TestRoleOperatorReservedFromCaller(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		storedRoleName    string
		want              bool
	}{
		{name: "nil principal is not treated as reserved", namespacesEnabled: true, principal: nil, storedRoleName: "global_admin", want: false},
		{name: "namespaces disabled hides nothing", namespacesEnabled: false, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "global_admin", want: false},
		{name: "global operator sees reserved roles", namespacesEnabled: true, principal: &models.Principal{Username: "op", IsGlobalOperator: true}, storedRoleName: "global_admin", want: false},
		{name: "confined non-operator: reserved global role hidden", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "operator_secret", want: true},
		{name: "confined non-operator: ordinary role visible", namespacesEnabled: true, principal: &models.Principal{Username: "alice", Namespace: "customer1"}, storedRoleName: "customer1:editor", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, RoleOperatorReservedFromCaller(tt.namespacesEnabled, tt.principal, tt.storedRoleName))
		})
	}
}

func TestRolePoliciesVisibleToPrincipal(t *testing.T) {
	// allScopeGranted returns nil for the READ ALL-scope roles check.
	allScopeGranted := func(t *testing.T) *authorization.MockAuthorizer {
		a := authorization.NewMockAuthorizer(t)
		a.On("Authorize", mock.Anything, mock.Anything,
			authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
			authorization.Roles()[0]).Return(nil).Maybe()
		return a
	}
	// allScopeDenied denies the ALL-scope check and defers to a per-verb and
	// per-resource silent verdict.
	allScopeDenied := func(t *testing.T, silent func(verb, resource string) error) *authorization.MockAuthorizer {
		a := authorization.NewMockAuthorizer(t)
		a.On("Authorize", mock.Anything, mock.Anything,
			authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
			authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
		a.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(func(_ context.Context, _ *models.Principal, verb string, resources ...string) error {
				return silent(verb, resources[0])
			}).Maybe()
		return a
	}

	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		policies          []authorization.Policy
		authorizer        func(*testing.T) *authorization.MockAuthorizer
		want              bool
	}{
		{
			name:       "all-scope on roles sees every role",
			principal:  &models.Principal{Username: "g"},
			policies:   []authorization.Policy{collPolicy(authorization.CREATE, "Secret")},
			authorizer: allScopeGranted,
			want:       true,
		},
		{
			name:      "global caller holding every permission",
			principal: &models.Principal{Username: "g"},
			policies:  []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				return allScopeDenied(t, func(_, _ string) error { return nil })
			},
			want: true,
		},
		{
			name:      "global caller missing a permission",
			principal: &models.Principal{Username: "g"},
			policies:  []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				return allScopeDenied(t, func(_, _ string) error { return fmt.Errorf("forbidden") })
			},
			want: false,
		},
		{
			name:      "global caller missing one verb of a manage permission",
			principal: &models.Principal{Username: "g"},
			policies:  []authorization.Policy{{Resource: conv.CasbinBackups("*"), Verb: conv.CRUD, Domain: authorization.BackupsDomain}},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				return allScopeDenied(t, func(verb, _ string) error {
					if verb == authorization.CREATE {
						return fmt.Errorf("forbidden")
					}
					return nil
				})
			},
			want: false,
		},
		{
			name:      "permission-less placeholder policy grants visibility",
			principal: &models.Principal{Username: "g"},
			policies:  []authorization.Policy{{Resource: conv.InternalPlaceHolder}},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				// AuthorizeSilent must never be called for a placeholder.
				return allScopeDenied(t, func(_, _ string) error {
					t.Fatal("AuthorizeSilent called for placeholder policy")
					return nil
				})
			},
			want: true,
		},
		{
			name:              "confined caller: bare permission projected into own namespace and held",
			namespacesEnabled: true,
			principal:         &models.Principal{Username: "alice", Namespace: "customer1"},
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				return allScopeDenied(t, func(_, resource string) error {
					require.Equal(t, authorization.Collections("customer1:Movies")[0], resource)
					return nil
				})
			},
			want: true,
		},
		{
			name:              "confined caller: permission bound to a foreign namespace",
			namespacesEnabled: true,
			principal:         &models.Principal{Username: "alice", Namespace: "customer1"},
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "customer2:Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				// Projection fails before any silent check runs.
				return allScopeDenied(t, func(_, _ string) error {
					t.Fatal("AuthorizeSilent called despite projection error")
					return nil
				})
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RolePoliciesVisibleToPrincipal(context.Background(), tt.authorizer(t), tt.namespacesEnabled, tt.principal, tt.policies)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRoleVisibleToCaller(t *testing.T) {
	confined := &models.Principal{Username: "alice", Namespace: "customer1"}

	tests := []struct {
		name              string
		namespacesEnabled bool
		principal         *models.Principal
		storedRoleName    string
		policies          []authorization.Policy
		authorizer        func(*testing.T) *authorization.MockAuthorizer
		want              bool
	}{
		{
			name:              "namespaces disabled: always visible, no authorizer call",
			namespacesEnabled: false,
			principal:         confined,
			storedRoleName:    "customer2:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Secret")},
			authorizer:        func(t *testing.T) *authorization.MockAuthorizer { return authorization.NewMockAuthorizer(t) },
			want:              true,
		},
		{
			name:              "foreign-namespace role hidden by name",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "customer2:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "customer2:Secret")},
			// Hidden by name before any authorizer call.
			authorizer: func(t *testing.T) *authorization.MockAuthorizer { return authorization.NewMockAuthorizer(t) },
			want:       false,
		},
		{
			name:              "operator-reserved global role hidden from confined caller",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "operator_secret",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			// Hidden as reserved before the content gate.
			authorizer: func(t *testing.T) *authorization.MockAuthorizer { return authorization.NewMockAuthorizer(t) },
			want:       false,
		},
		{
			name:              "own-namespace role hidden when caller lacks the permission",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "customer1:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				a := authorization.NewMockAuthorizer(t)
				a.On("Authorize", mock.Anything, mock.Anything,
					authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
					authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
				a.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(fmt.Errorf("forbidden")).Maybe()
				return a
			},
			want: false,
		},
		{
			name:              "own-namespace role visible when caller holds the permission",
			namespacesEnabled: true,
			principal:         confined,
			storedRoleName:    "customer1:editor",
			policies:          []authorization.Policy{collPolicy(authorization.CREATE, "Movies")},
			authorizer: func(t *testing.T) *authorization.MockAuthorizer {
				a := authorization.NewMockAuthorizer(t)
				a.On("Authorize", mock.Anything, mock.Anything,
					authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
					authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
				a.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil).Maybe()
				return a
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RoleVisibleToCaller(context.Background(), tt.authorizer(t), tt.namespacesEnabled, tt.principal, tt.storedRoleName, tt.policies)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGrantedVerbs(t *testing.T) {
	tests := []struct {
		verb string
		want []string
	}{
		{verb: authorization.READ, want: []string{"R"}},
		{verb: authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), want: []string{"U_ALL"}},
		{verb: conv.CRUD, want: []string{"C", "R", "U", "D"}},
		{verb: conv.CRUD + "_" + authorization.ROLE_SCOPE_MATCH, want: []string{"C_MATCH", "R_MATCH", "U_MATCH", "D_MATCH"}},
	}
	for _, tt := range tests {
		t.Run(tt.verb, func(t *testing.T) {
			require.Equal(t, tt.want, grantedVerbs(tt.verb))
		})
	}
}

// TestRegexSegmentsToWildcard covers the regex syntax conv stores, each letting
// a pattern match a name its text does not contain. The "<ns>:" qualifier is
// dropped when it carries regex syntax or when a '?' makes its colon optional.
func TestRegexSegmentsToWildcard(t *testing.T) {
	tests := []struct {
		resource string
		want     string
	}{
		{resource: "users/admin[eu]", want: "users/.*"},
		{resource: "users/eu?admin", want: "users/.*"},
		{resource: "users/e+u", want: "users/.*"},
		{resource: "users/eu{0}admin", want: "users/.*"},
		{resource: `users/\x61dmin`, want: "users/.*"},
		{resource: "users/(admin|customer2:eu)", want: "users/.*"},
		{resource: "users/customer1:b?", want: "users/customer1:.*"},
		{resource: "users/customer1:?b", want: "users/.*"},
		{resource: "users/(z|.*)", want: "users/.*"},
		{resource: "data/collections/(?i)Movies/shards/t1/objects/.*", want: "data/collections/.*/.*/.*/.*/.*"},
		{resource: "aliases/collections/customer1:(?i)A/aliases/customer1:b", want: "aliases/collections/customer1:.*/.*/.*"},
		{resource: "users/customer1:+b", want: "users/customer1:.*"},
		{resource: "users/customer1:{2}b", want: "users/customer1:.*"},
	}
	for _, tt := range tests {
		t.Run(tt.resource, func(t *testing.T) {
			require.Equal(t, tt.want, regexSegmentsToWildcard(tt.resource))
		})
	}
}

// TestRolePoliciesVisibleToPrincipalCasbin hides a role whose wildcard reaches
// past the caller's pattern, though that pattern matches the wildcard's text.
func TestRolePoliciesVisibleToPrincipalCasbin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true}, config.Authentication{}, true, nil, logger)
	require.NoError(t, err)
	updateUsers := func(name, users string) *models.Role {
		return &models.Role{Name: authorization.String(name), Permissions: []*models.Permission{{
			Action: authorization.String(authorization.UpdateUsers), Users: &models.PermissionUsers{Users: &users},
		}}}
	}
	policies, err := conv.RolesToPolicies(updateUsers("a-and-two-users", "a.."), updateUsers("a-users", "a*"), updateUsers("abc-users", "abc"),
		updateUsers("not-a-users", "[^a]*"), updateUsers("all-users", "*"), updateUsers("bob-users", "bob"))
	require.NoError(t, err)
	require.NoError(t, m.CreateRolesPermissions(policies))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("ops", authentication.AuthTypeDb), []string{"a-and-two-users"}))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("viewer", authentication.AuthTypeDb), []string{"not-a-users"}))
	roles, err := m.GetRoles("a-users", "abc-users", "all-users", "bob-users")
	require.NoError(t, err)

	ops := &models.Principal{Username: "ops", UserType: models.UserTypeInputDb}
	require.False(t, RolePoliciesVisibleToPrincipal(context.Background(), m, true, ops, roles["a-users"]))
	require.True(t, RolePoliciesVisibleToPrincipal(context.Background(), m, true, ops, roles["abc-users"]))
	// A pattern excluding names covers none of the patterns it matches as text.
	viewer := &models.Principal{Username: "viewer", UserType: models.UserTypeInputDb}
	require.False(t, RolePoliciesVisibleToPrincipal(context.Background(), m, true, viewer, roles["all-users"]))
	require.True(t, RolePoliciesVisibleToPrincipal(context.Background(), m, true, viewer, roles["bob-users"]))
}

// TestAuthorizeRolePermissionsCasbin runs the check against the real enforcer,
// whose unanchored verb match a mocked authorizer cannot show.
func TestAuthorizeRolePermissionsCasbin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	role := func(name, action string, permission models.Permission) *models.Role {
		permission.Action = authorization.String(action)
		return &models.Role{Name: authorization.String(name), Permissions: []*models.Permission{&permission}}
	}
	users := func(user string) models.Permission {
		return models.Permission{Users: &models.PermissionUsers{Users: &user}}
	}
	collections := func(collection string) models.Permission {
		return models.Permission{Collections: &models.PermissionCollections{Collection: &collection}}
	}
	groups := func(group string) models.Permission {
		return models.Permission{Groups: &models.PermissionGroups{Group: &group, GroupType: models.GroupTypeOidc}}
	}
	all := "*"
	updateUsers := func(name, users string) *models.Role {
		return &models.Role{Name: authorization.String(name), Permissions: []*models.Permission{{
			Action: authorization.String(authorization.UpdateUsers), Users: &models.PermissionUsers{Users: &users},
		}}}
	}
	readData := func(name, collection, tenant string) *models.Role {
		return &models.Role{Name: authorization.String(name), Permissions: []*models.Permission{{
			Action: authorization.String(authorization.ReadData), Data: &models.PermissionData{Collection: &collection, Tenant: &tenant},
		}}}
	}
	readMoviesData := func(name, tenant string) *models.Role {
		return &models.Role{Name: authorization.String(name), Permissions: []*models.Permission{{
			Action: authorization.String(authorization.ReadData), Data: &models.PermissionData{Collection: authorization.String("Movies"), Tenant: &tenant},
		}}}
	}
	custom, err := conv.RolesToPolicies(
		&models.Role{Name: authorization.String("backup-manager"), Permissions: []*models.Permission{{
			Action: authorization.String(authorization.ManageBackups), Backups: &models.PermissionBackups{Collection: &all},
		}}},
		updateUsers("user-updater", all),
		&models.Role{Name: authorization.String("movies-creator"), Permissions: []*models.Permission{{
			Action: authorization.String(authorization.CreateCollections), Collections: &models.PermissionCollections{Collection: authorization.String("Movies")},
		}}},
		updateUsers("eu-users", "*eu*"),
		updateUsers("eu-or-admin-users", "eu|admin"),
		updateUsers("customer1-eu-or-admin-users", "customer1:eu|admin"),
		updateUsers("eu-west-users", "eu.west*"),
		readMoviesData("movies-reader", all),
		readMoviesData("eu-movies-reader", "*eu*"),
		readMoviesData("eu-or-secret-movies-reader", "eu|secret"),
		updateUsers("two-letter-users", ".."),
		updateUsers("a-users", "a*"),
		updateUsers("ab-users", "ab*"),
		updateUsers("a-optional-users", "a?*"),
		updateUsers("a-and-two-users", "a.."),
		updateUsers("a-and-more-users", "a.+"),
		updateUsers("non-empty-users", ".*"),
		updateUsers("customer1-a-and-two-users", "customer1:a.."),
		updateUsers("customer1-a-users", "customer1:a*"),
		updateUsers("a-and-two-colon-bob", "a..:bob"),
		updateUsers("a-colon-bob", "a*:bob"),
		updateUsers("a-colon-non-empty", "a:.*"),
		updateUsers("a-colon-any", "a:*"),
		updateUsers("customer1-users", "customer1:*"),
		updateUsers("a-optional-colon-b", "a:?b"),
		updateUsers("customer1-optional-colon-b", "customer1:?b"),
		updateUsers("john-dot-doe", "john.doe"),
		updateUsers("john-escaped-dot-doe", `john\.doe`),
		updateUsers("dot-doe-users", "*.doe"),
		role("bob-reader", authorization.ReadUsers, users("bob")),
		updateUsers("dot-com-users", "*.com"),
		updateUsers("com-users", "*com"),
		updateUsers("john-at-x-com", "john.doe@x.com"),
		updateUsers("team-dot-users", "team.*"),
		updateUsers("team-users", "team*"),
		updateUsers("eu-dash-and-two-users", "eu-.."),
		updateUsers("eu-dash-users", "eu-*"),
		updateUsers("not-a-users", "[^a]*"),
		updateUsers("bob-updater", "bob"),
		updateUsers("a-only", "a"),
		updateUsers("a-and-two-or-a-users", "a..|a"),
		updateUsers("a-up-to-two-more-users", "a.?.?"),
		updateUsers("one-or-more-users", ".+"),
		readData("t1-reader", "*", "t1"),
		readData("case-insensitive-movies-t1-reader", "M(?i)ovies", "t1"),
		updateUsers("one-or-more-colon-any-users", ".+:*"),
		updateUsers("any-colon-any-users", "*:*"),
		updateUsers("x-or-y-domain-users", "*@x.com|*@y.com"),
		updateUsers("bob-dot-smith", "bob.smith"),
		updateUsers("three-character-users", "..."),
		updateUsers("e-acute-and-one-users", "é."),
		role("a-colon-non-empty-groups", authorization.ReadGroups, groups("a:.*")),
		role("a-colon-any-groups", authorization.ReadGroups, groups("a:*")),
		role("foreign-movies-reader", authorization.ReadCollections, collections("customer2:Movies")),
	)
	require.NoError(t, err)

	managers := map[bool]*rbac.Manager{}
	for _, namespacesEnabled := range []bool{false, true} {
		m, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
			config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"root"}}}, namespacesEnabled, nil, logger)
		require.NoError(t, err)
		require.NoError(t, m.CreateRolesPermissions(custom))
		managers[namespacesEnabled] = m
	}

	// A denied manage permission is reported once, under the action an admin can grant.
	deniedManageBackups := "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to manage_backups [[Domain: backups,Collection: *]]"
	// A pattern with regex syntax beyond '.' and '*' is checked as ".*".
	deniedAllUsers := "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *]]"
	deniedAllMoviesTenants := "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to read_data [[Domain: data, Collection: Movies, Tenant: *]]"

	tests := []struct {
		name              string
		caller            string
		callerNamespace   string
		callerRoles       []string
		targetRoles       []string
		namespacesEnabled bool
		targetNamespace   string
		wantDenied        string
	}{
		{name: "one verb of a manage permission is not enough", caller: "caller", callerRoles: []string{authorization.Viewer, "user-updater"}, targetRoles: []string{"backup-manager"}, wantDenied: deniedManageBackups},
		{name: "a manage permission covers itself", caller: "caller", callerRoles: []string{"backup-manager"}, targetRoles: []string{"backup-manager"}},
		{name: "root covers the built-in admin role", caller: "root", targetRoles: []string{authorization.Admin}},
		{name: "one role beyond the caller refuses them all", caller: "caller", callerRoles: []string{"user-updater"}, targetRoles: []string{"backup-manager", "user-updater"}, wantDenied: deniedManageBackups},
		{name: "a global caller's bare collection grant covers it in the target's namespace", caller: "caller", callerRoles: []string{"movies-creator"}, targetRoles: []string{"movies-creator"}, namespacesEnabled: true, targetNamespace: "customer1"},
		{name: "an alternation of users reaching past the caller's is refused", caller: "caller", callerRoles: []string{"eu-users"}, targetRoles: []string{"eu-or-admin-users"}, wantDenied: deniedAllUsers},
		{name: "an alternation of tenants reaching past the caller's is refused", caller: "caller", callerRoles: []string{"eu-movies-reader"}, targetRoles: []string{"eu-or-secret-movies-reader"}, wantDenied: deniedAllMoviesTenants},
		{name: "every tenant of a collection covers an alternation of its tenants", caller: "caller", callerRoles: []string{"movies-reader"}, targetRoles: []string{"eu-or-secret-movies-reader"}},
		{name: "a confined caller with every user covers an alternation in its namespace", caller: "caller", callerNamespace: "customer1", callerRoles: []string{"user-updater"}, targetRoles: []string{"customer1-eu-or-admin-users"}, namespacesEnabled: true},
		{name: "a dotted name with a trailing wildcard inside the caller's pattern is covered", caller: "caller", callerRoles: []string{"eu-users"}, targetRoles: []string{"eu-west-users"}},
		{name: "a caller pattern matching the text of a wildcard is not a wildcard", caller: "caller", callerRoles: []string{"two-letter-users"}, targetRoles: []string{"eu-or-admin-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *]]"},
		{name: "a caller pattern of fixed length does not cover a wildcard", caller: "caller", callerRoles: []string{"two-letter-users"}, targetRoles: []string{"user-updater"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *]]"},
		{name: "a caller pattern needing more after a prefix does not cover the prefix's wildcard", caller: "caller", callerRoles: []string{"a-and-two-users"}, targetRoles: []string{"a-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a*]]"},
		{name: "a caller pattern needing one more after a prefix does not cover the prefix's wildcard", caller: "caller", callerRoles: []string{"a-and-more-users"}, targetRoles: []string{"a-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a*]]"},
		{name: "an optional prefix character reaching past the caller's pattern is refused", caller: "caller", callerRoles: []string{"a-users"}, targetRoles: []string{"a-optional-users"}, wantDenied: deniedAllUsers},
		{name: "a caller wildcard covers itself and a longer prefix", caller: "caller", callerRoles: []string{"a-users"}, targetRoles: []string{"a-users", "ab-users"}},
		{name: "a caller pattern needing more before a colon does not cover the wildcard there", caller: "caller", callerRoles: []string{"a-and-two-colon-bob"}, targetRoles: []string{"a-colon-bob"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a*:bob]]"},
		{name: "a caller pattern needing more after a colon does not cover the wildcard there", caller: "caller", callerRoles: []string{"a-colon-non-empty"}, targetRoles: []string{"a-colon-any"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a:*]]"},
		{name: "a group pattern needing more after a colon does not cover the wildcard there, even with namespaces", caller: "caller", callerRoles: []string{"a-colon-non-empty-groups"}, targetRoles: []string{"a-colon-any-groups"}, namespacesEnabled: true, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to read_groups []"},
		{name: "a group wildcard after a colon covers itself with namespaces", caller: "caller", callerRoles: []string{"a-colon-any-groups"}, targetRoles: []string{"a-colon-any-groups"}, namespacesEnabled: true},
		{name: "a kept prefix does not cover a grant that makes its colon optional", caller: "caller", callerRoles: []string{"a-colon-any"}, targetRoles: []string{"a-optional-colon-b"}, wantDenied: deniedAllUsers},
		{name: "a namespace does not cover a grant that makes its colon optional", caller: "caller", callerRoles: []string{"customer1-users"}, targetRoles: []string{"customer1-optional-colon-b"}, namespacesEnabled: true, wantDenied: deniedAllUsers},
		{name: "a literal dot does not cover a grant's any-character dot", caller: "caller", callerRoles: []string{"john-escaped-dot-doe"}, targetRoles: []string{"john-dot-doe"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: john.doe]]"},
		{name: "a grant that makes a colon optional needs every user", caller: "caller", callerRoles: []string{"a-users"}, targetRoles: []string{"a-optional-colon-b"}, wantDenied: deniedAllUsers},
		{name: "an inline flag reaches past its own segment", caller: "caller", callerRoles: []string{"t1-reader"}, targetRoles: []string{"case-insensitive-movies-t1-reader"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to read_data [[Domain: data, Collection: *, Tenant: *]]"},
		{name: "a held dot does not line up with the grant's wildcard", caller: "caller", callerRoles: []string{"non-empty-users"}, targetRoles: []string{"dot-com-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *.com]]"},
		{name: "a dot counts a multi-byte character once", caller: "caller", callerRoles: []string{"three-character-users"}, targetRoles: []string{"e-acute-and-one-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: é.]]"},
		{name: "a caller pattern of one or more characters does not cover every user, which includes an empty name", caller: "caller", callerRoles: []string{"one-or-more-users"}, targetRoles: []string{"user-updater"}, wantDenied: deniedAllUsers},
		// Each grant below reaches a name no held pattern does.
		{name: "a dot in the held pattern does not cover a granted wildcard: *.com, *com", caller: "caller", callerRoles: []string{"dot-com-users"}, targetRoles: []string{"com-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *com]]"},
		{name: "a dot in the held pattern does not cover a granted wildcard: team.*, team*", caller: "caller", callerRoles: []string{"team-dot-users"}, targetRoles: []string{"team-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: team*]]"},
		{name: "a dot in the held pattern does not cover a granted wildcard: eu-.., eu-*", caller: "caller", callerRoles: []string{"eu-dash-and-two-users"}, targetRoles: []string{"eu-dash-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: eu-*]]"},
		{name: "a negated class does not cover an alternation it matches as text", caller: "caller", callerRoles: []string{"not-a-users"}, targetRoles: []string{"eu-or-admin-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *]]"},
		{name: "a negated class does not cover every user, as rotating that user's key would need", caller: "caller", callerRoles: []string{"not-a-users"}, targetRoles: []string{"user-updater"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *]]"},
		{name: "two held patterns that each cover part of a grant do not cover it", caller: "caller", callerRoles: []string{"a-and-two-users", "a-only"}, targetRoles: []string{"a-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a*]]"},
		{name: "a held alternation does not cover a grant its branches cover together", caller: "caller", callerRoles: []string{"a-and-two-or-a-users"}, targetRoles: []string{"a-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a*]]"},
		{name: "a held pattern of bounded length does not cover a wildcard", caller: "caller", callerRoles: []string{"a-up-to-two-more-users"}, targetRoles: []string{"a-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a*]]"},
		{name: "a negated class still covers a name it matches", caller: "caller", callerRoles: []string{"not-a-users"}, targetRoles: []string{"bob-updater"}},
		{name: "a dotted suffix pattern covers itself and a name it matches", caller: "caller", callerRoles: []string{"dot-com-users"}, targetRoles: []string{"dot-com-users", "john-at-x-com"}},
		{name: "a dotted name covers itself", caller: "caller", callerRoles: []string{"john-dot-doe"}, targetRoles: []string{"john-dot-doe"}},
		{name: "a dotted pattern covers a dotted name it matches", caller: "caller", callerRoles: []string{"dot-doe-users"}, targetRoles: []string{"john-dot-doe"}},
		{name: "a confined caller pattern of one or more characters does not cover its namespace's wildcard", caller: "caller", callerNamespace: "customer1", callerRoles: []string{"non-empty-users"}, targetRoles: []string{"customer1-users"}, namespacesEnabled: true, wantDenied: deniedAllUsers},
		{name: "a confined caller holding every user covers its namespace's wildcard", caller: "caller", callerNamespace: "customer1", callerRoles: []string{"user-updater"}, targetRoles: []string{"customer1-users"}, namespacesEnabled: true},
		{name: "a pattern needing a name before a colon does not cover one that may be empty there", caller: "caller", callerRoles: []string{"one-or-more-colon-any-users"}, targetRoles: []string{"any-colon-any-users"}, namespacesEnabled: true, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: *:*]]"},
		{name: "a caller pattern of one or more characters does not cover a wildcard", caller: "caller", callerRoles: []string{"non-empty-users"}, targetRoles: []string{"user-updater"}, wantDenied: deniedAllUsers},
		{name: "a held alternation covers no dotted name, which is a pattern", caller: "caller", callerRoles: []string{"x-or-y-domain-users"}, targetRoles: []string{"john-at-x-com"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: john.doe@x.com]]"},
		{name: "a held negated class covers no dotted name, which is a pattern", caller: "caller", callerRoles: []string{"not-a-users"}, targetRoles: []string{"bob-dot-smith"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: bob.smith]]"},
		{name: "a confined caller pattern needing more after a prefix does not cover the prefix's wildcard", caller: "caller", callerNamespace: "customer1", callerRoles: []string{"customer1-a-and-two-users"}, targetRoles: []string{"customer1-a-users"}, namespacesEnabled: true, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: a*]]"},
		{name: "global caller: a bare user grant is checked in the target's namespace", namespacesEnabled: true, caller: "ops", callerRoles: []string{"bob-reader"}, targetNamespace: "customer1", targetRoles: []string{"bob-reader"}, wantDenied: "rbac: authorization, forbidden action: user 'ops' has insufficient permissions to read_users [[Domain: users, User: customer1:bob]]"},
		{name: "a grant bound to another namespace gives the target nothing", namespacesEnabled: true, caller: "ops", callerRoles: []string{"user-updater"}, targetNamespace: "customer1", targetRoles: []string{"foreign-movies-reader"}},
		{name: "namespaces disabled: the target namespace is ignored", caller: "ops", callerRoles: []string{"bob-reader"}, targetNamespace: "customer1", targetRoles: []string{"bob-reader"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := managers[tt.namespacesEnabled]
			if len(tt.callerRoles) > 0 {
				require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId(tt.caller, authentication.AuthTypeDb), tt.callerRoles))
				t.Cleanup(func() {
					require.NoError(t, m.RevokeRolesForUser(conv.UserNameWithTypeFromId(tt.caller, authentication.AuthTypeDb), tt.callerRoles...))
				})
			}
			targetRoles, err := m.GetRoles(tt.targetRoles...)
			require.NoError(t, err)
			require.Len(t, targetRoles, len(tt.targetRoles))

			principal := &models.Principal{Username: tt.caller, UserType: models.UserTypeInputDb, Namespace: tt.callerNamespace}
			// targetRoles is a map, so repeat the check until every role order has run.
			for range 100 {
				err = AuthorizeRolePermissions(context.Background(), m, tt.namespacesEnabled, principal, tt.targetNamespace, targetRoles)
				if tt.wantDenied != "" {
					require.Error(t, err)
					require.Equal(t, tt.wantDenied, err.Error())
					continue
				}
				require.NoError(t, err)
			}
		})
	}
}
