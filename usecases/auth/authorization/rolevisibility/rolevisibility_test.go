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

// TestRegexSegmentsToWildcard has one row per kind of regex syntax conv stores,
// each letting a pattern match a name its text does not contain. In the last
// row the "<ns>:" qualifier carries regex syntax, so it is dropped too.
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
	}
	for _, tt := range tests {
		t.Run(tt.resource, func(t *testing.T) {
			require.Equal(t, tt.want, regexSegmentsToWildcard(tt.resource))
		})
	}
}

// TestAuthorizeRolePermissionsCasbin runs the check against the real enforcer,
// whose unanchored verb match a mocked authorizer cannot show.
func TestAuthorizeRolePermissionsCasbin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
		config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"root"}}}, false, nil, logger)
	require.NoError(t, err)

	all := "*"
	updateUsers := func(name, users string) *models.Role {
		return &models.Role{Name: authorization.String(name), Permissions: []*models.Permission{{
			Action: authorization.String(authorization.UpdateUsers), Users: &models.PermissionUsers{Users: &users},
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
	)
	require.NoError(t, err)
	require.NoError(t, m.CreateRolesPermissions(custom))

	// A denied manage permission is reported once, under the action an admin can grant.
	deniedManageBackups := "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to manage_backups [[Domain: backups,Collection: *]]"
	// A pattern with regex syntax beyond '.' and '*' must pass as stored and as ".*".
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
		{name: "a global caller is checked against the stored resource", caller: "caller", callerRoles: []string{"movies-creator"}, targetRoles: []string{"movies-creator"}, namespacesEnabled: true, targetNamespace: "customer1"},
		{name: "an alternation of users reaching past the caller's is refused", caller: "caller", callerRoles: []string{"eu-users"}, targetRoles: []string{"eu-or-admin-users"}, wantDenied: deniedAllUsers},
		{name: "an alternation of tenants reaching past the caller's is refused", caller: "caller", callerRoles: []string{"eu-movies-reader"}, targetRoles: []string{"eu-or-secret-movies-reader"}, wantDenied: deniedAllMoviesTenants},
		{name: "every tenant of a collection covers an alternation of its tenants", caller: "caller", callerRoles: []string{"movies-reader"}, targetRoles: []string{"eu-or-secret-movies-reader"}},
		{name: "a confined caller with every user covers an alternation in its namespace", caller: "caller", callerNamespace: "customer1", callerRoles: []string{"user-updater"}, targetRoles: []string{"customer1-eu-or-admin-users"}, namespacesEnabled: true},
		{name: "a dotted name with a trailing wildcard inside the caller's pattern is covered", caller: "caller", callerRoles: []string{"eu-users"}, targetRoles: []string{"eu-west-users"}},
		{name: "a caller pattern matching the text of a wildcard is not a wildcard", caller: "caller", callerRoles: []string{"two-letter-users"}, targetRoles: []string{"eu-or-admin-users"}, wantDenied: "rbac: authorization, forbidden action: user 'caller' has insufficient permissions to update_users [[Domain: users, User: eu|admin]]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
