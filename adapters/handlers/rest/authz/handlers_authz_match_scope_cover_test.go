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

package authz

import (
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestMatchScopeGrantsReachingPastHeld drives a MATCH-scope role manager
// through the REST handlers with the real enforcer. Each refused grant reaches
// names the manager's own pattern does not.
func TestMatchScopeGrantsReachingPastHeld(t *testing.T) {
	users := func(action, pattern string) *models.Permission {
		return &models.Permission{Action: String(action), Users: &models.PermissionUsers{Users: String(pattern)}}
	}
	matchRoles := func(action, pattern string) *models.Permission {
		return &models.Permission{Action: String(action), Roles: &models.PermissionRoles{Role: String(pattern), Scope: String(models.PermissionRolesScopeMatch)}}
	}
	setup := func(t *testing.T) (*authZHandlers, *models.Principal) {
		t.Helper()
		logger, _ := test.NewNullLogger()
		m, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true},
			config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"root"}}}, false, nil, logger)
		require.NoError(t, err)
		policies, err := conv.RolesToPolicies(
			&models.Role{Name: String("manager"), Permissions: []*models.Permission{
				matchRoles(authorization.CreateRoles, "*"),
				matchRoles(authorization.UpdateRoles, "*"),
				users(authorization.ReadUsers, "a*"),
				users(authorization.UpdateUsers, "a.."),
				users(authorization.UpdateUsers, "a"),
				users(authorization.DeleteUsers, "[^a]*"),
				users(authorization.AssignAndRevokeUsers, "*"),
			}},
			&models.Role{Name: String("a-updaters"), Permissions: []*models.Permission{users(authorization.UpdateUsers, "a*")}},
			&models.Role{Name: String("abc-updaters"), Permissions: []*models.Permission{users(authorization.UpdateUsers, "abc")}},
		)
		require.NoError(t, err)
		require.NoError(t, m.CreateRolesPermissions(policies))
		require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("mallory", authentication.AuthTypeDb), []string{"manager"}))
		return &authZHandlers{authorizer: m, controller: rbacController{m}, logger: logger},
			&models.Principal{Username: "mallory", UserType: models.UserTypeInputDb}
	}

	tests := []struct {
		name      string
		perm      *models.Permission
		forbidden bool
	}{
		{name: "an optional letter reaches every user", perm: users(authorization.ReadUsers, "a?*"), forbidden: true},
		{name: "a wildcard reaches past a fixed number of letters, even with the prefix held", perm: users(authorization.UpdateUsers, "a*"), forbidden: true},
		{name: "a held negated class covers no alternation", perm: users(authorization.DeleteUsers, "eu|admin"), forbidden: true},
		{name: "a held negated class covers no wildcard", perm: users(authorization.DeleteUsers, "*"), forbidden: true},
		{name: "a held negated class covers a name it matches", perm: users(authorization.DeleteUsers, "bob")},
		{name: "a negated class reaches past a prefix", perm: users(authorization.ReadUsers, "[^b]*"), forbidden: true},
		{name: "a narrower prefix", perm: users(authorization.ReadUsers, "ab*")},
		{name: "a name of the held length", perm: users(authorization.UpdateUsers, "abc")},
		{name: "a class needs a held wildcard", perm: users(authorization.UpdateUsers, "a[bc]d"), forbidden: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("add permissions", func(t *testing.T) {
				h, mallory := setup(t)
				res := h.addPermissions(authz.AddPermissionsParams{
					ID:          "manager",
					HTTPRequest: req,
					Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{tt.perm}},
				}, mallory)
				if tt.forbidden {
					require.IsType(t, &authz.AddPermissionsForbidden{}, res)
					return
				}
				require.IsType(t, &authz.AddPermissionsOK{}, res)
			})
			t.Run("create role", func(t *testing.T) {
				h, mallory := setup(t)
				res := h.createRole(authz.CreateRoleParams{
					HTTPRequest: req,
					Body:        &models.Role{Name: String("granted"), Permissions: []*models.Permission{tt.perm}},
				}, mallory)
				if tt.forbidden {
					require.IsType(t, &authz.CreateRoleForbidden{}, res)
					return
				}
				require.IsType(t, &authz.CreateRoleCreated{}, res)
			})
		})
	}

	// Assigning an existing role runs the same check against the target.
	for _, tc := range []struct {
		role      string
		forbidden bool
	}{{role: "a-updaters", forbidden: true}, {role: "abc-updaters"}} {
		t.Run("assign "+tc.role, func(t *testing.T) {
			h, mallory := setup(t)
			res := h.assignRoleToUser(authz.AssignRoleToUserParams{
				ID:          "bob",
				HTTPRequest: req,
				Body:        authz.AssignRoleToUserBody{Roles: []string{tc.role}, UserType: models.UserTypeInputDb},
			}, mallory)
			if tc.forbidden {
				require.IsType(t, &authz.AssignRoleToUserForbidden{}, res)
				return
			}
			// Past the permission check, the handler looks for bob, who does not exist.
			require.IsType(t, &authz.AssignRoleToUserNotFound{}, res)
		})
	}
}
