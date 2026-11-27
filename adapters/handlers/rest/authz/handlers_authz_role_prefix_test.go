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
	"fmt"
	"slices"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/dbuser"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

const (
	prefixCustomRole = "custom"
	prefixCaller     = "user1"
	prefixRootUser   = "root-user"
	prefixRoleNS     = "role"
	prefixCustomerNS = "customer1"
	prefixFreshRole  = "fresh"
	prefixNSUser     = "alice"
)

var prefixedRoleIDs = []string{
	conv.ROLE_NAME_PREFIX + authorization.Viewer,
	conv.ROLE_NAME_PREFIX + authorization.Admin,
	conv.ROLE_NAME_PREFIX + authorization.Root,
	conv.ROLE_NAME_PREFIX + authorization.ReadOnly,
	conv.ROLE_NAME_PREFIX + authorization.MetadataReader,
	conv.ROLE_NAME_PREFIX + prefixCustomRole,
}

type rolePrefixCase struct {
	id                string
	namespacesEnabled bool
}

func (c rolePrefixCase) name() string {
	return fmt.Sprintf("%s/namespaces=%t", c.id, c.namespacesEnabled)
}

func (c rolePrefixCase) envVarAlias() bool {
	return slices.Contains(authorization.EnvVarRoles, conv.TrimRoleNamePrefix(c.id))
}

func rolePrefixCases(namespaceModes ...bool) []rolePrefixCase {
	if len(namespaceModes) == 0 {
		namespaceModes = []bool{false, true}
	}
	var out []rolePrefixCase
	for _, ns := range namespaceModes {
		for _, id := range prefixedRoleIDs {
			out = append(out, rolePrefixCase{id: id, namespacesEnabled: ns})
		}
	}
	return out
}

func rolePrefixPrincipal(namespacesEnabled bool) *models.Principal {
	return &models.Principal{Username: prefixCaller, UserType: models.UserTypeInputDb, IsGlobalOperator: namespacesEnabled}
}

func rolePrefixHandler(t *testing.T, namespacesEnabled bool) (*authZHandlers, *MockControllerAndGetUsers) {
	t.Helper()
	stored := map[string][]authorization.Policy{
		authorization.Viewer:                      {collPolicy(authorization.READ, "*")},
		authorization.Admin:                       {collPolicy(authorization.CREATE, "*")},
		authorization.Root:                        {collPolicy(authorization.DELETE, "*")},
		authorization.ReadOnly:                    {collPolicy(authorization.READ, "*")},
		authorization.MetadataReader:              {collPolicy(authorization.READ, "*")},
		prefixCustomRole:                          {collPolicy(authorization.READ, "Movies")},
		prefixCustomerNS + ":" + prefixCustomRole: {collPolicy(authorization.READ, prefixCustomerNS+":Movies")},
	}

	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles").Return(stored, nil).Maybe()
	controller.On("GetRoles", mock.Anything).Return(func(names ...string) map[string][]authorization.Policy {
		out := map[string][]authorization.Policy{}
		for _, n := range names {
			if p, ok := stored[conv.TrimRoleNamePrefix(n)]; ok {
				out[conv.TrimRoleNamePrefix(n)] = p
			}
		}
		return out
	}, nil).Maybe()
	controller.On("CreateRolesPermissions", mock.Anything).Return(nil).Maybe()
	controller.On("UpdateRolesPermissions", mock.Anything).Return(nil).Maybe()
	controller.On("RemovePermissions", mock.Anything, mock.Anything).Return(nil).Maybe()
	controller.On("DeleteRoles", mock.Anything).Return(nil).Maybe()
	controller.On("AddRolesForUser", mock.Anything, mock.Anything).Return(nil).Maybe()
	controller.On("RevokeRolesForUser", mock.Anything, mock.Anything).Return(nil).Maybe()
	controller.On("HasPermission", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	controller.On("GetUsersOrGroupForRole", mock.Anything, mock.Anything, mock.Anything).Return([]string{prefixRootUser}, nil).Maybe()
	controller.On("GetUsers", mock.Anything).Return(func(ids ...string) map[string]dbuser.View {
		out := map[string]dbuser.View{}
		for _, id := range ids {
			if ns := namespacing.NamespaceFromQualified(id); ns != "" {
				out[id] = dbuser.View{Id: id, Active: true, Namespace: ns}
			}
		}
		return out
	}, nil).Maybe()

	logger, _ := test.NewNullLogger()
	return &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		apiKeysConfigs:    config.StaticAPIKey{Enabled: true, Users: []string{prefixCaller, prefixRootUser}},
		rbacconfig:        rbacconf.Config{Enabled: true, RootUsers: []string{prefixRootUser}},
		namespacesEnabled: namespacesEnabled,
	}, controller
}

func rolePrefixPermission() *models.Permission {
	return &models.Permission{
		Action:      String(authorization.CreateCollections),
		Collections: &models.PermissionCollections{Collection: String("Films")},
	}
}

func TestRolePrefixAddPermissions(t *testing.T) {
	for _, tc := range rolePrefixCases() {
		t.Run(tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
			res := h.addPermissions(authz.AddPermissionsParams{
				HTTPRequest: req,
				ID:          tc.id,
				Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{rolePrefixPermission()}},
			}, rolePrefixPrincipal(tc.namespacesEnabled))
			require.IsType(t, &authz.AddPermissionsBadRequest{}, res)
			controller.AssertNotCalled(t, "UpdateRolesPermissions", mock.Anything)
		})
	}
	for _, ns := range []bool{false, true} {
		t.Run(fmt.Sprintf("bare custom/namespaces=%t", ns), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, ns)
			res := h.addPermissions(authz.AddPermissionsParams{
				HTTPRequest: req,
				ID:          prefixCustomRole,
				Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{rolePrefixPermission()}},
			}, rolePrefixPrincipal(ns))
			require.IsType(t, &authz.AddPermissionsOK{}, res)
			controller.AssertCalled(t, "UpdateRolesPermissions", mock.MatchedBy(func(m map[string][]authorization.Policy) bool {
				_, ok := m[prefixCustomRole]
				return ok && len(m) == 1
			}))
		})
	}
}

func TestRolePrefixRemovePermissions(t *testing.T) {
	for _, tc := range rolePrefixCases() {
		t.Run(tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
			res := h.removePermissions(authz.RemovePermissionsParams{
				HTTPRequest: req,
				ID:          tc.id,
				Body:        authz.RemovePermissionsBody{Permissions: []*models.Permission{rolePrefixPermission()}},
			}, rolePrefixPrincipal(tc.namespacesEnabled))
			require.IsType(t, &authz.RemovePermissionsBadRequest{}, res)
			controller.AssertNotCalled(t, "RemovePermissions", mock.Anything, mock.Anything)
		})
	}
	for _, ns := range []bool{false, true} {
		t.Run(fmt.Sprintf("bare custom/namespaces=%t", ns), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, ns)
			res := h.removePermissions(authz.RemovePermissionsParams{
				HTTPRequest: req,
				ID:          prefixCustomRole,
				Body:        authz.RemovePermissionsBody{Permissions: []*models.Permission{rolePrefixPermission()}},
			}, rolePrefixPrincipal(ns))
			require.IsType(t, &authz.RemovePermissionsOK{}, res)
			controller.AssertCalled(t, "RemovePermissions", prefixCustomRole, mock.Anything)
		})
	}
}

func TestRolePrefixDeleteRole(t *testing.T) {
	for _, tc := range rolePrefixCases() {
		t.Run(tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
			res := h.deleteRole(authz.DeleteRoleParams{HTTPRequest: req, ID: tc.id}, rolePrefixPrincipal(tc.namespacesEnabled))
			require.IsType(t, &authz.DeleteRoleBadRequest{}, res)
			controller.AssertNotCalled(t, "DeleteRoles", mock.Anything)
		})
	}
	for _, ns := range []bool{false, true} {
		t.Run(fmt.Sprintf("bare custom/namespaces=%t", ns), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, ns)
			res := h.deleteRole(authz.DeleteRoleParams{HTTPRequest: req, ID: prefixCustomRole}, rolePrefixPrincipal(ns))
			require.IsType(t, &authz.DeleteRoleNoContent{}, res)
			controller.AssertCalled(t, "DeleteRoles", prefixCustomRole)
		})
	}
}

func TestRolePrefixAssignRevokeUser(t *testing.T) {
	assign := func(h *authZHandlers, role string, ns bool) middleware.Responder {
		return h.assignRoleToUser(authz.AssignRoleToUserParams{
			HTTPRequest: req,
			ID:          prefixCaller,
			Body:        authz.AssignRoleToUserBody{Roles: []string{role}, UserType: models.UserTypeInputDb},
		}, rolePrefixPrincipal(ns))
	}
	revoke := func(h *authZHandlers, role string, ns bool) middleware.Responder {
		return h.revokeRoleFromUser(authz.RevokeRoleFromUserParams{
			HTTPRequest: req,
			ID:          prefixRootUser,
			Body:        authz.RevokeRoleFromUserBody{Roles: []string{role}, UserType: models.UserTypeInputDb},
		}, rolePrefixPrincipal(ns))
	}

	for _, tc := range rolePrefixCases() {
		t.Run("assign/"+tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
			res := assign(h, tc.id, tc.namespacesEnabled)
			if tc.envVarAlias() {
				require.IsType(t, &authz.AssignRoleToUserForbidden{}, res)
			} else {
				require.IsType(t, &authz.AssignRoleToUserBadRequest{}, res)
			}
			controller.AssertNotCalled(t, "AddRolesForUser", mock.Anything, mock.Anything)
		})
		t.Run("revoke/"+tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
			res := revoke(h, tc.id, tc.namespacesEnabled)
			if tc.envVarAlias() {
				require.IsType(t, &authz.RevokeRoleFromUserForbidden{}, res)
			} else {
				require.IsType(t, &authz.RevokeRoleFromUserBadRequest{}, res)
			}
			controller.AssertNotCalled(t, "RevokeRolesForUser", mock.Anything, mock.Anything)
		})
	}
	for _, ns := range []bool{false, true} {
		t.Run(fmt.Sprintf("assign bare custom/namespaces=%t", ns), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, ns)
			require.IsType(t, &authz.AssignRoleToUserOK{}, assign(h, prefixCustomRole, ns))
			controller.AssertCalled(t, "AddRolesForUser", mock.Anything, []string{prefixCustomRole})
		})
		t.Run(fmt.Sprintf("revoke bare custom/namespaces=%t", ns), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, ns)
			require.IsType(t, &authz.RevokeRoleFromUserOK{}, revoke(h, prefixCustomRole, ns))
			controller.AssertCalled(t, "RevokeRolesForUser", mock.Anything, prefixCustomRole)
		})
	}
}

func TestRolePrefixAssignRevokeGroup(t *testing.T) {
	assign := func(h *authZHandlers, role string, ns bool) middleware.Responder {
		return h.assignRoleToGroup(authz.AssignRoleToGroupParams{
			HTTPRequest: req,
			ID:          "group1",
			Body:        authz.AssignRoleToGroupBody{Roles: []string{role}, GroupType: models.GroupTypeOidc},
		}, rolePrefixPrincipal(ns))
	}
	revoke := func(h *authZHandlers, role string, ns bool) middleware.Responder {
		return h.revokeRoleFromGroup(authz.RevokeRoleFromGroupParams{
			HTTPRequest: req,
			ID:          "group1",
			Body:        authz.RevokeRoleFromGroupBody{Roles: []string{role}, GroupType: models.GroupTypeOidc},
		}, rolePrefixPrincipal(ns))
	}

	for _, tc := range rolePrefixCases() {
		t.Run("assign/"+tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
			res := assign(h, tc.id, tc.namespacesEnabled)
			if tc.envVarAlias() {
				require.IsType(t, &authz.AssignRoleToGroupForbidden{}, res)
			} else {
				require.IsType(t, &authz.AssignRoleToGroupBadRequest{}, res)
			}
			controller.AssertNotCalled(t, "AddRolesForUser", mock.Anything, mock.Anything)
		})
		t.Run("revoke/"+tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
			res := revoke(h, tc.id, tc.namespacesEnabled)
			if tc.envVarAlias() {
				require.IsType(t, &authz.RevokeRoleFromGroupForbidden{}, res)
			} else {
				require.IsType(t, &authz.RevokeRoleFromGroupBadRequest{}, res)
			}
			controller.AssertNotCalled(t, "RevokeRolesForUser", mock.Anything, mock.Anything)
		})
	}
	for _, ns := range []bool{false, true} {
		t.Run(fmt.Sprintf("assign bare custom/namespaces=%t", ns), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, ns)
			require.IsType(t, &authz.AssignRoleToGroupOK{}, assign(h, prefixCustomRole, ns))
			controller.AssertCalled(t, "AddRolesForUser", conv.PrefixGroupName("group1"), []string{prefixCustomRole})
		})
		t.Run(fmt.Sprintf("revoke bare custom/namespaces=%t", ns), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, ns)
			require.IsType(t, &authz.RevokeRoleFromGroupOK{}, revoke(h, prefixCustomRole, ns))
			controller.AssertCalled(t, "RevokeRolesForUser", conv.PrefixGroupName("group1"), prefixCustomRole)
		})
	}
}

func TestRolePrefixRoleReads(t *testing.T) {
	type readCall func(h *authZHandlers, id string, ns bool) middleware.Responder
	reads := []struct {
		name         string
		call         readCall
		envForbidden bool
		badRequest   any
		forbidden    any
		ok           any
	}{
		{
			name: "getRole",
			call: func(h *authZHandlers, id string, ns bool) middleware.Responder {
				return h.getRole(authz.GetRoleParams{HTTPRequest: req, ID: id}, rolePrefixPrincipal(ns))
			},
			badRequest: &authz.GetRoleBadRequest{},
			ok:         &authz.GetRoleOK{},
		},
		{
			name: "hasPermission",
			call: func(h *authZHandlers, id string, ns bool) middleware.Responder {
				return h.hasPermission(authz.HasPermissionParams{HTTPRequest: req, ID: id, Body: rolePrefixPermission()}, rolePrefixPrincipal(ns))
			},
			badRequest: &authz.HasPermissionBadRequest{},
			ok:         &authz.HasPermissionOK{},
		},
		{
			name: "getUsersForRole",
			call: func(h *authZHandlers, id string, ns bool) middleware.Responder {
				return h.getUsersForRole(authz.GetUsersForRoleParams{HTTPRequest: req, ID: id}, rolePrefixPrincipal(ns))
			},
			envForbidden: true,
			badRequest:   &authz.GetUsersForRoleBadRequest{},
			forbidden:    &authz.GetUsersForRoleForbidden{},
			ok:           &authz.GetUsersForRoleOK{},
		},
		{
			name: "getGroupsForRole",
			call: func(h *authZHandlers, id string, ns bool) middleware.Responder {
				return h.getGroupsForRole(authz.GetGroupsForRoleParams{HTTPRequest: req, ID: id}, rolePrefixPrincipal(ns))
			},
			envForbidden: true,
			badRequest:   &authz.GetGroupsForRoleBadRequest{},
			forbidden:    &authz.GetGroupsForRoleForbidden{},
			ok:           &authz.GetGroupsForRoleOK{},
		},
	}

	for _, rd := range reads {
		for _, tc := range rolePrefixCases() {
			t.Run(rd.name+"/"+tc.name(), func(t *testing.T) {
				h, controller := rolePrefixHandler(t, tc.namespacesEnabled)
				res := rd.call(h, tc.id, tc.namespacesEnabled)
				if rd.envForbidden && tc.envVarAlias() {
					require.IsType(t, rd.forbidden, res)
				} else {
					require.IsType(t, rd.badRequest, res)
				}
				controller.AssertNotCalled(t, "HasPermission", mock.Anything, mock.Anything)
				controller.AssertNotCalled(t, "GetUsersOrGroupForRole", mock.Anything, mock.Anything, mock.Anything)
			})
		}
		for _, ns := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/bare custom/namespaces=%t", rd.name, ns), func(t *testing.T) {
				h, _ := rolePrefixHandler(t, ns)
				require.IsType(t, rd.ok, rd.call(h, prefixCustomRole, ns))
			})
		}
	}
}

func TestRolePrefixGetUsersForRoleDeprecated(t *testing.T) {
	for _, tc := range rolePrefixCases(false) {
		t.Run(tc.name(), func(t *testing.T) {
			h, controller := rolePrefixHandler(t, false)
			res := h.getUsersForRoleDeprecated(authz.GetUsersForRoleDeprecatedParams{HTTPRequest: req, ID: tc.id}, rolePrefixPrincipal(false))
			require.IsType(t, &authz.GetUsersForRoleDeprecatedBadRequest{}, res)
			controller.AssertNotCalled(t, "GetUsersOrGroupForRole", mock.Anything, mock.Anything, mock.Anything)
		})
	}
	t.Run("bare custom", func(t *testing.T) {
		h, _ := rolePrefixHandler(t, false)
		res := h.getUsersForRoleDeprecated(authz.GetUsersForRoleDeprecatedParams{HTTPRequest: req, ID: prefixCustomRole}, rolePrefixPrincipal(false))
		require.IsType(t, &authz.GetUsersForRoleDeprecatedOK{}, res)
	})
	t.Run("bare root by non-root", func(t *testing.T) {
		h, controller := rolePrefixHandler(t, false)
		res := h.getUsersForRoleDeprecated(authz.GetUsersForRoleDeprecatedParams{HTTPRequest: req, ID: authorization.Root}, rolePrefixPrincipal(false))
		require.IsType(t, &authz.GetUsersForRoleForbidden{}, res)
		controller.AssertNotCalled(t, "GetUsersOrGroupForRole", mock.Anything, mock.Anything, mock.Anything)
	})
}

func TestValidateRoleID(t *testing.T) {
	tests := []struct {
		id      string
		wantErr bool
	}{
		{id: "role:viewer", wantErr: true},
		{id: "role:admin", wantErr: true},
		{id: "role:root", wantErr: true},
		{id: "role:custom", wantErr: true},
		{id: "role:", wantErr: true},
		{id: "role:role:viewer", wantErr: true},
		{id: "viewer"},
		{id: "custom"},
		{id: "role"},
		{id: "roles"},
		{id: "myrole:viewer"},
		{id: "Role:viewer"},
		{id: "customer1:editor"},
		{id: ""},
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			err := validateRoleID(tt.id)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, namespacing.ErrInvalidRoleName)
			assert.Contains(t, err.Error(), tt.id)
		})
	}
}

func TestValidateEnvVarRolesTrimsPrefix(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: authorization.Root, wantErr: true},
		{name: authorization.ReadOnly, wantErr: true},
		{name: authorization.MetadataReader, wantErr: true},
		{name: "role:" + authorization.Root, wantErr: true},
		{name: "role:" + authorization.ReadOnly, wantErr: true},
		{name: "role:" + authorization.MetadataReader, wantErr: true},
		{name: authorization.Viewer},
		{name: authorization.Admin},
		{name: "role:" + authorization.Viewer},
		{name: prefixCustomRole},
		{name: "rootx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEnvVarRoles(tt.name)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.name)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBuiltInRoleGuardsTrimPrefix(t *testing.T) {
	for _, builtIn := range authorization.BuiltInRoles {
		id := conv.ROLE_NAME_PREFIX + builtIn
		t.Run(id, func(t *testing.T) {
			h, _ := rolePrefixHandler(t, false)
			principal := rolePrefixPrincipal(false)
			add := h.addPermissions(authz.AddPermissionsParams{HTTPRequest: req, ID: id, Body: authz.AddPermissionsBody{Permissions: []*models.Permission{rolePrefixPermission()}}}, principal)
			require.IsType(t, &authz.AddPermissionsBadRequest{}, add)
			assert.Contains(t, add.(*authz.AddPermissionsBadRequest).Payload.Error[0].Message, "built-in role")
			remove := h.removePermissions(authz.RemovePermissionsParams{HTTPRequest: req, ID: id, Body: authz.RemovePermissionsBody{Permissions: []*models.Permission{rolePrefixPermission()}}}, principal)
			require.IsType(t, &authz.RemovePermissionsBadRequest{}, remove)
			assert.Contains(t, remove.(*authz.RemovePermissionsBadRequest).Payload.Error[0].Message, "built-in role")
			del := h.deleteRole(authz.DeleteRoleParams{HTTPRequest: req, ID: id}, principal)
			require.IsType(t, &authz.DeleteRoleBadRequest{}, del)
			assert.Contains(t, del.(*authz.DeleteRoleBadRequest).Payload.Error[0].Message, "built-in role")
		})
	}
}

func TestRolePrefixConfinedNamespaceAlias(t *testing.T) {
	perm := []*models.Permission{rolePrefixPermission()}
	calls := []struct {
		name     string
		call     func(h *authZHandlers, p *models.Principal, role string) middleware.Responder
		mutation string
		refused  any
		ok       any
	}{
		{
			name: "deleteRole",
			call: func(h *authZHandlers, p *models.Principal, role string) middleware.Responder {
				return h.deleteRole(authz.DeleteRoleParams{HTTPRequest: req, ID: role}, p)
			},
			mutation: "DeleteRoles",
			refused:  &authz.DeleteRoleBadRequest{},
			ok:       &authz.DeleteRoleNoContent{},
		},
		{
			name: "addPermissions",
			call: func(h *authZHandlers, p *models.Principal, role string) middleware.Responder {
				return h.addPermissions(authz.AddPermissionsParams{HTTPRequest: req, ID: role, Body: authz.AddPermissionsBody{Permissions: perm}}, p)
			},
			mutation: "UpdateRolesPermissions",
			refused:  &authz.AddPermissionsBadRequest{},
			ok:       &authz.AddPermissionsOK{},
		},
		{
			name: "removePermissions",
			call: func(h *authZHandlers, p *models.Principal, role string) middleware.Responder {
				return h.removePermissions(authz.RemovePermissionsParams{HTTPRequest: req, ID: role, Body: authz.RemovePermissionsBody{Permissions: perm}}, p)
			},
			mutation: "RemovePermissions",
			refused:  &authz.RemovePermissionsBadRequest{},
			ok:       &authz.RemovePermissionsOK{},
		},
		{
			name: "assignRoleToUser",
			call: func(h *authZHandlers, p *models.Principal, role string) middleware.Responder {
				return h.assignRoleToUser(authz.AssignRoleToUserParams{HTTPRequest: req, ID: prefixNSUser, Body: authz.AssignRoleToUserBody{Roles: []string{role}, UserType: models.UserTypeInputDb}}, p)
			},
			mutation: "AddRolesForUser",
			refused:  &authz.AssignRoleToUserBadRequest{},
			ok:       &authz.AssignRoleToUserOK{},
		},
		{
			name: "revokeRoleFromUser",
			call: func(h *authZHandlers, p *models.Principal, role string) middleware.Responder {
				return h.revokeRoleFromUser(authz.RevokeRoleFromUserParams{HTTPRequest: req, ID: prefixNSUser, Body: authz.RevokeRoleFromUserBody{Roles: []string{role}, UserType: models.UserTypeInputDb}}, p)
			},
			mutation: "RevokeRolesForUser",
			refused:  &authz.RevokeRoleFromUserBadRequest{},
			ok:       &authz.RevokeRoleFromUserOK{},
		},
		{
			name: "getRole",
			call: func(h *authZHandlers, p *models.Principal, role string) middleware.Responder {
				return h.getRole(authz.GetRoleParams{HTTPRequest: req, ID: role}, p)
			},
			refused: &authz.GetRoleBadRequest{},
			ok:      &authz.GetRoleOK{},
		},
		{
			name: "getUsersForRole",
			call: func(h *authZHandlers, p *models.Principal, role string) middleware.Responder {
				return h.getUsersForRole(authz.GetUsersForRoleParams{HTTPRequest: req, ID: role}, p)
			},
			mutation: "GetUsersOrGroupForRole",
			refused:  &authz.GetUsersForRoleBadRequest{},
			ok:       &authz.GetUsersForRoleOK{},
		},
	}

	for _, c := range calls {
		t.Run(c.name+"/namespace="+prefixRoleNS, func(t *testing.T) {
			h, controller := rolePrefixHandler(t, true)
			assert.IsType(t, c.refused, c.call(h, rolePrefixConfinedPrincipal(prefixRoleNS), prefixCustomRole))
			if c.mutation != "" {
				require.False(t, rolePrefixMethodCalled(controller, c.mutation))
			}
		})
		t.Run(c.name+"/namespace="+prefixCustomerNS, func(t *testing.T) {
			h, controller := rolePrefixHandler(t, true)
			require.IsType(t, c.ok, c.call(h, rolePrefixConfinedPrincipal(prefixCustomerNS), prefixCustomRole))
			if c.mutation != "" {
				require.True(t, rolePrefixMethodCalled(controller, c.mutation))
			}
		})
	}
}

func TestRolePrefixConfinedNamespaceCreate(t *testing.T) {
	tests := []struct {
		namespace string
		name      string
		want      any
		stored    string
	}{
		{namespace: prefixRoleNS, name: prefixCustomRole, want: &authz.CreateRoleUnprocessableEntity{}},
		{namespace: prefixRoleNS, name: prefixFreshRole, want: &authz.CreateRoleUnprocessableEntity{}},
		{namespace: prefixCustomerNS, name: prefixFreshRole, want: &authz.CreateRoleCreated{}, stored: prefixCustomerNS + ":" + prefixFreshRole},
	}
	for _, tt := range tests {
		t.Run(tt.namespace+"/"+tt.name, func(t *testing.T) {
			h, controller := rolePrefixHandler(t, true)
			res := h.createRole(authz.CreateRoleParams{
				HTTPRequest: req,
				Body:        &models.Role{Name: String(tt.name), Permissions: []*models.Permission{rolePrefixPermission()}},
			}, rolePrefixConfinedPrincipal(tt.namespace))
			assert.IsType(t, tt.want, res)
			if tt.stored == "" {
				controller.AssertNotCalled(t, "CreateRolesPermissions", mock.Anything)
				return
			}
			controller.AssertCalled(t, "CreateRolesPermissions", mock.MatchedBy(func(m map[string][]authorization.Policy) bool {
				_, ok := m[tt.stored]
				return ok && len(m) == 1
			}))
		})
	}
}

func rolePrefixConfinedPrincipal(namespace string) *models.Principal {
	return &models.Principal{Username: prefixNSUser, UserType: models.UserTypeInputDb, Namespace: namespace}
}

func rolePrefixMethodCalled(controller *MockControllerAndGetUsers, method string) bool {
	return slices.ContainsFunc(controller.Calls, func(c mock.Call) bool { return c.Method == method })
}
