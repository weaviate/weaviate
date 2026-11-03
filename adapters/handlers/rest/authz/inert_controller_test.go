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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestInertController(t *testing.T) {
	c := InertController{}
	policy := authorization.Policy{Resource: "collections/*", Verb: authorization.READ, Domain: authorization.CollectionsDomain}

	t.Run("mutations return nil", func(t *testing.T) {
		for name, mutate := range map[string]func() error{
			"UpdateRolesPermissions": func() error { return c.UpdateRolesPermissions(map[string][]authorization.Policy{"r": {policy}}) },
			"AddRolesForUser":        func() error { return c.AddRolesForUser("db:u", []string{"r"}) },
			"RevokeRolesForUser":     func() error { return c.RevokeRolesForUser("db:u", "r") },
			"RemovePermissions":      func() error { return c.RemovePermissions("r", []*authorization.Policy{&policy}) },
		} {
			assert.NoError(t, mutate(), name)
		}
	})
}

func TestRoleAPI(t *testing.T) {
	principal := &models.Principal{Username: "admin"}
	roleName := "newRole"
	createParams := authz.CreateRoleParams{
		HTTPRequest: req,
		Body: &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{{
				Action:      String(authorization.ReadCollections),
				Collections: &models.PermissionCollections{Collection: String("*")},
			}},
		},
	}
	newHandlers := func(controller ControllerAndGetUsers) *authZHandlers {
		logger, _ := test.NewNullLogger()
		return &authZHandlers{
			authorizer: &authorization.DummyAuthorizer{},
			controller: controller,
			logger:     logger,
		}
	}
	listedRoles := func(t *testing.T, h *authZHandlers) []*models.Role {
		res, ok := h.getRoles(authz.GetRolesParams{HTTPRequest: req}, principal).(*authz.GetRolesOK)
		require.True(t, ok)
		return res.Payload
	}

	t.Run("with RBAC disabled a created role is accepted and not listed", func(t *testing.T) {
		h := newHandlers(InertController{Users: NewMockControllerAndGetUsers(t)})

		assert.IsType(t, &authz.CreateRoleCreated{}, h.createRole(createParams, principal))
		assert.Empty(t, listedRoles(t, h))
		assert.IsType(t, &authz.DeleteRoleNoContent{}, h.deleteRole(authz.DeleteRoleParams{HTTPRequest: req, ID: roleName}, principal))
	})

	t.Run("with RBAC disabled reads return empty", func(t *testing.T) {
		userSource := NewMockControllerAndGetUsers(t)
		// getUsersForRole looks up the DB users among the role's (empty) members.
		userSource.On("GetUsers").Return(map[string]apikey.UserView{}, nil).Once()
		h := newHandlers(InertController{Users: userSource})

		assert.IsType(t, &authz.GetRoleNotFound{}, h.getRole(authz.GetRoleParams{HTTPRequest: req, ID: roleName}, principal))

		groups, ok := h.getGroups(authz.GetGroupsParams{HTTPRequest: req, GroupType: string(authentication.AuthTypeOIDC)}, principal).(*authz.GetGroupsOK)
		require.True(t, ok)
		assert.Empty(t, groups.Payload)

		users, ok := h.getUsersForRole(authz.GetUsersForRoleParams{HTTPRequest: req, ID: roleName}, principal).(*authz.GetUsersForRoleOK)
		require.True(t, ok)
		assert.Empty(t, users.Payload)

		has, ok := h.hasPermission(authz.HasPermissionParams{HTTPRequest: req, ID: roleName, Body: createParams.Body.Permissions[0]}, principal).(*authz.HasPermissionOK)
		require.True(t, ok)
		assert.False(t, has.Payload)
	})

	t.Run("with RBAC disabled an existing DB user's roles read succeeds with no roles", func(t *testing.T) {
		users := NewMockControllerAndGetUsers(t)
		users.On("GetUsers", "dbUser").Return(map[string]apikey.UserView{"dbUser": {}}, nil).Once()
		h := newHandlers(InertController{Users: users})

		res, ok := h.getRolesForUser(authz.GetRolesForUserParams{HTTPRequest: req, ID: "dbUser", UserType: string(models.UserTypeInputDb)}, principal).(*authz.GetRolesForUserOK)
		require.True(t, ok)
		assert.Empty(t, res.Payload)
	})
}
