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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestAuthzRolePrefixAliasRefused(t *testing.T) {
	_, down := composeUpShared(t)
	defer down()

	managerUser := "custom-user"
	managerAuth := helper.CreateAuth(sharedPlainPrincipals[managerUser])
	matchAll := &models.PermissionRoles{Role: String("*"), Scope: String(models.PermissionRolesScopeMatch)}
	createAnyCollection := &models.Permission{
		Action:      String(authorization.CreateCollections),
		Collections: &models.PermissionCollections{Collection: String("*")},
	}
	helper.CreateRoleAndAssign(t, sharedRootKey, managerUser, "prefix-role-manager",
		&models.Permission{Action: String(authorization.UpdateRoles), Roles: matchAll},
		&models.Permission{Action: String(authorization.DeleteRoles), Roles: matchAll},
		&models.Permission{Action: String(authorization.AssignAndRevokeUsers), Users: &models.PermissionUsers{Users: String("*")}},
		createAnyCollection,
	)

	t.Run("add permissions to prefixed viewer is refused", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().
				WithID(conv.ROLE_NAME_PREFIX+authorization.Viewer).
				WithBody(authz.AddPermissionsBody{Permissions: []*models.Permission{createAnyCollection}}),
			managerAuth,
		)
		var badRequest *authz.AddPermissionsBadRequest
		require.True(t, errors.As(err, &badRequest), "expected 400, got %v", err)

		err = createClass(t, &models.Class{Class: "RolePrefixViewerProbe"}, helper.CreateAuth(sharedViewerKey))
		var forbidden *clschema.SchemaObjectsCreateForbidden
		require.True(t, errors.As(err, &forbidden), "viewer must not create collections, got %v", err)
	})

	t.Run("delete prefixed admin is refused", func(t *testing.T) {
		_, err := helper.Client(t).Authz.DeleteRole(
			authz.NewDeleteRoleParams().WithID(conv.ROLE_NAME_PREFIX+authorization.Admin),
			managerAuth,
		)
		var badRequest *authz.DeleteRoleBadRequest
		require.True(t, errors.As(err, &badRequest), "expected 400, got %v", err)

		admin := helper.GetRoleByName(t, sharedRootKey, authorization.Admin)
		require.Equal(t, authorization.Admin, *admin.Name)
		require.NotEmpty(t, admin.Permissions)
	})

	t.Run("revoke prefixed root from root user is refused", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().
				WithID(sharedRoot2User).
				WithBody(authz.RevokeRoleFromUserBody{
					Roles:    []string{conv.ROLE_NAME_PREFIX + authorization.Root},
					UserType: models.UserTypeInputDb,
				}),
			managerAuth,
		)
		var forbidden *authz.RevokeRoleFromUserForbidden
		require.True(t, errors.As(err, &forbidden), "expected 403, got %v", err)

		names := make([]string, 0)
		for _, r := range helper.GetRolesForUser(t, sharedRoot2User, sharedRootKey, false) {
			names = append(names, *r.Name)
		}
		require.Contains(t, names, authorization.Root)
	})
}
