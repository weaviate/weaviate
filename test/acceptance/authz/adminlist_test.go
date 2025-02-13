//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAdminlistWithRBACEndpoints(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"
	customKey := "custom-key"
	customUser := "custom-user"
	readonlyKey := "readonly-key"
	readonlyUser := "readonly-user"

	adminAuth := helper.CreateAuth(adminKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).WithUserApiKey(readonlyUser, readonlyKey).
		WithAdminListAdmins(adminUser).WithAdminListUsers(readonlyUser).
		Start(ctx)
	require.NoError(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()
	defer compose.Terminate(ctx)

	// as admin you can all the endpoints, but nothing happens
	testRoleName := "testing"
	testRole := &models.Role{Name: &testRoleName, Permissions: []*models.Permission{
		{
			Action: String(authorization.CreateCollections),
			Collections: &models.PermissionCollections{
				Collection: String("*"),
			},
		},
	}}

	helper.Client(t).Authz.DeleteRole(
		authz.NewDeleteRoleParams().WithID(testRoleName),
		adminAuth,
	)
	helper.DeleteRole(t, adminKey, testRoleName)
	helper.CreateRole(t, adminKey, testRole)

	roles := helper.GetRoles(t, adminKey)
	require.Len(t, roles, 0)

	// as read only user you can cannot do anything
	_, err = helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), helper.CreateAuth(readonlyKey))
	require.NoError(t, err)

	_, err = helper.Client(t).Authz.CreateRole(authz.NewCreateRoleParams().WithBody(testRole), helper.CreateAuth(readonlyKey))
	require.Error(t, err)

	// if you are not admin or readonly you also cannot do anything
	_, err = helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), helper.CreateAuth(customKey))
	require.NoError(t, err)
}
