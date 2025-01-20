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
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzGetOwnRole(t *testing.T) {
	var err error

	customUser := "custom-user"
	customKey := "custom-key"
	testingRole := "testingOwnRole"
	adminKey := "admin-key"
	adminUser := "admin-user"
	adminAuth := helper.CreateAuth(adminKey)

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	helper.Client(t).Authz.DeleteRole(
		authz.NewDeleteRoleParams().WithID(testingRole),
		adminAuth,
	)

	t.Run("Get own roles - empty", func(t *testing.T) {
		roles := helper.GetRolesForOwnUser(t, customKey)
		require.Len(t, roles, 0)
	})

	_, err = helper.Client(t).Authz.CreateRole(
		authz.NewCreateRoleParams().WithBody(&models.Role{
			Name: &testingRole,
			Permissions: []*models.Permission{{
				Action:      String(authorization.CreateCollections),
				Collections: &models.PermissionCollections{Collection: String("*")},
			}},
		}),
		adminAuth,
	)
	require.NoError(t, err)
	helper.AssignRoleToUser(t, "admin-key", testingRole, customUser)

	t.Run("Get own roles - existing role", func(t *testing.T) {
		roles := helper.GetRolesForOwnUser(t, customKey)
		require.Len(t, roles, 1)
		require.Equal(t, testingRole, *roles[0].Name)
	})
}

func TestUserWithSimilarBuiltInRoleName(t *testing.T) {
	customUser := "custom-admin-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)
	testingRole := "testingOwnRole"
	adminKey := "admin-key"
	adminUser := "admin-user"
	adminAuth := helper.CreateAuth(adminKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		WithRBAC().WithRbacAdmins(adminUser).Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	helper.Client(t).Authz.DeleteRole(
		authz.NewDeleteRoleParams().WithID(testingRole),
		adminAuth,
	)

	t.Run("Create role with custom user - fail", func(t *testing.T) {
		_, err = helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name: &testingRole,
				Permissions: []*models.Permission{
					{
						Action: String(authorization.CreateCollections),
						Collections: &models.PermissionCollections{
							Collection: String("*"),
						},
					},
				},
			}),
			customAuth,
		)
		require.Error(t, err)
	})
}

func TestAuthzBuiltInRolesJourney(t *testing.T) {
	var err error

	adminUser := "admin-user"
	adminKey := "admin-key"
	adminRole := "admin"

	clientAuth := helper.CreateAuth(adminKey)

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, nil, nil)
	defer down()

	t.Run("get all roles to check if i have perm.", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles, len(roles))
	})

	t.Run("fail to create builtin role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name: &adminRole,
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			}),
			clientAuth,
		)
		require.NotNil(t, err)
		var parsed *authz.CreateRoleBadRequest
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "builtin role")
	})

	t.Run("fail to delete builtin role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.DeleteRole(
			authz.NewDeleteRoleParams().WithID(adminRole),
			clientAuth,
		)
		require.NotNil(t, err)
		var parsed *authz.DeleteRoleBadRequest
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "builtin role")
	})

	t.Run("add builtin role permission", func(t *testing.T) {
		_, err = helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(adminRole).WithBody(authz.AddPermissionsBody{
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			}),
			clientAuth,
		)
		require.NotNil(t, err)
		var parsed *authz.AddPermissionsBadRequest
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "builtin role")
	})

	t.Run("remove builtin role permission", func(t *testing.T) {
		_, err = helper.Client(t).Authz.RemovePermissions(
			authz.NewRemovePermissionsParams().WithID(adminRole).WithBody(authz.RemovePermissionsBody{
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			}),
			clientAuth,
		)
		require.NotNil(t, err)
		var parsed *authz.RemovePermissionsBadRequest
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "builtin role")
	})
}

func TestAuthzRolesJourney(t *testing.T) {
	var err error

	adminUser := "existing-user"
	adminKey := "existing-key"
	existingRole := "admin"

	testRoleName := "test-role"
	createCollectionsAction := authorization.CreateCollections
	deleteCollectionsAction := authorization.DeleteCollections
	all := "*"

	testRole1 := &models.Role{
		Name: &testRoleName,
		Permissions: []*models.Permission{{
			Action:      &createCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &all},
		}},
	}

	clientAuth := helper.CreateAuth(adminKey)

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, nil, nil)
	defer down()

	t.Run("get all roles before create", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles, len(roles))
	})

	t.Run("create role", func(t *testing.T) {
		helper.CreateRole(t, adminKey, testRole1)
	})

	t.Run("fail to create existing role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.CreateRole(authz.NewCreateRoleParams().WithBody(testRole1), clientAuth)
		require.NotNil(t, err)
		var parsed *authz.CreateRoleConflict
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "builtin role")
	})

	t.Run("get all roles after create", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles+1, len(roles))
	})

	t.Run("get role by name", func(t *testing.T) {
		role := helper.GetRoleByName(t, adminKey, testRoleName)
		require.NotNil(t, role)
		require.Equal(t, testRoleName, *role.Name)
		require.Equal(t, 1, len(role.Permissions))
		require.Equal(t, createCollectionsAction, *role.Permissions[0].Action)
	})

	t.Run("add permission to role", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(testRoleName).WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{{Action: &deleteCollectionsAction, Collections: &models.PermissionCollections{Collection: &all}}},
		}), clientAuth)
		require.Nil(t, err)
	})

	t.Run("get role by name after adding permission", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(testRoleName), clientAuth)
		require.Nil(t, err)
		require.Equal(t, testRoleName, *res.Payload.Name)
		require.Equal(t, 2, len(res.Payload.Permissions))
		require.Equal(t, createCollectionsAction, *res.Payload.Permissions[0].Action)
		require.Equal(t, deleteCollectionsAction, *res.Payload.Permissions[1].Action)
	})

	t.Run("removing all permissions from role allowed without role deletion", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RemovePermissions(authz.NewRemovePermissionsParams().WithID(testRoleName).WithBody(authz.RemovePermissionsBody{
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(createCollectionsAction).WithCollection(all).Permission(),
				helper.NewCollectionsPermission().WithAction(deleteCollectionsAction).WithCollection(all).Permission(),
			},
		}), clientAuth)
		require.Nil(t, err)
	})

	t.Run("get role by name after removing permission", func(t *testing.T) {
		role := helper.GetRoleByName(t, adminKey, testRoleName)
		require.NotNil(t, role)
		require.Equal(t, testRoleName, *role.Name)
		require.Equal(t, 0, len(role.Permissions))
	})

	t.Run("assign role to user", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, testRoleName, adminUser)
	})

	t.Run("get roles for user after assignment", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(adminUser), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 2, len(res.Payload))

		names := make([]string, 2)
		for i, role := range res.Payload {
			names[i] = *role.Name
		}
		sort.Strings(names)

		roles := []string{existingRole, testRoleName}
		sort.Strings(roles)

		require.Equal(t, roles, names)
	})

	t.Run("get users for role after assignment", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetUsersForRole(authz.NewGetUsersForRoleParams().WithID(testRoleName), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
		require.Equal(t, adminUser, res.Payload[0])
	})

	t.Run("delete role by name", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, testRoleName)
	})

	t.Run("get roles for user after deletion", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(adminUser), clientAuth)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
		require.Equal(t, existingRole, *res.Payload[0].Name)
	})

	t.Run("get all roles after delete", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles, len(roles))
	})

	t.Run("get non-existent role by name", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(testRoleName), clientAuth)
		require.NotNil(t, err)
		require.ErrorIs(t, err, authz.NewGetRoleNotFound())
	})

	t.Run("error with add permissions on non-existent role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID("upsert-role").WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{{Action: &createCollectionsAction, Collections: &models.PermissionCollections{Collection: &all}}},
		}), clientAuth)
		require.NotNil(t, err)
		require.ErrorIs(t, err, authz.NewAddPermissionsNotFound())
	})

	t.Run("error with remove permissions on non-existent role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.RemovePermissions(authz.NewRemovePermissionsParams().WithID("upsert-role").WithBody(authz.RemovePermissionsBody{
			Permissions: []*models.Permission{{Action: &createCollectionsAction, Collections: &models.PermissionCollections{Collection: &all}}},
		}), clientAuth)
		require.NotNil(t, err)
		require.ErrorIs(t, err, authz.NewRemovePermissionsNotFound())
	})
}

func TestAuthzRolesRemoveAlsoAssignments(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	testRoleName := "test-role"
	testUser := "test-user"
	testKey := "test-key"

	testRole := &models.Role{
		Name: &testRoleName,
		Permissions: []*models.Permission{{
			Action: &authorization.CreateCollections,
			Collections: &models.PermissionCollections{
				Collection: authorization.All,
			},
		}},
	}

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{testUser: testKey}, nil)
	defer down()

	t.Run("get all roles before create", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles, len(roles))
	})

	t.Run("create role", func(t *testing.T) {
		helper.CreateRole(t, adminKey, testRole)
	})

	t.Run("assign role to user", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, testRoleName, testUser)
	})

	t.Run("get role assigned to user", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, testUser, adminKey)
		require.Equal(t, 1, len(roles))
	})

	t.Run("delete role", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, *testRole.Name)
	})

	t.Run("create the role again", func(t *testing.T) {
		helper.CreateRole(t, adminKey, testRole)
	})

	t.Run("get role assigned to user expected none", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, testUser, adminKey)
		require.Equal(t, 0, len(roles))
	})
}

func TestAuthzRolesMultiNodeJourney(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	testRole := "test-role"
	createCollectionsAction := authorization.CreateCollections
	deleteCollectionsAction := authorization.DeleteCollections
	all := "*"

	clientAuth := helper.CreateAuth(adminKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).WithApiKey().WithUserApiKey(adminUser, adminKey).WithRBAC().WithRbacAdmins(adminUser).Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("add role while 1 node is down", func(t *testing.T) {
		t.Run("get all roles before create", func(t *testing.T) {
			roles := helper.GetRoles(t, adminKey)
			require.Equal(t, NumBuildInRoles, len(roles))
		})

		t.Run("StopNode-3", func(t *testing.T) {
			require.Nil(t, compose.StopAt(ctx, 2, nil))
		})

		t.Run("create role", func(t *testing.T) {
			helper.CreateRole(t, adminKey, &models.Role{
				Name: &testRole,
				Permissions: []*models.Permission{{
					Action:      &createCollectionsAction,
					Collections: &models.PermissionCollections{Collection: &all},
				}},
			})
		})

		t.Run("StartNode-3", func(t *testing.T) {
			require.Nil(t, compose.StartAt(ctx, 2))
		})

		helper.SetupClient(compose.GetWeaviateNode3().URI())

		t.Run("get all roles after create", func(t *testing.T) {
			roles := helper.GetRoles(t, adminKey)
			require.Equal(t, NumBuildInRoles+1, len(roles))
		})

		t.Run("get role by name", func(t *testing.T) {
			role := helper.GetRoleByName(t, adminKey, testRole)
			require.NotNil(t, role)
			require.Equal(t, testRole, *role.Name)
			require.Equal(t, 1, len(role.Permissions))
			require.Equal(t, createCollectionsAction, *role.Permissions[0].Action)
		})

		t.Run("add permission to role Node3", func(t *testing.T) {
			_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(testRole).WithBody(authz.AddPermissionsBody{
				Permissions: []*models.Permission{{Action: &deleteCollectionsAction, Collections: &models.PermissionCollections{Collection: &all}}},
			}), clientAuth)
			require.Nil(t, err)
		})

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("get role by name after adding permission Node1", func(t *testing.T) {
			role := helper.GetRoleByName(t, adminKey, testRole)
			require.NotNil(t, role)
			require.Equal(t, testRole, *role.Name)
			require.Equal(t, 2, len(role.Permissions))
			require.Equal(t, createCollectionsAction, *role.Permissions[0].Action)
			require.Equal(t, deleteCollectionsAction, *role.Permissions[1].Action)
		})
	})
}

func TestAuthzRolesHasPermission(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	testRole := "test-role"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	t.Run("create role", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{
			Name: &testRole,
			Permissions: []*models.Permission{{
				Action: &authorization.CreateCollections,
				Collections: &models.PermissionCollections{
					Collection: authorization.All,
				},
			}},
		})
	})

	t.Run("true", func(t *testing.T) {
		res, err := helper.Client(t).Authz.HasPermission(authz.NewHasPermissionParams().WithID(testRole).WithBody(&models.Permission{
			Action: &authorization.CreateCollections,
			Collections: &models.PermissionCollections{
				Collection: authorization.All,
			},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.True(t, res.Payload)
	})

	t.Run("false", func(t *testing.T) {
		res, err := helper.Client(t).Authz.HasPermission(authz.NewHasPermissionParams().WithID(testRole).WithBody(&models.Permission{
			Action: &authorization.DeleteCollections,
			Collections: &models.PermissionCollections{
				Collection: authorization.All,
			},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.False(t, res.Payload)
	})

	t.Run("forbidden", func(t *testing.T) {
		_, err := helper.Client(t).Authz.HasPermission(authz.NewHasPermissionParams().WithID(testRole).WithBody(&models.Permission{
			Action: &authorization.CreateCollections,
			Collections: &models.PermissionCollections{
				Collection: authorization.All,
			},
		}), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		var parsed *authz.HasPermissionForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})
}

func TestAuthzRolesHasPermissionMultipleNodes(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	testRole := "test-role"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviateCluster(3).WithApiKey().WithUserApiKey(adminUser, adminKey).WithRBAC().WithRbacAdmins(adminUser).Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("StopNode-3", func(t *testing.T) {
		require.Nil(t, compose.StopAt(ctx, 2, nil))
	})

	t.Run("create role", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{
			Name: &testRole,
			Permissions: []*models.Permission{{
				Action: &authorization.CreateCollections,
				Collections: &models.PermissionCollections{
					Collection: authorization.All,
				},
			}},
		})
	})

	t.Run("permission in node 1", func(t *testing.T) {
		res, err := helper.Client(t).Authz.HasPermission(authz.NewHasPermissionParams().WithID(testRole).WithBody(&models.Permission{
			Action: &authorization.CreateCollections,
			Collections: &models.PermissionCollections{
				Collection: authorization.All,
			},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.True(t, res.Payload)
	})

	t.Run("permission in 2 without waiting", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviateNode2().URI())
		res, err := helper.Client(t).Authz.HasPermission(authz.NewHasPermissionParams().WithID(testRole).WithBody(&models.Permission{
			Action: &authorization.CreateCollections,
			Collections: &models.PermissionCollections{
				Collection: authorization.All,
			},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.True(t, res.Payload)
	})

	t.Run("StartNode-3", func(t *testing.T) {
		require.Nil(t, compose.StartAt(ctx, 2))
	})

	t.Run("permission in 3 without waiting", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviateNode3().URI())
		res, err := helper.Client(t).Authz.HasPermission(authz.NewHasPermissionParams().WithID(testRole).WithBody(&models.Permission{
			Action: &authorization.CreateCollections,
			Collections: &models.PermissionCollections{
				Collection: authorization.All,
			},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
		require.True(t, res.Payload)
	})
}

func TestAuthzEmptyRole(t *testing.T) {
	var err error

	adminUser := "admin-user"
	adminKey := "admin-key"
	customEmptyRole := "customEmpty"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, nil, nil)
	defer down()

	t.Run("create empty role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name:        &customEmptyRole,
				Permissions: []*models.Permission{},
			}),
			helper.CreateAuth(adminKey),
		)
		require.Nil(t, err)
	})

	t.Run("get all roles, shall be 4 for the newly created empty role", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles+1, len(roles))
	})
}

func TestAuthzRoleRemoveToEmptyAndAddPermission(t *testing.T) {
	var err error

	adminUser := "admin-user"
	adminKey := "admin-key"
	customRole := "customRole"

	clientAuth := helper.CreateAuth(adminKey)

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, nil, nil)
	defer down()

	t.Run("create role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name: &customRole,
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			}),
			clientAuth,
		)
		require.Nil(t, err)
	})

	t.Run("remove permissions", func(t *testing.T) {
		_, err = helper.Client(t).Authz.RemovePermissions(
			authz.NewRemovePermissionsParams().WithID(customRole).WithBody(authz.RemovePermissionsBody{
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			}),
			clientAuth,
		)
		require.Nil(t, err)
	})

	t.Run("get all roles, shall be 3 for the newly created empty role", func(t *testing.T) {
		roles := helper.GetRoles(t, adminKey)
		require.Equal(t, NumBuildInRoles+1, len(roles))
	})

	t.Run("get role after deleting permission", func(t *testing.T) {
		role := helper.GetRoleByName(t, adminKey, customRole)
		require.Equal(t, customRole, *role.Name)
		require.Equal(t, 0, len(role.Permissions))
	})

	t.Run("add permissions", func(t *testing.T) {
		_, err = helper.Client(t).Authz.AddPermissions(
			authz.NewAddPermissionsParams().WithID(customRole).WithBody(authz.AddPermissionsBody{
				Permissions: []*models.Permission{{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				}},
			}),
			clientAuth,
		)
		require.Nil(t, err)
	})

	t.Run("get role after adding permission", func(t *testing.T) {
		role := helper.GetRoleByName(t, adminKey, customRole)
		require.Equal(t, customRole, *role.Name)
		require.Equal(t, 1, len(role.Permissions))
		require.Equal(t, authorization.CreateCollections, *role.Permissions[0].Action)
	})
}
