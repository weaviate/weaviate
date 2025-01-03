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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZTenants(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)

	customUser := "custom-user"
	customKey := "custom-key"

	readSchemaAction := authorization.ReadCollections
	readTenantAction := authorization.ReadTenants
	createTenantAction := authorization.CreateTenants
	deleteTenantAction := authorization.DeleteTenants
	updateTenantAction := authorization.UpdateTenants

	ctx := context.Background()
	compose, err := docker.New().WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		WithRBAC().WithRbacAdmins(adminUser).
		WithBackendFilesystem().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	className := "AuthzTenantTestClass"
	deleteObjectClass(t, className, adminAuth)
	c := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	helper.CreateClassAuth(t, c, adminKey)
	defer deleteObjectClass(t, className, adminAuth)

	// user needs to be able to create objects and read the configs
	all := "*"

	createTenantRoleName := "readSchemaAndCreateTenant"
	createTenantRole := &models.Role{
		Name: &createTenantRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &createTenantAction, Tenants: &models.PermissionTenants{Collection: &all}},
		},
	}

	readTenantRoleName := "readSchemaAndReadTenant"
	readTenantRole := &models.Role{
		Name: &readTenantRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &readTenantAction, Tenants: &models.PermissionTenants{Collection: &all}},
		},
	}

	deleteTenantRoleName := "readSchemaAndDeleteTenant"
	deleteTenantRole := &models.Role{
		Name: &deleteTenantRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &deleteTenantAction, Tenants: &models.PermissionTenants{Collection: &all}},
		},
	}

	updateTenantRoleName := "readSchemaAndUpdateTenant"
	updateTenantRole := &models.Role{
		Name: &updateTenantRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &updateTenantAction, Tenants: &models.PermissionTenants{Collection: &all}},
		},
	}

	helper.DeleteRole(t, adminKey, *createTenantRole.Name)
	helper.DeleteRole(t, adminKey, *readTenantRole.Name)
	helper.DeleteRole(t, adminKey, *deleteTenantRole.Name)
	helper.DeleteRole(t, adminKey, *updateTenantRole.Name)
	helper.CreateRole(t, adminKey, createTenantRole)
	helper.CreateRole(t, adminKey, readTenantRole)
	helper.CreateRole(t, adminKey, deleteTenantRole)
	helper.CreateRole(t, adminKey, updateTenantRole)
	defer helper.DeleteRole(t, adminKey, *createTenantRole.Name)
	defer helper.DeleteRole(t, adminKey, *readTenantRole.Name)
	defer helper.DeleteRole(t, adminKey, *deleteTenantRole.Name)
	defer helper.DeleteRole(t, adminKey, *updateTenantRole.Name)

	tenants := []*models.Tenant{{Name: "Tenant1"}}

	t.Run("Create tenant", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, *createTenantRole.Name, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, *createTenantRole.Name, customUser)
		require.NoError(t, createTenant(t, className, tenants, customKey))
		require.NoError(t, deleteTenant(t, className, []string{tenants[0].Name}, adminKey))
	})

	t.Run("Tenant read and exist", func(t *testing.T) {
		require.NoError(t, createTenant(t, className, tenants, adminKey))
		defer deleteTenant(t, className, []string{tenants[0].Name}, adminKey)

		helper.AssignRoleToUser(t, adminKey, *readTenantRole.Name, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, *readTenantRole.Name, customUser)

		require.NoError(t, readTenant(t, className, tenants[0].Name, customKey))
		require.NoError(t, readTenants(t, className, customKey))
		require.NoError(t, existsTenant(t, className, tenants[0].Name, customKey))
	})

	t.Run("delete tenant", func(t *testing.T) {
		require.NoError(t, createTenant(t, className, tenants, adminKey))

		helper.AssignRoleToUser(t, adminKey, *deleteTenantRole.Name, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, *deleteTenantRole.Name, customUser)

		require.NoError(t, deleteTenant(t, className, []string{tenants[0].Name}, customKey))
	})

	t.Run("update tenant status", func(t *testing.T) {
		require.NoError(t, createTenant(t, className, tenants, adminKey))
		defer deleteTenant(t, className, []string{tenants[0].Name}, adminKey)

		helper.AssignRoleToUser(t, adminKey, *updateTenantRole.Name, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, *updateTenantRole.Name, customUser)

		require.NoError(t, updateTenantStatus(t, className, []*models.Tenant{{Name: "Tenant1", ActivityStatus: "INACTIVE"}}, customKey))
	})
}
