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
	"testing"

	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
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
	compose, err := docker.New().WithWeaviateWithGRPC().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		WithRBAC().WithRbacRoots(adminUser).
		WithBackendFilesystem().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())
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
	tenant1 := "Tenant1"
	tenant2 := "Tenant2"
	tenants := []*models.Tenant{{Name: tenant1}, {Name: tenant2}}
	tenantNames := []string{tenant1, tenant2}

	createAllTenantsRoleName := "readSchemaAndCreateAllTenants"
	createAllTenantsRole := &models.Role{
		Name: &createAllTenantsRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &createTenantAction, Tenants: &models.PermissionTenants{Collection: &all}},
		},
	}

	createSpecificTenantsRoleName := "readSchemaAndCreateSpecificTenant"
	createSpecificTenantsRole := &models.Role{
		Name: &createSpecificTenantsRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &createTenantAction, Tenants: &models.PermissionTenants{Collection: &all, Tenant: &tenants[0].Name}},
		},
	}

	readAllTenantsRoleName := "readSchemaAndReadAllTenants"
	readAllTenantsRole := &models.Role{
		Name: &readAllTenantsRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &readTenantAction, Tenants: &models.PermissionTenants{Collection: &all}},
		},
	}

	readSpecificTenantRoleName := "readSchemaAndReadSpecificTenant"
	readSpecificTenantRole := &models.Role{
		Name: &readSpecificTenantRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &readTenantAction, Tenants: &models.PermissionTenants{Collection: &all, Tenant: &tenants[0].Name}},
		},
	}

	deleteSpecificTenantRoleName := "readSchemaAndDeleteSpecificTenant"
	deleteSpecificTenantRole := &models.Role{
		Name: &deleteSpecificTenantRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &deleteTenantAction, Tenants: &models.PermissionTenants{Collection: &all, Tenant: &tenants[0].Name}},
		},
	}

	updateSpecificTenantRoleName := "readSchemaAndUpdateTenant"
	updateSpecificTenantRole := &models.Role{
		Name: &updateSpecificTenantRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &updateTenantAction, Tenants: &models.PermissionTenants{Collection: &all, Tenant: &tenants[0].Name}},
		},
	}

	roles := []*models.Role{
		createAllTenantsRole,
		createSpecificTenantsRole,
		readAllTenantsRole,
		readSpecificTenantRole,
		deleteSpecificTenantRole,
		updateSpecificTenantRole,
	}

	for _, role := range roles {
		helper.DeleteRole(t, adminKey, *role.Name)
		helper.CreateRole(t, adminKey, role)
	}
	defer func() {
		for _, role := range roles {
			helper.DeleteRole(t, adminKey, *role.Name)
		}
	}()

	t.Run("Create all tenants", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, *createAllTenantsRole.Name, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, *createAllTenantsRole.Name, customUser)
		require.NoError(t, createTenant(t, className, tenants, customKey))
		require.NoError(t, deleteTenant(t, className, tenantNames, adminKey))
	})

	t.Run("Create specific tenant with needed permission", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, *createSpecificTenantsRole.Name, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, *createSpecificTenantsRole.Name, customUser)
		require.NoError(t, createTenant(t, className, []*models.Tenant{{Name: tenant1}}, customKey))
		require.NoError(t, deleteTenant(t, className, []string{tenant1}, adminKey))
	})

	t.Run("Fail to create specific tenant without needed permission", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, *createSpecificTenantsRole.Name, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, *createSpecificTenantsRole.Name, customUser)
		err := createTenant(t, className, []*models.Tenant{{Name: tenant2}}, customKey)
		require.NotNil(t, err)
		var forbidden *clschema.TenantsCreateForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	setupRUDTests := func(role string) func() {
		require.NoError(t, createTenant(t, className, tenants, adminKey))
		helper.AssignRoleToUser(t, adminKey, role, customUser)
		return func() {
			defer deleteTenant(t, className, tenantNames, adminKey)
			defer helper.RevokeRoleFromUser(t, adminKey, role, customUser)
		}
	}

	t.Run("Tenant read and exist", func(t *testing.T) {
		cleanup := setupRUDTests(readAllTenantsRoleName)
		defer cleanup()

		require.NoError(t, readTenant(t, className, tenants[0].Name, customKey))
		require.NoError(t, existsTenant(t, className, tenants[0].Name, customKey))
	})

	t.Run("Read a specific tenant with needed permission", func(t *testing.T) {
		cleanup := setupRUDTests(readSpecificTenantRoleName)
		defer cleanup()

		require.NoError(t, readTenant(t, className, tenants[0].Name, customKey))
		require.NoError(t, existsTenant(t, className, tenants[0].Name, customKey))
	})

	t.Run("Fail to read a specific tenant and all tenants without needed permission", func(t *testing.T) {
		cleanup := setupRUDTests(readSpecificTenantRoleName)
		defer cleanup()

		err := readTenant(t, className, tenants[1].Name, customKey)
		require.NotNil(t, err)
		var forbiddenGetOne *clschema.TenantsGetOneForbidden
		require.True(t, errors.As(err, &forbiddenGetOne))

		err = existsTenant(t, className, tenants[1].Name, customKey)
		var forbiddenExists *clschema.TenantExistsForbidden
		require.True(t, errors.As(err, &forbiddenExists))

		// tests for tenant filtering
		res, err := readTenants(t, className, customKey)
		require.Nil(t, err)
		require.Len(t, res.Payload, 1)
		require.Equal(t, tenants[0].Name, res.Payload[0].Name)
	})

	t.Run("Get specific tenant using grpc", func(t *testing.T) {
		cleanup := setupRUDTests(readSpecificTenantRoleName)
		defer cleanup()

		_, err := readTenantGRPC(t, ctx, className, tenants[0].Name, customKey)
		require.Nil(t, err)
	})

	t.Run("Return no tenants via grpc when one is forbidden", func(t *testing.T) {
		cleanup := setupRUDTests(readSpecificTenantRoleName)
		defer cleanup()

		res, err := readTenantGRPC(t, ctx, className, tenants[1].Name, customKey)
		require.Nil(t, err)
		require.Len(t, res.Tenants, 0)
	})

	t.Run("Get filtered tenants using rest", func(t *testing.T) {
		cleanup := setupRUDTests(readSpecificTenantRoleName)
		defer cleanup()

		res, err := readTenants(t, className, customKey)
		require.Nil(t, err)
		require.Len(t, res.Payload, 1)
		require.Equal(t, tenants[0].Name, res.Payload[0].Name)
	})

	t.Run("Get filtered tenants using grpc", func(t *testing.T) {
		cleanup := setupRUDTests(readSpecificTenantRoleName)
		defer cleanup()

		res, err := readTenantsGRPC(t, ctx, className, customKey)
		require.Nil(t, err)
		require.Len(t, res.Tenants, 1)
		require.Equal(t, tenants[0].Name, res.Tenants[0].Name)
	})

	t.Run("Delete specific tenant with needed permission", func(t *testing.T) {
		cleanup := setupRUDTests(deleteSpecificTenantRoleName)
		defer cleanup()

		require.NoError(t, deleteTenant(t, className, []string{tenants[0].Name}, customKey))
	})

	t.Run("Fail to delete specific tenant without needed permission", func(t *testing.T) {
		cleanup := setupRUDTests(deleteSpecificTenantRoleName)
		defer cleanup()

		err := deleteTenant(t, className, []string{tenants[1].Name}, customKey)
		require.NotNil(t, err)
		var forbidden *clschema.TenantsDeleteForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	t.Run("Update specific tenant status with needed permission", func(t *testing.T) {
		cleanup := setupRUDTests(updateSpecificTenantRoleName)
		defer cleanup()

		require.NoError(t, updateTenantStatus(t, className, []*models.Tenant{{Name: tenants[0].Name, ActivityStatus: "INACTIVE"}}, customKey))
	})

	t.Run("Fail to update specific tenant status without needed permission", func(t *testing.T) {
		cleanup := setupRUDTests(updateSpecificTenantRoleName)
		defer cleanup()

		err := updateTenantStatus(t, className, []*models.Tenant{{Name: tenants[1].Name, ActivityStatus: "INACTIVE"}}, customKey)
		require.NotNil(t, err)
		var forbidden *clschema.TenantsUpdateForbidden
		require.True(t, errors.As(err, &forbidden))
	})
}
