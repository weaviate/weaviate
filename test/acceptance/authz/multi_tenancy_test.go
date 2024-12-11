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

package test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzAutoTenantActivation(t *testing.T) {
	// existingUser := "admin-user"
	existingKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	testRoleName := "test-role"

	adminAuth := helper.CreateAuth(existingKey)

	// _, teardown := composeUp(t, map[string]string{existingUser: existingKey}, map[string]string{customUser: customKey}, nil)

	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	tenant := "tenant"
	obj := articles.NewParagraph().WithID("00000000-0000-0000-0000-000000000001").WithTenant(tenant).Object()

	defer func() {
		helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
		helper.DeleteRole(t, existingKey, testRoleName)
		// teardown()
	}()

	t.Run("setup", func(*testing.T) {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   false,
		}
		helper.CreateClassAuth(t, cls, existingKey)
		helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: tenant, ActivityStatus: models.TenantActivityStatusCOLD}}, existingKey)
	})

	t.Run("create and assign role that can create objects in and read schema of tenant of collection", func(t *testing.T) {
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(testRoleName),
			Permissions: []*models.Permission{
				helper.NewDataPermission().WithAction(authorization.CreateData).WithCollection(cls.Class).Permission(),
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(cls.Class).Permission(),
			},
		})
		_, err := helper.Client(t).Authz.AssignRole(authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("fail with 403 when trying to create an object in an inactive tenant due to lacking authorization.UpdateCollections for autoTenantActivation", func(t *testing.T) {
		err := helper.CreateObjectAuth(t, obj, customKey)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsCreateUnprocessableEntity)
		require.True(t, forbidden)
		t.Log(parsed.Payload.Error[0].Message)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add permission allowing to update schema of collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(testRoleName).WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(cls.Class).Permission(),
			},
		}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("successfully create object in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		err := helper.CreateObjectAuth(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})
}

func TestAuthzAutoTenantCreation(t *testing.T) {
	existingUser := "existing-user"
	existingKey := "existing-key"

	customUser := "custom-user"
	customKey := "custom-key"

	testRoleName := "test-role"

	adminAuth := helper.CreateAuth(existingKey)

	_, teardown := composeUp(t, map[string]string{existingUser: existingKey}, map[string]string{customUser: customKey}, nil)

	cls := articles.ParagraphsClass()
	tenant := "tenant"
	obj := articles.NewParagraph().WithID("00000000-0000-0000-0000-000000000001").WithTenant(tenant).Object()

	defer func() {
		helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
		helper.DeleteRole(t, existingKey, testRoleName)
		teardown()
	}()

	t.Run("setup", func(*testing.T) {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: false,
			AutoTenantCreation:   true,
		}
		helper.CreateClassAuth(t, cls, existingKey)
	})

	t.Run("create and assign role that can create objects in and read schema of tenant of collection", func(t *testing.T) {
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(testRoleName),
			Permissions: []*models.Permission{
				helper.NewDataPermission().WithAction(authorization.CreateData).WithCollection(cls.Class).Permission(),
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(cls.Class).Permission(),
			},
		})
		_, err := helper.Client(t).Authz.AssignRole(authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("fail with 403 when trying to create an object in a non-existent tenant due to lacking authorization.CreateCollections for autoTenantCreation", func(t *testing.T) {
		err := helper.CreateObjectAuth(t, obj, customKey)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsCreateForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add permission allowing to create schema of collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(testRoleName).WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.CreateCollections).WithCollection(cls.Class).Permission(),
			},
		}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("successfully create object in tenant after adding permission for autoTenantCreation", func(t *testing.T) {
		err := helper.CreateObjectAuth(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})
}
