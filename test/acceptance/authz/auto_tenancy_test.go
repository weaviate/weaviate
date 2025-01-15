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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthzAutoTenantActivation(t *testing.T) {
	existingUser := "admin-user"
	existingKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	testRoleName := "test-role"

	adminAuth := helper.CreateAuth(existingKey)

	_, teardown := composeUp(t, map[string]string{existingUser: existingKey}, map[string]string{customUser: customKey}, nil)

	cls := articles.ParagraphsClass()
	tenant := "tenant"
	obj := articles.NewParagraph().WithID("00000000-0000-0000-0000-000000000001").WithTenant(tenant).Object()
	obj2 := articles.NewParagraph().WithID("00000000-0000-0000-0000-000000000002").WithTenant(tenant).Object()

	defer func() {
		helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
		helper.DeleteRole(t, existingKey, testRoleName)
		teardown()
	}()

	deactivateTenant := func(t *testing.T) {
		helper.UpdateTenantsWithAuthz(t, cls.Class, []*models.Tenant{{Name: obj.Tenant, ActivityStatus: models.TenantActivityStatusCOLD}}, adminAuth)
	}

	t.Run("setup", func(*testing.T) {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   false,
		}
		helper.CreateClassAuth(t, cls, existingKey)
		helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: obj.Tenant, ActivityStatus: models.TenantActivityStatusHOT}}, existingKey)
		helper.CreateObjectAuth(t, obj2, existingKey)
		deactivateTenant(t)
	})

	t.Run("create and assign role that can CRUD objects in and read schema of tenant of collection", func(t *testing.T) {
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(testRoleName),
			Permissions: []*models.Permission{
				helper.NewDataPermission().WithAction(authorization.CreateData).WithCollection(cls.Class).Permission(),
				helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(cls.Class).Permission(),
				helper.NewDataPermission().WithAction(authorization.UpdateData).WithCollection(cls.Class).Permission(),
				helper.NewDataPermission().WithAction(authorization.DeleteData).WithCollection(cls.Class).Permission(),
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission(), // all needed for gql search
			},
		})
		_, err := helper.Client(t).Authz.AssignRole(authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("fail with 403 when trying to create an object in an inactive tenant due to lacking authorization.UpdateTenants for autoTenantActivation", func(t *testing.T) {
		_, err := createObject(t, obj, customKey)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsCreateForbidden)
		if !forbidden {
			helper.AssertRequestOk(t, nil, err, nil)
		}
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail with 403 when trying to read an object in an inactive tenant due to lacking authorization.UpdateTenants for autoTenantActivation", func(t *testing.T) {
		_, err := getObject(t, obj2.Class, obj2.ID, &obj2.Tenant, customKey)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsClassGetForbidden)
		if !forbidden {
			helper.AssertRequestOk(t, nil, err, nil)
		}
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail with 403 when trying to update an object in an inactive tenant due to lacking authorization.UpdateTenants for autoTenantActivation", func(t *testing.T) {
		_, err := updateObject(t, obj, customKey)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsClassPatchForbidden)
		if !forbidden {
			helper.AssertRequestOk(t, nil, err, nil)
		}
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail with 403 when trying to delete an object in an inactive tenant due to lacking authorization.UpdateTenants for autoTenantActivation", func(t *testing.T) {
		_, err := deleteObject(t, obj.Class, obj.ID, &obj.Tenant, customKey)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsClassDeleteForbidden)
		if !forbidden {
			helper.AssertRequestOk(t, nil, err, nil)
		}
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail with gql when trying to search (Get) an inactive tenant due to lacking authorization.UpdateTenants for autoTenantActivation", func(t *testing.T) {
		res, err := queryGQL(t, fmt.Sprintf(`{Get{%s(tenant:%q){_additional{id}}}}`, cls.Class, obj.Tenant), customKey)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.GetPayload().Errors))
		require.Contains(t, res.GetPayload().Errors[0].Message, "forbidden")
	})

	t.Run("fail with gql when trying to search (Aggregate) an inactive tenant due to lacking authorization.UpdateTenants for autoTenantActivation", func(t *testing.T) {
		res, err := queryGQL(t, fmt.Sprintf(`{Aggregate{%s(tenant:%q){meta{count}}}}`, cls.Class, obj.Tenant), customKey)
		require.Nil(t, err)
		require.Equal(t, 1, len(res.GetPayload().Errors))
		require.Contains(t, res.GetPayload().Errors[0].Message, "forbidden")
	})

	t.Run("fail with grpc when trying to search an inactivate tenant due to lacking authorization.UpdateTenants for autoTenantActivation", func(t *testing.T) {
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		_, err := helper.ClientGRPC(t).Search(ctx, &protocol.SearchRequest{
			Collection: cls.Class,
			Tenant:     tenant,
		})
		require.NotNil(t, err)
		require.Equal(t, status.Code(err), codes.PermissionDenied)
	})

	t.Run("add permission allowing to update schema of collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(testRoleName).WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{
				helper.NewTenantsPermission().WithAction(authorization.UpdateTenants).WithCollection(cls.Class).WithTenant(obj.Tenant).Permission(),
			},
		}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("successfully create object in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		defer deactivateTenant(t)
		err := helper.CreateObjectAuth(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully get object in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		defer deactivateTenant(t)
		_, err := getObject(t, obj.Class, obj.ID, &obj.Tenant, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully update object in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		defer deactivateTenant(t)
		obj.Properties = map[string]string{"contents": "updated"}
		_, err := updateObject(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully search (Get) with gql in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		defer deactivateTenant(t)
		res, err := queryGQL(t, fmt.Sprintf(`{Get{%s(tenant:%q){_additional{id}}}}`, cls.Class, obj.Tenant), customKey)
		require.Nil(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.GetPayload().Data)
		require.Empty(t, res.GetPayload().Errors)
	})

	t.Run("successfully search (Aggregate) with gql in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		defer deactivateTenant(t)
		res, err := queryGQL(t, fmt.Sprintf(`{Aggregate{%s(tenant:%q){meta{count}}}}`, cls.Class, obj.Tenant), customKey)
		require.Nil(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.GetPayload().Data)
		require.Empty(t, res.GetPayload().Errors)
	})

	t.Run("successfully search with grpc in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		defer deactivateTenant(t)
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		resp, err := helper.ClientGRPC(t).Search(ctx, &protocol.SearchRequest{
			Collection: cls.Class,
			Tenant:     tenant,
		})
		require.Nil(t, err)
		require.NotNil(t, resp)
	})

	t.Run("successfully delete object in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		defer deactivateTenant(t)
		_, err := deleteObject(t, obj.Class, obj.ID, &obj.Tenant, customKey)
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

	t.Run("add permission allowing to create tenants of collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(testRoleName).WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{
				helper.NewTenantsPermission().WithAction(authorization.CreateCollections).WithCollection(cls.Class).WithTenant(obj.Tenant).Permission(),
			},
		}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("successfully create object in tenant after adding permission for autoTenantCreation", func(t *testing.T) {
		err := helper.CreateObjectAuth(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})
}
