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
	"fmt"
	"testing"
	"time"

	grpchelper "github.com/weaviate/weaviate/test/helper/grpc"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzAutoTenantActivationRBAC(t *testing.T) {
	existingUser := "admin-user"
	existingKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	testRoleName := "test-role"

	adminAuth := helper.CreateAuth(existingKey)

	_, teardown := composeUp(t, map[string]string{existingUser: existingKey}, map[string]string{customUser: customKey}, nil)

	cls := articles.ParagraphsClass()
	tenant := "tenant"
	obj := articles.NewParagraph().WithID(UUID1).WithTenant(tenant).Object()
	obj2 := articles.NewParagraph().WithID(UUID2).WithTenant(tenant).Object()

	defer func() {
		helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
		helper.DeleteRole(t, existingKey, testRoleName)
		teardown()
	}()

	deactivateTenant := func(t *testing.T) {
		helper.UpdateTenantsWithAuthz(t, cls.Class, []*models.Tenant{{Name: obj.Tenant, ActivityStatus: models.TenantActivityStatusCOLD}}, adminAuth)
	}

	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   false,
	}
	helper.CreateClassAuth(t, cls, existingKey)
	helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: obj.Tenant, ActivityStatus: models.TenantActivityStatusHOT}}, existingKey)
	helper.CreateObjectAuth(t, obj2, existingKey)
	deactivateTenant(t)

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
		helper.AssignRoleToUser(t, existingKey, testRoleName, customUser)
	})

	t.Run("successfully create object in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		err := helper.CreateObjectAuth(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully get object in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		_, err := getObject(t, obj.Class, obj.ID, &obj.Tenant, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully update object in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		obj.Properties = map[string]string{"contents": "updated"}
		_, err := updateObject(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully search (Get) with gql in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		res, err := queryGQL(t, fmt.Sprintf(`{Get{%s(tenant:%q){_additional{id}}}}`, cls.Class, obj.Tenant), customKey)
		require.Nil(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.GetPayload().Data)
		require.Empty(t, res.GetPayload().Errors)
	})

	t.Run("successfully search (Aggregate) with gql in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		res, err := queryGQL(t, fmt.Sprintf(`{Aggregate{%s(tenant:%q){meta{count}}}}`, cls.Class, obj.Tenant), customKey)
		require.Nil(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.GetPayload().Data)
		require.Empty(t, res.GetPayload().Errors)
	})

	t.Run("successfully search (Get) with grpc in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		resp, err := helper.ClientGRPC(t).Search(ctx, &protocol.SearchRequest{
			Collection: cls.Class,
			Tenant:     tenant,
		})
		require.Nil(t, err)
		require.NotNil(t, resp)
	})

	t.Run("successfully search (Aggregate) with grpc in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey))
		resp, err := helper.ClientGRPC(t).Aggregate(ctx, &protocol.AggregateRequest{
			Collection:   cls.Class,
			Tenant:       tenant,
			ObjectsCount: true,
		})
		require.Nil(t, err)
		require.NotNil(t, resp)
	})

	t.Run("successfully delete object in tenant", func(t *testing.T) {
		defer deactivateTenant(t)
		_, err := deleteObject(t, obj.Class, obj.ID, &obj.Tenant, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})
}

func TestAuthzAutoTenantActivationAdminList(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"
	readonlyKey := "viewer-key"
	readonlyUser := "viewer-user"
	adminAuth := helper.CreateAuth(adminKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	builder := docker.New().WithWeaviateWithGRPC().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(readonlyUser, readonlyKey).
		WithAdminListAdmins(adminUser).WithAdminListUsers(readonlyUser).WithApiKey()
	compose, err := builder.Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	teardown := func() {
		helper.ResetClient()
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
		cancel()
	}

	cls := articles.ParagraphsClass()
	tenant := "tenant"
	obj1 := articles.NewParagraph().WithID(UUID1).WithTenant(tenant).Object()
	obj2 := articles.NewParagraph().WithID(UUID2).WithTenant(tenant).Object()
	obj3 := articles.NewParagraph().WithID(UUID3).WithTenant("tenant2").Object()
	obj4 := articles.NewParagraph().WithID(UUID4).WithTenant("tenant3").Object()

	defer func() {
		helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
		teardown()
	}()

	deactivateTenants := func(t *testing.T) {
		for _, obj := range []*models.Object{obj1, obj2, obj3, obj4} {
			helper.UpdateTenantsWithAuthz(t, cls.Class, []*models.Tenant{{Name: obj.Tenant, ActivityStatus: models.TenantActivityStatusCOLD}}, adminAuth)
		}
	}

	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   false,
	}
	helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
	helper.CreateClassAuth(t, cls, adminKey)
	helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: obj1.Tenant, ActivityStatus: models.TenantActivityStatusHOT}}, adminKey)
	helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: obj3.Tenant, ActivityStatus: models.TenantActivityStatusHOT}}, adminKey)
	helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: obj4.Tenant, ActivityStatus: models.TenantActivityStatusHOT}}, adminKey)
	helper.CreateObjectAuth(t, obj2, adminKey)
	deactivateTenants(t)

	t.Run("successfully create object in tenant as admin", func(t *testing.T) {
		defer deactivateTenants(t)
		err := helper.CreateObjectAuth(t, obj1, adminKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully update object in tenant as admin", func(t *testing.T) {
		defer deactivateTenants(t)
		obj1.Properties = map[string]string{"contents": "updated"}
		_, err := updateObject(t, obj1, adminKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("successfully batch-create object in tenant as admin", func(t *testing.T) {
		defer deactivateTenants(t)
		helper.CreateObjectsBatchAuth(t, []*models.Object{obj3}, adminKey)
	})

	t.Run("successfully GRPC batch-create object in tenant as admin", func(t *testing.T) {
		defer deactivateTenants(t)
		batchReply, err := grpchelper.BatchGRPCWithTenantAuth(t, []*models.Object{obj4}, adminKey)
		require.NoError(t, err)
		require.Len(t, batchReply.Errors, 0)
	})

	t.Run("fail to update object in tenant as read-only", func(t *testing.T) {
		defer deactivateTenants(t)
		obj1.Properties = map[string]string{"contents": "updated"}
		_, err := updateObject(t, obj1, readonlyKey)
		helper.AssertRequestFail(t, nil, err, nil)
	})

	t.Run("fail to delete object in tenant as read-only", func(t *testing.T) {
		defer deactivateTenants(t)
		_, err := deleteObject(t, obj1.Class, obj1.ID, &obj1.Tenant, readonlyKey)
		helper.AssertRequestFail(t, nil, err, nil)
	})

	tests := []struct {
		name string
		key  string
	}{
		{"admin", adminKey},
		{"read-only", readonlyKey},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("successfully get object in tenant as %s", tt.name), func(t *testing.T) {
			defer deactivateTenants(t)
			_, err := getObject(t, obj1.Class, obj1.ID, &obj1.Tenant, tt.key)
			helper.AssertRequestOk(t, nil, err, nil)
		})

		t.Run(fmt.Sprintf("successfully search (Get) with gql in tenant as %s", tt.name), func(t *testing.T) {
			defer deactivateTenants(t)
			res, err := queryGQL(t, fmt.Sprintf(`{Get{%s(tenant:%q){_additional{id}}}}`, cls.Class, obj1.Tenant), tt.key)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.NotEmpty(t, res.GetPayload().Data)
			require.Empty(t, res.GetPayload().Errors)
		})

		t.Run(fmt.Sprintf("successfully search (Aggregate) with gql in tenant as %s", tt.name), func(t *testing.T) {
			defer deactivateTenants(t)
			res, err := queryGQL(t, fmt.Sprintf(`{Aggregate{%s(tenant:%q){meta{count}}}}`, cls.Class, obj1.Tenant), tt.key)
			require.Nil(t, err)
			require.NotNil(t, res)
			require.NotEmpty(t, res.GetPayload().Data)
			require.Empty(t, res.GetPayload().Errors)
		})

		t.Run(fmt.Sprintf("successfully search (Get) with grpc in tenant as %s", tt.name), func(t *testing.T) {
			defer deactivateTenants(t)
			ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", tt.key))
			resp, err := helper.ClientGRPC(t).Search(ctx, &protocol.SearchRequest{
				Collection: cls.Class,
				Tenant:     tenant,
			})
			require.Nil(t, err)
			require.NotNil(t, resp)
		})

		t.Run(fmt.Sprintf("successfully search (Aggregate) with grpc in tenant as %s", tt.name), func(t *testing.T) {
			defer deactivateTenants(t)
			ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", tt.key))
			resp, err := helper.ClientGRPC(t).Aggregate(ctx, &protocol.AggregateRequest{
				Collection:   cls.Class,
				Tenant:       tenant,
				ObjectsCount: true,
			})
			require.Nil(t, err)
			require.NotNil(t, resp)
		})
	}

	t.Run("successfully delete object in tenant as admin", func(t *testing.T) {
		defer deactivateTenants(t)
		_, err := deleteObject(t, obj1.Class, obj1.ID, &obj1.Tenant, adminKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})

	t.Run("fail to create object in tenant as read-only", func(t *testing.T) {
		defer deactivateTenants(t)
		err := helper.CreateObjectAuth(t, obj1, readonlyKey)
		helper.AssertRequestFail(t, nil, err, nil)
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
		helper.AssignRoleToUser(t, existingKey, testRoleName, customUser)
	})

	t.Run("fail with 403 when trying to create an object in a non-existent tenant due to lacking authorization.CreateCollections for autoTenantCreation", func(t *testing.T) {
		err := helper.CreateObjectAuth(t, obj, customKey)
		require.NotNil(t, err)
		var parsed *objects.ObjectsCreateForbidden
		require.True(t, errors.As(err, &parsed))
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add permission allowing to create tenants of collection", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithID(testRoleName).WithBody(authz.AddPermissionsBody{
			Permissions: []*models.Permission{
				helper.NewTenantsPermission().WithAction(authorization.CreateTenants).WithCollection(cls.Class).WithTenant(obj.Tenant).Permission(),
			},
		}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("successfully create object in tenant after adding permission for autoTenantCreation", func(t *testing.T) {
		err := helper.CreateObjectAuth(t, obj, customKey)
		helper.AssertRequestOk(t, nil, err, nil)
	})
}
