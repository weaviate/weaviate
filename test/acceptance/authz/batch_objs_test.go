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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZBatchObjs(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)
	testRoleName := "test-role"

	updateDataAction := authorization.UpdateData
	createDataAction := authorization.CreateData

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	tests := []struct {
		name             string
		mtEnabled        bool
		tenantName       string
		tenantPermission *string
	}{
		{
			name:             "with multi-tenancy",
			mtEnabled:        true,
			tenantName:       "tenant1",
			tenantPermission: String("tenant1"),
		},
		{
			name:             "without multi-tenancy",
			mtEnabled:        false,
			tenantName:       "",
			tenantPermission: nil,
		},
	}

	for _, tt := range tests {
		// add classes with object
		className1 := "AuthZBatchObjs1"
		className2 := "AuthZBatchObjs2"
		deleteObjectClass(t, className1, adminAuth)
		deleteObjectClass(t, className2, adminAuth)
		defer deleteObjectClass(t, className1, adminAuth)
		defer deleteObjectClass(t, className2, adminAuth)
		c1 := &models.Class{
			Class: className1,
			Properties: []*models.Property{
				{
					Name:     "prop1",
					DataType: schema.DataTypeText.PropString(),
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: tt.mtEnabled},
		}
		c2 := &models.Class{
			Class: className2,
			Properties: []*models.Property{
				{
					Name:     "prop2",
					DataType: schema.DataTypeText.PropString(),
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: tt.mtEnabled},
		}
		require.Nil(t, createClass(t, c1, adminAuth))
		require.Nil(t, createClass(t, c2, adminAuth))
		if tt.mtEnabled {
			require.Nil(t, createTenant(t, c1.Class, []*models.Tenant{{Name: tt.tenantName}}, adminKey))
			require.Nil(t, createTenant(t, c2.Class, []*models.Tenant{{Name: tt.tenantName}}, adminKey))
		}

		allPermissions := []*models.Permission{
			{
				Action: &createDataAction,
				Data:   &models.PermissionData{Collection: &className1, Tenant: tt.tenantPermission},
			},
			{
				Action: &updateDataAction,
				Data:   &models.PermissionData{Collection: &className1, Tenant: tt.tenantPermission},
			},
			{
				Action: &createDataAction,
				Data:   &models.PermissionData{Collection: &className2, Tenant: tt.tenantPermission},
			},
			{
				Action: &updateDataAction,
				Data:   &models.PermissionData{Collection: &className2, Tenant: tt.tenantPermission},
			},
		}
		restObjs := []*models.Object{
			{Class: className1, Properties: map[string]interface{}{"prop1": "test"}, Tenant: tt.tenantName},
			{Class: className2, Properties: map[string]interface{}{"prop2": "test"}, Tenant: tt.tenantName},
		}
		grpcObjs := []*pb.BatchObject{
			{
				Collection: className1,
				Properties: &pb.BatchObject_Properties{
					NonRefProperties: &structpb.Struct{
						Fields: map[string]*structpb.Value{"prop1": {Kind: &structpb.Value_StringValue{StringValue: "test"}}},
					},
				},
				Tenant: tt.tenantName,
				Uuid:   string(UUID1),
			},
			{
				Collection: className2,
				Properties: &pb.BatchObject_Properties{
					NonRefProperties: &structpb.Struct{
						Fields: map[string]*structpb.Value{"prop2": {Kind: &structpb.Value_StringValue{StringValue: "test"}}},
					},
				},
				Tenant: tt.tenantName,
				Uuid:   string(UUID2),
			},
		}
		t.Run(fmt.Sprintf("all rights for both classes %s", tt.name), func(t *testing.T) {
			deleteRole := &models.Role{
				Name:        &testRoleName,
				Permissions: allPermissions,
			}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

			params := batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{Objects: restObjs})
			rest, err := helper.Client(t).Batch.BatchObjectsCreate(params, customAuth)
			require.Nil(t, err)
			for _, elem := range rest.Payload {
				assert.Nil(t, elem.Result.Errors)
			}

			grpc, err := helper.ClientGRPC(t).BatchObjects(
				metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey)),
				&pb.BatchObjectsRequest{Objects: grpcObjs},
			)
			require.Nil(t, err)
			require.Len(t, grpc.Errors, 0)

			_, err = helper.Client(t).Authz.RevokeRoleFromUser(
				authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)
			helper.DeleteRole(t, adminKey, testRoleName)
		})

		for _, permissions := range generateMissingLists(allPermissions) {
			t.Run(fmt.Sprintf("single permission missing %s", tt.name), func(t *testing.T) {
				deleteRole := &models.Role{
					Name:        &testRoleName,
					Permissions: permissions,
				}
				helper.DeleteRole(t, adminKey, *deleteRole.Name)
				helper.CreateRole(t, adminKey, deleteRole)
				_, err := helper.Client(t).Authz.AssignRoleToUser(
					authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{testRoleName}}),
					adminAuth,
				)
				require.Nil(t, err)

				params := batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{Objects: restObjs})
				_, err = helper.Client(t).Batch.BatchObjectsCreate(params, customAuth)
				var batchObjectsCreateForbidden *batch.BatchObjectsCreateForbidden
				require.True(t, errors.As(err, &batchObjectsCreateForbidden))

				res, err := helper.ClientGRPC(t).BatchObjects(
					metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey)),
					&pb.BatchObjectsRequest{Objects: grpcObjs},
				)
				require.Nil(t, err)
				require.Len(t, res.Errors, 1) // the other object is in another class so is covered by one of the permissions
				for _, err := range res.Errors {
					require.Contains(t, err.Error, "rbac: authorization, forbidden action: user 'custom-user' has insufficient permissions")
				}

				_, err = helper.Client(t).Authz.RevokeRoleFromUser(
					authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{testRoleName}}),
					adminAuth,
				)
				require.Nil(t, err)
				helper.DeleteRole(t, adminKey, testRoleName)
			})
		}
	}
}

func TestAuthZBatchObjsTenantFiltering(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	// customAuth := helper.CreateAuth(customKey)
	testRoleName := "test-role"

	updateDataAction := authorization.UpdateData
	createDataAction := authorization.CreateData

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
	helper.CreateClassAuth(t, cls, adminKey)
	defer helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)

	helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: "tenant1"}, {Name: "tenant2"}}, adminKey)

	objs := []*pb.BatchObject{
		{
			Collection: cls.Class,
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{"contents": {Kind: &structpb.Value_StringValue{StringValue: "test"}}},
				},
			},
			Tenant: "tenant1",
			Uuid:   string(UUID1),
		},
		{
			Collection: cls.Class,
			Properties: &pb.BatchObject_Properties{
				NonRefProperties: &structpb.Struct{
					Fields: map[string]*structpb.Value{"contents": {Kind: &structpb.Value_StringValue{StringValue: "test"}}},
				},
			},
			Tenant: "tenant2",
			Uuid:   string(UUID2),
		},
	}

	t.Run("cannot insert into either tenant without permissions", func(t *testing.T) {
		res, err := helper.ClientGRPC(t).BatchObjects(
			metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey)),
			&pb.BatchObjectsRequest{Objects: objs},
		)
		require.Nil(t, err)
		require.Len(t, res.Errors, 2)
		for _, err := range res.Errors {
			require.Contains(t, err.Error, "rbac: authorization, forbidden action: user 'custom-user' has insufficient permissions")
		}
	})

	t.Run("assign permissions to insert data into tenant1", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{
			Name: &testRoleName,
			Permissions: []*models.Permission{{
				Action: &createDataAction,
				Data:   &models.PermissionData{Collection: &cls.Class, Tenant: String("tenant1")},
			}, {
				Action: &updateDataAction,
				Data:   &models.PermissionData{Collection: &cls.Class, Tenant: String("tenant1")},
			}},
		})
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)
	})

	t.Run("can insert into tenant1 but not into tenant2", func(t *testing.T) {
		res, err := helper.ClientGRPC(t).BatchObjects(
			metadata.AppendToOutgoingContext(context.Background(), "authorization", fmt.Sprintf("Bearer %s", customKey)),
			&pb.BatchObjectsRequest{Objects: objs},
		)
		require.Nil(t, err)
		require.Len(t, res.Errors, 1)
		require.Contains(t, res.Errors[0].Error, "rbac: authorization, forbidden action: user 'custom-user' has insufficient permissions")
		require.Equal(t, res.Errors[0].Index, int32(1))
	})
}
