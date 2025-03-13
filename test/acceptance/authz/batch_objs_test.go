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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZBatchObjREST(t *testing.T) {
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
		className1 := "AuthZBatchObjREST1"
		className2 := "AuthZBatchObjREST2"
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
		t.Run(fmt.Sprintf("all rights for both classes %s", tt.name), func(t *testing.T) {
			deleteRole := &models.Role{
				Name:        &testRoleName,
				Permissions: allPermissions,
			}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			_, err := helper.Client(t).Authz.AssignRoleToUser(
				authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)

			params := batch.NewBatchObjectsCreateParams().WithBody(
				batch.BatchObjectsCreateBody{
					Objects: []*models.Object{
						{Class: className1, Properties: map[string]interface{}{"prop1": "test"}, Tenant: tt.tenantName},
						{Class: className2, Properties: map[string]interface{}{"prop2": "test"}, Tenant: tt.tenantName},
					},
				},
			)
			res, err := helper.Client(t).Batch.BatchObjectsCreate(params, customAuth)
			require.Nil(t, err)
			for _, elem := range res.Payload {
				assert.Nil(t, elem.Result.Errors)
			}

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

				params := batch.NewBatchObjectsCreateParams().WithBody(
					batch.BatchObjectsCreateBody{
						Objects: []*models.Object{
							{Class: className1, Properties: map[string]interface{}{"prop1": "test"}, Tenant: tt.tenantName},
							{Class: className2, Properties: map[string]interface{}{"prop2": "test"}, Tenant: tt.tenantName},
						},
					},
				)
				_, err = helper.Client(t).Batch.BatchObjectsCreate(params, customAuth)
				var batchObjectsCreateForbidden *batch.BatchObjectsCreateForbidden
				require.True(t, errors.As(err, &batchObjectsCreateForbidden))

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
