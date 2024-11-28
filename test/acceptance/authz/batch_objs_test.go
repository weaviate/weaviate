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
	"errors"
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

func TestBatchObjREST(t *testing.T) {
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customAuth := helper.CreateAuth("custom-key")
	testRoleName := "test-role"
	readSchemaAction := authorization.ReadSchema
	// readDataAction := authorization.ReadData
	updateDataAction := authorization.UpdateData
	createDataAction := authorization.CreateData

	helper.SetupClient("127.0.0.1:8081")
	defer helper.ResetClient()

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
	}
	c2 := &models.Class{
		Class: className2,
		Properties: []*models.Property{
			{
				Name:     "prop2",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	require.Nil(t, createClass(t, c1, adminAuth))
	require.Nil(t, createClass(t, c2, adminAuth))

	allPermissions := []*models.Permission{
		{
			Action:     &createDataAction,
			Collection: &className1,
		},
		{
			Action:     &updateDataAction,
			Collection: &className1,
		},
		{
			Action:     &readSchemaAction,
			Collection: &className1,
		},
		{
			Action:     &createDataAction,
			Collection: &className2,
		},
		{
			Action:     &updateDataAction,
			Collection: &className2,
		},
		{
			Action:     &readSchemaAction,
			Collection: &className2,
		},
	}
	t.Run("all rights for both classes", func(t *testing.T) {
		deleteRole := &models.Role{
			Name:        &testRoleName,
			Permissions: allPermissions,
		}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		_, err := helper.Client(t).Authz.AssignRole(
			authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}),
			adminAuth,
		)
		require.Nil(t, err)

		params := batch.NewBatchObjectsCreateParams().WithBody(
			batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{Class: className1, Properties: map[string]interface{}{"prop1": "test"}},
					{Class: className2, Properties: map[string]interface{}{"prop2": "test"}},
				},
			},
		)
		res, err := helper.Client(t).Batch.BatchObjectsCreate(params, customAuth)
		require.Nil(t, err)
		for _, elem := range res.Payload {
			assert.Nil(t, elem.Result.Errors)
		}

		_, err = helper.Client(t).Authz.RevokeRole(
			authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{testRoleName}}),
			adminAuth,
		)
		require.Nil(t, err)
		helper.DeleteRole(t, adminKey, testRoleName)
	})

	for _, permissions := range generateMissingLists(allPermissions) {
		t.Run("single permission missing", func(t *testing.T) {
			deleteRole := &models.Role{
				Name:        &testRoleName,
				Permissions: permissions,
			}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			_, err := helper.Client(t).Authz.AssignRole(
				authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)

			params := batch.NewBatchObjectsCreateParams().WithBody(
				batch.BatchObjectsCreateBody{
					Objects: []*models.Object{
						{Class: className1, Properties: map[string]interface{}{"prop1": "test"}},
						{Class: className2, Properties: map[string]interface{}{"prop2": "test"}},
					},
				},
			)
			_, err = helper.Client(t).Batch.BatchObjectsCreate(params, customAuth)
			var batchObjectsCreateForbidden *batch.BatchObjectsCreateForbidden
			require.True(t, errors.As(err, &batchObjectsCreateForbidden))

			_, err = helper.Client(t).Authz.RevokeRole(
				authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)
			helper.DeleteRole(t, adminKey, testRoleName)
		})
	}
}
