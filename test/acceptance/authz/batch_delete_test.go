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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	UUIDTo      = strfmt.UUID("00000000-0000-0000-0000-000000000001")
	UUIDFrom    = strfmt.UUID("00000000-0000-0000-0000-000000000002")
	beaconStart = "weaviate://localhost/"
)

func TestAuthZBatchDelete(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)
	testRoleName := "test-role"
	deleteDataAction := authorization.DeleteData
	readCollectionsAction := authorization.ReadCollections
	readDataAction := authorization.ReadData

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	// add classes with object
	classNameTarget := "AuthZBatchDeleteTestTarget"
	c := &models.Class{
		Class: classNameTarget,
		Properties: []*models.Property{
			{
				Name:     "prop",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	deleteObjectClass(t, classNameTarget, adminAuth)
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, adminAuth)
	assert.NoError(t, err)
	assert.NotEmpty(t, resp)

	paramsObj := objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			ID:    UUIDTo,
			Class: classNameTarget,
			Properties: map[string]interface{}{
				"prop": "test",
			},
		})

	respObj, err := helper.Client(t).Objects.ObjectsCreate(paramsObj, adminAuth)
	assert.NoError(t, err)
	assert.NotEmpty(t, respObj)

	classNameSource := "AuthZBatchDeleteTestSource"
	c2 := &models.Class{
		Class: classNameSource,
		Properties: []*models.Property{
			{
				Name:     "someProperty",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "ref",
				DataType: []string{classNameTarget},
			},
		},
	}

	deleteObjectClass(t, classNameSource, adminAuth)
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c2)
	resp2, err2 := helper.Client(t).Schema.SchemaObjectsCreate(params2, adminAuth)
	assert.NoError(t, err2)
	assert.NotEmpty(t, resp2)

	paramsObj = objects.NewObjectsCreateParams().WithBody(
		&models.Object{
			ID:    UUIDFrom,
			Class: classNameSource,
			Properties: map[string]interface{}{
				"prop": "test",
			},
		})

	respObj, err = helper.Client(t).Objects.ObjectsCreate(paramsObj, adminAuth)
	assert.NoError(t, err)
	assert.NotEmpty(t, respObj)

	// add refs
	from := beaconStart + classNameSource + "/" + UUIDFrom.String() + "/ref"
	to := beaconStart + UUIDTo
	batchRefs := []*models.BatchReference{
		{From: strfmt.URI(from), To: strfmt.URI(to)},
	}
	paramsRef := batch.NewBatchReferencesCreateParams().WithBody(batchRefs)
	_, err = helper.Client(t).Batch.BatchReferencesCreate(paramsRef, adminAuth)
	require.Nil(t, err)

	allNonRefPermissions := []*models.Permission{
		{
			Action: &deleteDataAction,
			Data:   &models.PermissionData{Collection: &classNameSource},
		},
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &classNameSource},
		},
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &classNameSource},
		},
	}
	t.Run("all rights without reference", func(t *testing.T) {
		deleteRole := &models.Role{
			Name:        &testRoleName,
			Permissions: allNonRefPermissions,
		}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		_, err = helper.Client(t).Authz.AssignRole(
			authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}),
			adminAuth,
		)
		require.Nil(t, err)

		params := getBatchDelete(classNameSource, []string{"prop"}, "something", true)
		resp, err := helper.Client(t).Batch.BatchObjectsDelete(params, customAuth)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, resp.Payload.Results.Matches, int64(1))

		_, err = helper.Client(t).Authz.RevokeRole(
			authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{testRoleName}}),
			adminAuth,
		)
		require.Nil(t, err)
		helper.DeleteRole(t, adminKey, testRoleName)
	})

	for _, permissions := range generateMissingLists(allNonRefPermissions) {
		t.Run("Single class without permissions", func(t *testing.T) {
			role := &models.Role{
				Name:        &testRoleName,
				Permissions: permissions,
			}
			helper.DeleteRole(t, adminKey, testRoleName)
			helper.CreateRole(t, adminKey, role)
			_, err = helper.Client(t).Authz.AssignRole(
				authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)

			params := getBatchDelete(classNameSource, []string{"someProperty"}, "something", true)
			_, err := helper.Client(t).Batch.BatchObjectsDelete(params, customAuth)
			require.NotNil(t, err)
			var batchObjectsDeleteUnauthorized *batch.BatchObjectsDeleteForbidden
			require.True(t, errors.As(err, &batchObjectsDeleteUnauthorized))

			_, err = helper.Client(t).Authz.RevokeRole(
				authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)
			helper.DeleteRole(t, adminKey, testRoleName)
		})
	}

	allRefPermissions := []*models.Permission{
		{
			Action: &deleteDataAction,
			Data:   &models.PermissionData{Collection: &classNameSource},
		},
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &classNameSource},
		},
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &classNameSource},
		},
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &classNameTarget},
		},
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &classNameTarget},
		},
	}
	t.Run("all rights with reference", func(t *testing.T) {
		deleteRole := &models.Role{
			Name:        &testRoleName,
			Permissions: allRefPermissions,
		}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		_, err = helper.Client(t).Authz.AssignRole(
			authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}),
			adminAuth,
		)
		require.Nil(t, err)

		params := getBatchDelete(classNameSource, []string{"ref", classNameTarget, "prop"}, "something", true)
		resp, err := helper.Client(t).Batch.BatchObjectsDelete(params, customAuth)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, resp.Payload.Results.Matches, int64(1))

		_, err = helper.Client(t).Authz.RevokeRole(
			authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{testRoleName}}),
			adminAuth,
		)
		require.Nil(t, err)
		helper.DeleteRole(t, adminKey, testRoleName)
	})

	for _, permissions := range generateMissingLists(allRefPermissions) {
		t.Run("No delete rights for class ref class", func(t *testing.T) {
			deleteRole := &models.Role{
				Name:        &testRoleName,
				Permissions: permissions,
			}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			_, err = helper.Client(t).Authz.AssignRole(
				authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)

			params := getBatchDelete(classNameSource, []string{"ref", classNameTarget}, "something", true)
			_, err := helper.Client(t).Batch.BatchObjectsDelete(params, customAuth)
			require.NotNil(t, err)
			var batchObjectsDeleteUnauthorized *batch.BatchObjectsDeleteForbidden
			require.True(t, errors.As(err, &batchObjectsDeleteUnauthorized))

			_, err = helper.Client(t).Authz.RevokeRole(
				authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{testRoleName}}),
				adminAuth,
			)
			require.Nil(t, err)
			helper.DeleteRole(t, adminKey, testRoleName)
		})
	}
}

func getBatchDelete(className string, path []string, valueText string, dryRun bool) *batch.BatchObjectsDeleteParams {
	output := "verbose"
	params := batch.NewBatchObjectsDeleteParams().WithBody(&models.BatchDelete{
		Match: &models.BatchDeleteMatch{
			Class: className,
			Where: &models.WhereFilter{
				Operator:  "NotEqual",
				Path:      path,
				ValueText: &valueText,
			},
		},
		DryRun: &dryRun,
		Output: &output,
	})
	return params
}
