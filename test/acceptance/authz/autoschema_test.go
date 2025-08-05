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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAutoschemaAuthZ(t *testing.T) {
	customUser := "custom-user"
	customKey := "custom-key"
	adminKey := "admin-key"
	adminUser := "admin-user"
	adminAuth := helper.CreateAuth(adminKey)

	createDataAction := authorization.CreateData
	updateSchemaAction := authorization.UpdateCollections

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		WithRBAC().WithRbacRoots(adminUser).
		WithAutoschema().
		Start(ctx)

	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	className := "Class"
	classNameNew := "ClassNew"
	deleteObjectClass(t, className, adminAuth)
	deleteObjectClass(t, classNameNew, adminAuth)
	c := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	helper.CreateClassAuth(t, c, adminKey)

	// user needs to be able to create objects and read the configs
	all := "*"
	readSchemaAndCreateDataRoleName := "readSchemaAndCreateData"
	readSchemaRole := &models.Role{
		Name: &readSchemaAndCreateDataRoleName,
		Permissions: []*models.Permission{
			{Action: &authorization.ReadCollections, Collections: &models.PermissionCollections{Collection: &all}},
			{Action: &createDataAction, Data: &models.PermissionData{Collection: &all}},
		},
	}

	// create roles to assign to user later:
	updateSchemaRoleName := "updateSchema"
	updateSchemaRole := &models.Role{
		Name:        &updateSchemaRoleName,
		Permissions: []*models.Permission{{Action: &updateSchemaAction, Collections: &models.PermissionCollections{Collection: &className}}},
	}
	createSchemaRoleName := "createSchema"
	createSchemaRole := &models.Role{
		Name: &createSchemaRoleName,
		Permissions: []*models.Permission{
			{Action: &authorization.CreateCollections, Collections: &models.PermissionCollections{Collection: &classNameNew}},
		},
	}

	helper.DeleteRole(t, adminKey, *readSchemaRole.Name)
	helper.DeleteRole(t, adminKey, *createSchemaRole.Name)
	helper.DeleteRole(t, adminKey, *updateSchemaRole.Name)
	helper.CreateRole(t, adminKey, readSchemaRole)
	helper.CreateRole(t, adminKey, updateSchemaRole)
	helper.CreateRole(t, adminKey, createSchemaRole)
	defer helper.DeleteRole(t, adminKey, *readSchemaRole.Name)
	defer helper.DeleteRole(t, adminKey, *updateSchemaRole.Name)
	defer helper.DeleteRole(t, adminKey, *createSchemaRole.Name)

	// all tests need read schema
	helper.AssignRoleToUser(t, adminKey, readSchemaAndCreateDataRoleName, customUser)

	t.Run("Only read rights for schema", func(t *testing.T) {
		// object which does NOT introduce a new prop => no failure
		_, err = createObject(t, &models.Object{
			ID:         UUID1,
			Class:      className,
			Properties: map[string]interface{}{"name": "prop"},
			Tenant:     "",
		}, customKey)
		require.NoError(t, err)

		// object which does introduce a new prop => failure
		_, err = createObject(t, &models.Object{
			ID:         UUID2,
			Class:      className,
			Properties: map[string]interface{}{"other": "prop"},
			Tenant:     "",
		}, customKey)
		require.Error(t, err)

		var batchObjectsDeleteUnauthorized *objects.ObjectsCreateForbidden
		require.True(t, errors.As(err, &batchObjectsDeleteUnauthorized))
	})

	t.Run("read and update rights for schema", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{updateSchemaRoleName}}),
			adminAuth,
		)
		require.NoError(t, err)

		// object which does NOT introduce a new prop => no failure
		_, err = createObject(t, &models.Object{
			ID:         UUID2,
			Class:      className,
			Properties: map[string]interface{}{"name": "prop"},
			Tenant:     "",
		}, customKey)
		require.NoError(t, err)

		// object which does introduce a new prop => also no failure
		_, err = createObject(t, &models.Object{
			ID:         UUID3,
			Class:      className,
			Properties: map[string]interface{}{"different": "prop"},
			Tenant:     "",
		}, customKey)
		require.NoError(t, err)

		// object which does introduce a new class => failure
		_, err = createObject(t, &models.Object{
			ID:         UUID4,
			Class:      classNameNew,
			Properties: map[string]interface{}{"different": "prop"},
			Tenant:     "",
		}, customKey)
		require.Error(t, err)

		var batchObjectsDeleteUnauthorized *objects.ObjectsCreateForbidden
		require.True(t, errors.As(err, &batchObjectsDeleteUnauthorized))
	})

	t.Run("create rights for schema", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AssignRoleToUser(
			authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{updateSchemaRoleName, createSchemaRoleName}}),
			adminAuth,
		)
		require.NoError(t, err)

		// object which does NOT introduce a new class
		_, err = createObject(t, &models.Object{
			ID:         UUID5,
			Class:      className,
			Properties: map[string]interface{}{"name": "prop"},
			Tenant:     "",
		}, customKey)
		require.NoError(t, err)

		// object which does introduce a new class
		_, err = createObject(t, &models.Object{
			ID:         UUID6,
			Class:      classNameNew,
			Properties: map[string]interface{}{"different": "prop"},
			Tenant:     "",
		}, customKey)
		require.NoError(t, err)
	})
}
