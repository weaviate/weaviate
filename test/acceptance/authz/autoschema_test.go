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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAutoschemaAuthZ(t *testing.T) {
	customUser := "custom-user"
	customKey := "custom-key"
	adminKey := "admin-key"
	// adminUser := "admin-user"
	adminAuth := helper.CreateAuth(adminKey)

	readSchemaAction := authorization.ReadSchema
	createDataAction := authorization.CreateData

	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	//defer cancel()
	//
	//compose, err := docker.New().WithWeaviate().WithAutoschema().
	//	WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
	//	WithRBAC().WithRbacAdmins(adminUser).Start(ctx)
	//require.Nil(t, err)
	//defer func() {
	//	if err := compose.Terminate(ctx); err != nil {
	//		t.Fatalf("failed to terminate test containers: %v", err)
	//	}
	//}()
	//helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupClient("127.0.0.1:8081")
	defer helper.ResetClient()

	className := "Class"
	c := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	deleteObjectClass(t, className, adminAuth)
	helper.CreateClassAuth(t, c, adminKey)

	// user needs to be able to create objects and read the class schema

	// create roles to assign to user later:
	readSchemaAndCreateDataRoleName := "readSchemaAndCreateData"
	readSchemaRole := &models.Role{
		Name: &readSchemaAndCreateDataRoleName,
		Permissions: []*models.Permission{
			{Action: &readSchemaAction, Collection: &className},
			{Action: &createDataAction, Collection: &className},
		},
	}
	helper.DeleteRole(t, adminKey, *readSchemaRole.Name)
	helper.CreateRole(t, adminKey, readSchemaRole)

	_, err := helper.Client(t).Authz.AssignRole(
		authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{readSchemaAndCreateDataRoleName}}),
		adminAuth,
	)
	require.Nil(t, err)

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

	updateSchemaRoleName := "updateSchema"
	updateSchemaRole := &models.Role{
		Name:        &updateSchemaRoleName,
		Permissions: []*models.Permission{{Action: &readSchemaAction, Collection: &className}},
	}
	helper.DeleteRole(t, adminKey, *updateSchemaRole.Name)
	helper.CreateRole(t, adminKey, updateSchemaRole)

	t.Run("read and update rights for schema", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AssignRole(
			authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{updateSchemaRoleName}}),
			adminAuth,
		)
		require.Nil(t, err)

		// object which does NOT introduce a new prop => no failure
		_, err = createObject(t, &models.Object{
			ID:         UUID3,
			Class:      className,
			Properties: map[string]interface{}{"name": "prop"},
			Tenant:     "",
		}, customKey)
		require.NoError(t, err)

		// object which does introduce a new prop => failure
		_, err = createObject(t, &models.Object{
			ID:         UUID4,
			Class:      className,
			Properties: map[string]interface{}{"other": "prop"},
			Tenant:     "",
		}, customKey)
		require.NoError(t, err)
	})
}
