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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZObjectValidate(t *testing.T) {
	t.Parallel()
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)

	readDataAction := authorization.ReadData
	readCollectionsAction := authorization.ReadCollections

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	roleName := "AuthZObjectValidateTestRole"
	className := "AuthZObjectValidateTest"
	deleteObjectClass(t, className, adminAuth)
	require.NoError(t, createClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "prop",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}, adminAuth))

	t.Run("All rights", func(t *testing.T) {
		deleteRole := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				{
					Action: &readDataAction,
					Data:   &models.PermissionData{Collection: &className},
				},
				{
					Action:      &readCollectionsAction,
					Collections: &models.PermissionCollections{Collection: &className},
				},
			},
		}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		_, err := helper.Client(t).Authz.AssignRole(
			authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{roleName}}),
			adminAuth,
		)
		require.Nil(t, err)

		paramsObj := objects.NewObjectsValidateParams().WithBody(
			&models.Object{
				ID:    strfmt.UUID(uuid.New().String()),
				Class: className,
				Properties: map[string]interface{}{
					"prop": "test",
				},
			})
		_, err = helper.Client(t).Objects.ObjectsValidate(paramsObj, customAuth)
		require.Nil(t, err)

		_, err = helper.Client(t).Authz.RevokeRole(
			authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{roleName}}),
			adminAuth,
		)
		require.Nil(t, err)
		helper.DeleteRole(t, adminKey, roleName)
	})

	perms := []*models.Permission{{Action: &readDataAction, Data: &models.PermissionData{Collection: &className}}, {Action: &readCollectionsAction, Collections: &models.PermissionCollections{Collection: &className}}}
	for _, perm := range perms {
		t.Run("Only rights for "+*perm.Action, func(t *testing.T) {
			deleteRole := &models.Role{
				Name:        &roleName,
				Permissions: []*models.Permission{perm},
			}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			_, err := helper.Client(t).Authz.AssignRole(
				authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{roleName}}),
				adminAuth,
			)
			require.Nil(t, err)

			paramsObj := objects.NewObjectsValidateParams().WithBody(
				&models.Object{
					ID:    strfmt.UUID(uuid.New().String()),
					Class: className,
					Properties: map[string]interface{}{
						"prop": "test",
					},
				})
			_, err = helper.Client(t).Objects.ObjectsValidate(paramsObj, customAuth)
			require.NotNil(t, err)
			var errNoAuth *objects.ObjectsValidateForbidden
			require.True(t, errors.As(err, &errNoAuth))

			_, err = helper.Client(t).Authz.RevokeRole(
				authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{roleName}}),
				adminAuth,
			)
			require.Nil(t, err)
			helper.DeleteRole(t, adminKey, roleName)
		})
	}
}
