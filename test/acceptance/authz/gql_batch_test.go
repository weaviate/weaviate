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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZGQLBatchValidate(t *testing.T) {
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

	roleName := "AuthZGQLBatchTestRole"
	className := "AuthZGQLBatchTestClass"
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

	all := "*"

	t.Run("All rights", func(t *testing.T) {
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				{
					Action: &readDataAction,
					Data:   &models.PermissionData{Collection: &all},
				},
				{
					Action:      &readCollectionsAction,
					Collections: &models.PermissionCollections{Collection: &all},
				},
			},
		}
		helper.DeleteRole(t, adminKey, *role.Name)
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)

		paramsObj := graphql.NewGraphqlBatchParams().WithBody(
			models.GraphQLQueries{{Query: "mutation assign role $role: AssignRoleToUserInput!", OperationName: "POST"}})
		_, err := helper.Client(t).Graphql.GraphqlBatch(paramsObj, customAuth)
		require.Nil(t, err)

		_, err = helper.Client(t).Authz.RevokeRoleFromUser(
			authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{roleName}}),
			adminAuth,
		)
		require.Nil(t, err)
		helper.DeleteRole(t, adminKey, roleName)
	})

	permissionsAll := [][]*models.Permission{
		{{Action: &readDataAction, Data: &models.PermissionData{Collection: &all}}, {Action: &readCollectionsAction, Collections: &models.PermissionCollections{Collection: &className}}},
		{{Action: &readDataAction, Data: &models.PermissionData{Collection: &className}}, {Action: &readCollectionsAction, Collections: &models.PermissionCollections{Collection: &all}}},
	}
	for _, permissions := range permissionsAll {
		t.Run("Only read data action for a single class", func(t *testing.T) {
			role := &models.Role{
				Name:        &roleName,
				Permissions: permissions,
			}
			helper.DeleteRole(t, adminKey, roleName)
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)

			paramsObj := graphql.NewGraphqlBatchParams().WithBody(
				models.GraphQLQueries{{Query: "mutation assign role $role: AssignRoleToUserInput!", OperationName: "POST"}})
			resp, err := helper.Client(t).Graphql.GraphqlBatch(paramsObj, customAuth)
			require.NotNil(t, err)
			require.Nil(t, resp)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)

			helper.DeleteRole(t, adminKey, roleName)
		})
	}
}
