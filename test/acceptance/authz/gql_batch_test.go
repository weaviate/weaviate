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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZGQLBatchValidate(t *testing.T) {
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customAuth := helper.CreateAuth("custom-key")

	readDataAction := authorization.ReadData
	readCollectionsAction := authorization.ReadCollections

	helper.SetupClient("127.0.0.1:8081")
	defer helper.ResetClient()

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
		_, err := helper.Client(t).Authz.AssignRole(
			authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{roleName}}),
			adminAuth,
		)
		require.Nil(t, err)

		paramsObj := graphql.NewGraphqlBatchParams().WithBody(
			models.GraphQLQueries{{Query: "mutation assign role $role: AssignRoleInput!", OperationName: "POST"}})
		_, err = helper.Client(t).Graphql.GraphqlBatch(paramsObj, customAuth)
		require.Nil(t, err)

		_, err = helper.Client(t).Authz.RevokeRole(
			authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{roleName}}),
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
			_, err := helper.Client(t).Authz.AssignRole(
				authz.NewAssignRoleParams().WithID(customUser).WithBody(authz.AssignRoleBody{Roles: []string{roleName}}),
				adminAuth,
			)
			require.Nil(t, err)

			paramsObj := graphql.NewGraphqlBatchParams().WithBody(
				models.GraphQLQueries{{Query: "mutation assign role $role: AssignRoleInput!", OperationName: "POST"}})
			resp, err := helper.Client(t).Graphql.GraphqlBatch(paramsObj, customAuth)
			require.NotNil(t, err)
			require.Nil(t, resp)
			_, err = helper.Client(t).Authz.RevokeRole(
				authz.NewRevokeRoleParams().WithID(customUser).WithBody(authz.RevokeRoleBody{Roles: []string{roleName}}),
				adminAuth,
			)
			require.Nil(t, err)
			helper.DeleteRole(t, adminKey, roleName)
		})
	}
}
