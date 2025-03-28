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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZObjectValidate(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)

	readDataAction := authorization.ReadData

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
			tenantName:       "tenant-1",
			tenantPermission: nil,
		},
		{
			name:             "without multi-tenancy",
			mtEnabled:        false,
			tenantName:       "",
			tenantPermission: nil,
		},
	}

	roleName := "AuthZObjectValidateTestRole"
	className := "AuthZObjectValidateTest"
	for _, tt := range tests {
		deleteObjectClass(t, className, adminAuth)
		require.NoError(t, createClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "prop",
					DataType: schema.DataTypeText.PropString(),
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: tt.mtEnabled},
		}, adminAuth))
		if tt.mtEnabled {
			helper.CreateTenantsAuth(t, className, []*models.Tenant{{Name: "tenant1"}}, adminKey)
		}

		t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
			paramsObj := objects.NewObjectsValidateParams().WithBody(
				&models.Object{
					ID:    strfmt.UUID(uuid.New().String()),
					Class: className,
					Properties: map[string]interface{}{
						"prop": "test",
					},
					Tenant: tt.tenantName,
				})
			_, err := helper.Client(t).Objects.ObjectsValidate(paramsObj, customAuth)
			require.NotNil(t, err)
			var errNoAuth *objects.ObjectsValidateForbidden
			require.True(t, errors.As(err, &errNoAuth))
		})

		t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
			deleteRole := &models.Role{
				Name: &roleName,
				Permissions: []*models.Permission{
					{
						Action: &readDataAction,
						Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
					},
				},
			}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			_, err := helper.Client(t).Authz.AssignRoleToUser(
				authz.NewAssignRoleToUserParams().WithID(customUser).WithBody(authz.AssignRoleToUserBody{Roles: []string{roleName}}),
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
					Tenant: tt.tenantName,
				})
			_, err = helper.Client(t).Objects.ObjectsValidate(paramsObj, customAuth)
			require.Nil(t, err)

			_, err = helper.Client(t).Authz.RevokeRoleFromUser(
				authz.NewRevokeRoleFromUserParams().WithID(customUser).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{roleName}}),
				adminAuth,
			)
			require.Nil(t, err)
			helper.DeleteRole(t, adminKey, roleName)
		})
	}
}
