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

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZObjectsEndpoints(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)

	createDataAction := authorization.CreateData
	readDataAction := authorization.ReadData
	updateDataAction := authorization.UpdateData
	deleteDataAction := authorization.DeleteData

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
			tenantPermission: String("tenant-1"),
		},
		{
			name:             "without multi-tenancy",
			mtEnabled:        false,
			tenantName:       "",
			tenantPermission: nil,
		},
	}

	roleName := "AuthZObjectsTestRole"
	className := "AuthZObjectsTest"
	tenantNames := []string{"tenant-1", "tenant-2"}
	tenants := []*models.Tenant{
		{Name: tenantNames[0]},
		{Name: tenantNames[1]},
	}

	for _, tt := range tests {
		obj := &models.Object{
			ID:    strfmt.UUID(uuid.New().String()),
			Class: className,
			Properties: map[string]interface{}{
				"prop": "test",
			},
			Tenant: tt.tenantName,
		}

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
			helper.CreateTenantsAuth(t, className, tenants, adminKey)
		}

		t.Run("Objects create (POST)", func(t *testing.T) {
			t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
				_, err := createObject(t, obj, customKey)
				require.NotNil(t, err)
				var errNoAuth *objects.ObjectsCreateForbidden
				if !errors.As(err, &errNoAuth) {
					t.Fatalf("Expected error of type %T, got %T", errNoAuth, err)
				}
				require.True(t, errors.As(err, &errNoAuth))
			})

			role := &models.Role{
				Name: &roleName,
				Permissions: []*models.Permission{
					{
						Action: &createDataAction,
						Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
					},
				},
			}
			helper.CreateRole(t, adminKey, role)
			defer helper.DeleteRole(t, adminKey, *role.Name)

			t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				_, err := createObject(t, obj, customKey)
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				require.Nil(t, err)

				if tt.mtEnabled {
					t.Run("Fail to create with different tenant", func(t *testing.T) {
						objNew := *obj
						objNew.Tenant = tenantNames[1]
						_, err := createObject(t, &objNew, customKey)
						require.NotNil(t, err)
						var errNoAuth *objects.ObjectsCreateForbidden
						if !errors.As(err, &errNoAuth) {
							t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
						}
						require.True(t, errors.As(err, &errNoAuth))
					})
				}
			})
		})

		t.Run("Objects get (GET)", func(t *testing.T) {
			var tenant *string
			if tt.mtEnabled {
				tenant = &tt.tenantName
			}
			t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
				_, err := getObject(t, obj.Class, obj.ID, tenant, customKey)
				require.NotNil(t, err)
				var errNoAuth *objects.ObjectsClassGetForbidden
				if !errors.As(err, &errNoAuth) {
					t.Fatalf("Expected error of type %T, got %T", errNoAuth, err)
				}
				require.True(t, errors.As(err, &errNoAuth))
			})

			role := &models.Role{
				Name: &roleName,
				Permissions: []*models.Permission{
					{
						Action: &readDataAction,
						Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
					},
				},
			}
			helper.CreateRole(t, adminKey, role)
			defer helper.DeleteRole(t, adminKey, *role.Name)

			t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				_, err := getObject(t, obj.Class, obj.ID, tenant, customKey)
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				require.Nil(t, err)

				if tt.mtEnabled {
					t.Run("Fail to get with different tenant", func(t *testing.T) {
						_, err := getObject(t, obj.Class, obj.ID, &tenantNames[1], customKey)
						require.NotNil(t, err)
						var errNoAuth *objects.ObjectsClassGetForbidden
						if !errors.As(err, &errNoAuth) {
							t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
						}
						require.True(t, errors.As(err, &errNoAuth))
					})
				}
			})
		})

		t.Run("Objects class update (PATCH)", func(t *testing.T) {
			t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
				_, err := updateObject(t, obj, customKey)
				require.NotNil(t, err)
				var errNoAuth *objects.ObjectsClassPatchForbidden
				if !errors.As(err, &errNoAuth) {
					t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
				}
				require.True(t, errors.As(err, &errNoAuth))
			})

			role := &models.Role{
				Name: &roleName,
				Permissions: []*models.Permission{
					{
						Action: &updateDataAction,
						Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
					},
				},
			}
			helper.CreateRole(t, adminKey, role)
			defer helper.DeleteRole(t, adminKey, *role.Name)

			t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				_, err := updateObject(t, obj, customKey)
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				require.Nil(t, err)

				if tt.mtEnabled {
					t.Run("Fail to update with different tenant", func(t *testing.T) {
						objNew := *obj
						objNew.Tenant = tenantNames[1]
						_, err := updateObject(t, &objNew, customKey)
						require.NotNil(t, err)
						var errNoAuth *objects.ObjectsClassPatchForbidden
						if !errors.As(err, &errNoAuth) {
							t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
						}
						require.True(t, errors.As(err, &errNoAuth))
					})
				}
			})
		})

		t.Run("Objects class replace (PUT)", func(t *testing.T) {
			t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
				_, err := replaceObject(t, obj, customKey)
				require.NotNil(t, err)
				var errNoAuth *objects.ObjectsClassPutForbidden
				if !errors.As(err, &errNoAuth) {
					t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
				}
				require.True(t, errors.As(err, &errNoAuth))
			})

			role := &models.Role{
				Name: &roleName,
				Permissions: []*models.Permission{
					{
						Action: &updateDataAction,
						Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
					},
				},
			}
			helper.CreateRole(t, adminKey, role)
			defer helper.DeleteRole(t, adminKey, *role.Name)

			t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				_, err := replaceObject(t, obj, customKey)
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				require.Nil(t, err)

				if tt.mtEnabled {
					t.Run("Fail to update with different tenant", func(t *testing.T) {
						objNew := *obj
						objNew.Tenant = tenantNames[1]
						_, err := replaceObject(t, &objNew, customKey)
						require.NotNil(t, err)
						var errNoAuth *objects.ObjectsClassPutForbidden
						if !errors.As(err, &errNoAuth) {
							t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
						}
						require.True(t, errors.As(err, &errNoAuth))
					})
				}
			})
		})

		t.Run("Objects exists (HEAD)", func(t *testing.T) {
			t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
				paramsObj := objects.NewObjectsClassHeadParams().WithClassName(obj.Class).WithID(obj.ID)
				if tt.mtEnabled {
					paramsObj = paramsObj.WithTenant(&tt.tenantName)
				}
				_, err := helper.Client(t).Objects.ObjectsClassHead(paramsObj, customAuth)
				require.NotNil(t, err)
				var errNoAuth *objects.ObjectsClassHeadForbidden
				require.True(t, errors.As(err, &errNoAuth))
			})

			t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
				role := &models.Role{
					Name: &roleName,
					Permissions: []*models.Permission{
						{
							Action: &readDataAction,
							Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
						},
					},
				}

				helper.CreateRole(t, adminKey, role)
				defer helper.DeleteRole(t, adminKey, *role.Name)

				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				paramsObj := objects.NewObjectsClassHeadParams().WithClassName(obj.Class).WithID(obj.ID)
				if tt.mtEnabled {
					paramsObj = paramsObj.WithTenant(&tt.tenantName)
				}
				_, err := helper.Client(t).Objects.ObjectsClassHead(paramsObj, customAuth)
				require.Nil(t, err)

				if tt.mtEnabled {
					t.Run("Fail to check existence with different tenant", func(t *testing.T) {
						paramsObj := objects.NewObjectsClassHeadParams().WithClassName(obj.Class).WithTenant(&tenantNames[1]).WithID(obj.ID)
						_, err := helper.Client(t).Objects.ObjectsClassHead(paramsObj, customAuth)
						require.NotNil(t, err)
						var errNoAuth *objects.ObjectsClassHeadForbidden
						if !errors.As(err, &errNoAuth) {
							t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
						}
						require.True(t, errors.As(err, &errNoAuth))
					})
				}
			})
		})

		t.Run("Objects validate (POST /validate)", func(t *testing.T) {
			t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
				paramsObj := objects.NewObjectsValidateParams().WithBody(obj)
				_, err := helper.Client(t).Objects.ObjectsValidate(paramsObj, customAuth)
				require.NotNil(t, err)
				var errNoAuth *objects.ObjectsValidateForbidden
				require.True(t, errors.As(err, &errNoAuth))
			})

			t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
				role := &models.Role{
					Name: &roleName,
					Permissions: []*models.Permission{
						{
							Action: &readDataAction,
							Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
						},
					},
				}

				helper.CreateRole(t, adminKey, role)
				defer helper.DeleteRole(t, adminKey, *role.Name)

				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				paramsObj := objects.NewObjectsValidateParams().WithBody(obj)
				_, err := helper.Client(t).Objects.ObjectsValidate(paramsObj, customAuth)
				require.Nil(t, err)

				if tt.mtEnabled {
					t.Run("Fail to validate with different tenant", func(t *testing.T) {
						objNew := *obj
						objNew.Tenant = tenantNames[1]
						paramsObj := objects.NewObjectsValidateParams().WithBody(&objNew)
						_, err := helper.Client(t).Objects.ObjectsValidate(paramsObj, customAuth)
						require.NotNil(t, err)
						var errNoAuth *objects.ObjectsValidateForbidden
						if !errors.As(err, &errNoAuth) {
							t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
						}
						require.True(t, errors.As(err, &errNoAuth))
					})
				}
			})
		})

		t.Run("Objects class delete (DELETE)", func(t *testing.T) {
			t.Run(fmt.Sprintf("No rights %s", tt.name), func(t *testing.T) {
				paramsObj := objects.NewObjectsClassDeleteParams().WithClassName(obj.Class).WithID(obj.ID)
				if tt.mtEnabled {
					paramsObj = paramsObj.WithTenant(&tt.tenantName)
				}
				_, err := helper.Client(t).Objects.ObjectsClassDelete(paramsObj, customAuth)
				require.NotNil(t, err)
				var errNoAuth *objects.ObjectsClassDeleteForbidden
				require.True(t, errors.As(err, &errNoAuth))
			})

			t.Run(fmt.Sprintf("All rights %s", tt.name), func(t *testing.T) {
				role := &models.Role{
					Name: &roleName,
					Permissions: []*models.Permission{
						{
							Action: &deleteDataAction,
							Data:   &models.PermissionData{Collection: &className, Tenant: tt.tenantPermission},
						},
					},
				}

				helper.CreateRole(t, adminKey, role)
				defer helper.DeleteRole(t, adminKey, *role.Name)

				helper.AssignRoleToUser(t, adminKey, roleName, customUser)
				defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

				paramsObj := objects.NewObjectsClassDeleteParams().WithClassName(obj.Class).WithID(obj.ID)
				if tt.mtEnabled {
					paramsObj = paramsObj.WithTenant(&tt.tenantName)
				}
				_, err := helper.Client(t).Objects.ObjectsClassDelete(paramsObj, customAuth)
				require.Nil(t, err)

				if tt.mtEnabled {
					t.Run("Fail to delete with different tenant", func(t *testing.T) {
						paramsObj := objects.NewObjectsClassDeleteParams().WithClassName(obj.Class).WithID(obj.ID).WithTenant(&tenantNames[1])
						_, err := helper.Client(t).Objects.ObjectsClassDelete(paramsObj, customAuth)
						require.NotNil(t, err)
						var errNoAuth *objects.ObjectsClassDeleteForbidden
						if !errors.As(err, &errNoAuth) {
							t.Fatalf("Expected error of type %T, got %T: %v", errNoAuth, err, err)
						}
						require.True(t, errors.As(err, &errNoAuth))
					})
				}
			})
		})
	}
}
