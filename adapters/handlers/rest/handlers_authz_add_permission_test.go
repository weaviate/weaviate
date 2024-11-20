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

package rest

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	schemaMocks "github.com/weaviate/weaviate/usecases/schema/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestAddPermissionsSuccess(t *testing.T) {
	type testCase struct {
		name                        string
		principal                   *models.Principal
		params                      authz.AddPermissionsParams
		readCollection              bool
		readTenant                  bool
		readTenantWithoutCollection bool
	}

	tests := []testCase{
		{
			name:      "all are *",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("test"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
						},
					},
				},
			},
		},
		{
			name:      "collection checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:     String(authorization.CreateCollections),
							Collection: String("ABC"),
						},
					},
				},
			},
			readCollection: true,
		},
		{
			name:      "collection and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:     String(authorization.CreateCollections),
							Collection: String("ABC"),
							Tenant:     String("Tenant1"),
						},
					},
				},
			},
			readCollection: true,
			readTenant:     true,
		},
		{
			name:      "* collections and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Tenant: String("Tenant1"),
						},
					},
				},
			},
			readTenantWithoutCollection: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			schemaReader := schemaMocks.NewSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			policies, err := conv.RolesToPolicies(&models.Role{
				Name:        tt.params.Body.Name,
				Permissions: tt.params.Body.Permissions,
			})
			require.Nil(t, err)

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(nil)
			controller.On("UpsertRolesPermissions", policies).Return(nil)

			if tt.readCollection {
				schemaReader.On("ReadOnlyClass",
					*tt.params.Body.Permissions[0].Collection).
					Return(&models.Class{Class: *tt.params.Body.Permissions[0].Collection})
			}

			if tt.readTenant {
				schemaReader.On("TenantsShards", mock.Anything, *tt.params.Body.Permissions[0].Collection,
					*tt.params.Body.Permissions[0].Tenant).
					Return(map[string]string{*tt.params.Body.Permissions[0].Tenant: "ACTIVE"}, nil)
			}

			if tt.readTenantWithoutCollection {
				schemaReader.On("GetSchemaSkipAuth").Return(schema.Schema{
					Objects: &models.Schema{
						Classes: []*models.Class{{Class: "ABC"}},
					},
				})

				schemaReader.On("CopyShardingState", "ABC").Return(&sharding.State{
					Physical: map[string]sharding.Physical{*tt.params.Body.Permissions[0].Tenant: {Name: "anything"}},
				})
			}

			h := &authZHandlers{
				authorizer:   authorizer,
				controller:   controller,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestAddPermissionsBadRequest(t *testing.T) {
	type testCase struct {
		name                        string
		params                      authz.AddPermissionsParams
		principal                   *models.Principal
		expectedError               string
		readCollection              bool
		readTenant                  bool
		readTenantWithoutCollection bool
	}

	tests := []testCase{
		{
			name: "role name is required",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String(""),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role name is required",
		},
		{
			name: "role has to have at least 1 permission",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name:        String("someName"),
					Permissions: []*models.Permission{},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role has to have at least 1 permission",
		},
		{
			name: "invalid action",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("someName"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_somethingelse"),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "invalid permission",
		},
		{
			name: "update builtin role",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: &authorization.BuiltInRoles[0],
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "you can not update builtin role",
		},
		{
			name: "collection doesn't exist",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:     String("manage_roles"),
							Collection: String("ABC"),
						},
					},
				},
			},
			readCollection: true,
			expectedError:  "collection ABC doesn't exists",
		},
		{
			name: "tenant doesn't exist",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:     String("manage_roles"),
							Collection: String("ABC"),
							Tenant:     String("Tenant1"),
						},
					},
				},
			},
			readTenant:    true,
			expectedError: "tenant Tenant1 doesn't exist",
		},
		{
			name: "tenant doesn't exist with * collection",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Tenant: String("Tenant1"),
						},
					},
				},
			},
			readTenantWithoutCollection: true,
			expectedError:               "tenant Tenant1 doesn't exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := mocks.NewController(t)
			schemaReader := schemaMocks.NewSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			if tt.readCollection {
				schemaReader.On("ReadOnlyClass", *tt.params.Body.Permissions[0].Collection).Return(nil)
			}

			if tt.readTenant {
				schemaReader.On("ReadOnlyClass",
					*tt.params.Body.Permissions[0].Collection).
					Return(&models.Class{Class: *tt.params.Body.Permissions[0].Collection})

				schemaReader.On("TenantsShards", mock.Anything, *tt.params.Body.Permissions[0].Collection,
					*tt.params.Body.Permissions[0].Tenant).
					Return(map[string]string{}, nil)
			}

			if tt.readTenantWithoutCollection {
				schemaReader.On("GetSchemaSkipAuth").Return(schema.Schema{
					Objects: &models.Schema{
						Classes: []*models.Class{{Class: "ABC"}},
					},
				})

				schemaReader.On("CopyShardingState", "ABC").Return(&sharding.State{
					Physical: map[string]sharding.Physical{},
				})
			}
			h := &authZHandlers{
				controller:   controller,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAddPermissionsForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AddPermissionsParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "update some role",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("someRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("some error from authZ"),
			expectedError: "some error from authZ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAddPermissionsInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AddPermissionsParams
		principal     *models.Principal
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "update some role",
			params: authz.AddPermissionsParams{
				Body: authz.AddPermissionsBody{
					Name: String("someRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			upsertErr:     fmt.Errorf("some error from controller"),
			expectedError: "some error from controller",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(nil)
			controller.On("UpsertRolesPermissions", mock.Anything).Return(tt.upsertErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.addPermissions(tt.params, tt.principal)
			parsed, ok := res.(*authz.AddPermissionsInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
