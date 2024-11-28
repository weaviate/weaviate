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
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	schemaMocks "github.com/weaviate/weaviate/usecases/schema/mocks"
)

func TestCreateRoleSuccess(t *testing.T) {
	type testCase struct {
		name      string
		principal *models.Principal
		params    authz.CreateRoleParams
		// readCollection              bool
		// readTenant                  bool
		// readTenantWithoutCollection bool
	}

	tests := []testCase{
		{
			name:      "all are *",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateSchema),
						},
					},
				},
			},
		},
		{
			name:      "collection checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:     String(authorization.CreateSchema),
							Collection: String("ABC"),
						},
					},
				},
			},
			// readCollection: true,
		},
		{
			name:      "collection and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:     String(authorization.CreateSchema),
							Collection: String("ABC"),
							Tenant:     String("Tenant1"),
						},
					},
				},
			},
			// readCollection: true,
			// readTenant:     true,
		},
		{
			name:      "* collections and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateSchema),
							Tenant: String("Tenant1"),
						},
					},
				},
			},
			// readTenantWithoutCollection: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			schemaReader := schemaMocks.NewSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			controller.On("GetRoles", *tt.params.Body.Name).Return(map[string][]authorization.Policy{}, nil)
			controller.On("UpsertRolesPermissions", mock.Anything).Return(nil)

			// if tt.readCollection {
			// 	schemaReader.On("ReadOnlyClass",
			// 		*tt.params.Body.Permissions[0].Collection).
			// 		Return(&models.Class{Class: *tt.params.Body.Permissions[0].Collection})
			// }

			// if tt.readTenant {
			// 	schemaReader.On("TenantsShards", mock.Anything, *tt.params.Body.Permissions[0].Collection,
			// 		*tt.params.Body.Permissions[0].Tenant).
			// 		Return(map[string]string{*tt.params.Body.Permissions[0].Tenant: "ACTIVE"}, nil)
			// }

			// if tt.readTenantWithoutCollection {
			// 	schemaReader.On("GetSchemaSkipAuth").Return(schema.Schema{
			// 		Objects: &models.Schema{
			// 			Classes: []*models.Class{{Class: "ABC"}},
			// 		},
			// 	})

			// 	schemaReader.On("CopyShardingState", "ABC").Return(&sharding.State{
			// 		Physical: map[string]sharding.Physical{*tt.params.Body.Permissions[0].Tenant: {Name: "anything"}},
			// 	})
			// }

			h := &authZHandlers{
				authorizer:   authorizer,
				controller:   controller,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.createRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.CreateRoleCreated)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateRoleConflict(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.CreateRoleParams{
		Body: &models.Role{
			Name: String("newRole"),
			Permissions: []*models.Permission{
				{
					Action: String(authorization.CreateSchema),
				},
			},
		},
	}
	authorizer.On("Authorize", principal, authorization.CREATE, authorization.Roles("newRole")[0]).Return(nil)
	controller.On("GetRoles", *params.Body.Name).Return(map[string][]authorization.Policy{"newRole": {}}, nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.createRole(params, principal)
	parsed, ok := res.(*authz.CreateRoleConflict)
	assert.True(t, ok)
	assert.Contains(t, parsed.Payload.Error[0].Message, fmt.Sprintf("role with name %s already exists", *params.Body.Name))
}

func TestCreateRoleBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.CreateRoleParams
		upsertErr     error
		expectedError string
		// readCollection              bool
		// readTenant                  bool
		// readTenantWithoutCollection bool
	}

	tests := []testCase{
		{
			name: "role name is required",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String(""),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateSchema),
						},
					},
				},
			},
			expectedError: "role name is required",
		},
		{
			name: "role has to have at least 1 permission",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name:        String("newRole"),
					Permissions: []*models.Permission{},
				},
			},
			expectedError: "role has to have at least 1 permission",
		},
		{
			name: "invalid permission",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("someRole"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_something"),
						},
					},
				},
			},
			expectedError: "invalid permission",
		},
		{
			name: "cannot create role with the same name as builtin role",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: &authorization.BuiltInRoles[0],
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateSchema),
						},
					},
				},
			},
			expectedError: "you can not create role with the same name as builtin role",
		},
		// {
		// 	name: "collection doesn't exist",
		// 	params: authz.CreateRoleParams{
		// 		Body: &models.Role{
		// 			Name: String("newRole"),
		// 			Permissions: []*models.Permission{
		// 				{
		// 					Action:     String(authorization.CreateSchema),
		// 					Collection: String("ABC"),
		// 				},
		// 			},
		// 		},
		// 	},
		// 	readCollection: true,
		// 	expectedError:  "collection ABC doesn't exists",
		// },
		// {
		// 	name: "tenant doesn't exist",
		// 	params: authz.CreateRoleParams{
		// 		Body: &models.Role{
		// 			Name: String("newRole"),
		// 			Permissions: []*models.Permission{
		// 				{
		// 					Action:     String(authorization.CreateSchema),
		// 					Collection: String("ABC"),
		// 					Tenant:     String("Tenant1"),
		// 				},
		// 			},
		// 		},
		// 	},
		// 	readTenant:    true,
		// 	expectedError: "tenant Tenant1 doesn't exist",
		// },
		// {
		// 	name: "tenant doesn't exist with * collection",
		// 	params: authz.CreateRoleParams{
		// 		Body: &models.Role{
		// 			Name: String("newRole"),
		// 			Permissions: []*models.Permission{
		// 				{
		// 					Action: String(authorization.CreateSchema),
		// 					Tenant: String("Tenant1"),
		// 				},
		// 			},
		// 		},
		// 	},
		// 	readTenantWithoutCollection: true,
		// 	expectedError:               "tenant Tenant1 doesn't exist",
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			schemaReader := schemaMocks.NewSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			if tt.expectedError == "" {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				controller.On("GetRoles", *tt.params.Body.Name).Return(map[string][]authorization.Policy{}, nil)
				controller.On("UpsertRolesPermissions", mock.Anything).Return(tt.upsertErr)
			}

			// if tt.readCollection {
			// 	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			// 	schemaReader.On("ReadOnlyClass", *tt.params.Body.Permissions[0].Collection).Return(nil)
			// }

			// if tt.readTenant {
			// 	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			// 	schemaReader.On("ReadOnlyClass",
			// 		*tt.params.Body.Permissions[0].Collection).
			// 		Return(&models.Class{Class: *tt.params.Body.Permissions[0].Collection})

			// 	schemaReader.On("TenantsShards", mock.Anything, *tt.params.Body.Permissions[0].Collection,
			// 		*tt.params.Body.Permissions[0].Tenant).
			// 		Return(map[string]string{}, nil)
			// }

			// if tt.readTenantWithoutCollection {
			// 	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			// 	schemaReader.On("GetSchemaSkipAuth").Return(schema.Schema{
			// 		Objects: &models.Schema{
			// 			Classes: []*models.Class{{Class: "ABC"}},
			// 		},
			// 	})

			// 	schemaReader.On("CopyShardingState", "ABC").Return(&sharding.State{
			// 		Physical: map[string]sharding.Physical{},
			// 	})
			// }

			h := &authZHandlers{
				authorizer:   authorizer,
				controller:   controller,
				schemaReader: schemaReader,
				logger:       logger,
			}
			res := h.createRole(tt.params, nil)
			parsed, ok := res.(*authz.CreateRoleBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestCreateRoleForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.CreateRoleParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateSchema),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  errors.New("authorization error"),
			expectedError: "authorization error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.CREATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.createRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.CreateRoleForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestCreateRoleInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.CreateRoleParams
		principal     *models.Principal
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "upsert roles permissions error",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateSchema),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			upsertErr:     errors.New("upsert error"),
			expectedError: "upsert error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			policies, err := conv.RolesToPolicies(tt.params.Body)
			require.Nil(t, err)

			authorizer.On("Authorize", tt.principal, authorization.CREATE, authorization.Roles(*tt.params.Body.Name)[0]).Return(nil)
			controller.On("GetRoles", *tt.params.Body.Name).Return(map[string][]authorization.Policy{}, nil)
			controller.On("UpsertRolesPermissions", policies).Return(tt.upsertErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.createRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.CreateRoleInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func String(s string) *string {
	return &s
}
