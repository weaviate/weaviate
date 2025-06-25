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
	"github.com/weaviate/weaviate/usecases/schema"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestCreateRoleSuccess(t *testing.T) {
	type testCase struct {
		name      string
		principal *models.Principal
		params    authz.CreateRoleParams
	}

	tests := []testCase{
		{
			name:      "all are *",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
		},
		{
			name:      "collection checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{
								Collection: String("ABC"),
							},
						},
					},
				},
			},
		},
		{
			name:      "collection and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Tenants: &models.PermissionTenants{
								Collection: String("ABC"),
								Tenant:     String("Tenant1"),
							},
						},
					},
				},
			},
		},
		{
			name:      "* collections and tenant checks",
			principal: &models.Principal{Username: "user1"},
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String(authorization.CreateCollections),
							Tenants: &models.PermissionTenants{
								Tenant: String("Tenant1"),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			schemaReader := schema.NewMockSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			controller.On("GetRoles", *tt.params.Body.Name).Return(map[string][]authorization.Policy{}, nil)
			controller.On("CreateRolesPermissions", mock.Anything).Return(nil)

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
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.CreateRoleParams{
		HTTPRequest: req,
		Body: &models.Role{
			Name: String("newRole"),
			Permissions: []*models.Permission{
				{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{},
				},
			},
		},
	}
	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL), authorization.Roles("newRole")[0]).Return(nil)
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
	}

	tests := []testCase{
		{
			name: "role name is required",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String(""),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			expectedError: "role name is required",
		},
		{
			name: "invalid role name",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("something/wrong"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			expectedError: "role name is invalid",
		},
		{
			name: "invalid permission",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("someRole"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_something"),
						},
					},
				},
			},
			expectedError: "invalid role",
		},
		{
			name: "cannot create role with the same name as builtin role",
			params: authz.CreateRoleParams{
				HTTPRequest: req,
				Body: &models.Role{
					Name: &authorization.BuiltInRoles[0],
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
						},
					},
				},
			},
			expectedError: "you cannot create role with the same name as built-in role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			schemaReader := schema.NewMockSchemaGetter(t)
			logger, _ := test.NewNullLogger()

			if tt.expectedError == "" {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				controller.On("GetRoles", *tt.params.Body.Name).Return(map[string][]authorization.Policy{}, nil)
				controller.On("upsertRolesPermissions", mock.Anything).Return(tt.upsertErr)
			}

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
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
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
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(*tt.params.Body.Name)[0]).Return(tt.authorizeErr)
			if tt.authorizeErr != nil {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH), authorization.Roles(*tt.params.Body.Name)[0]).Return(tt.authorizeErr)
			}

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
				HTTPRequest: req,
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action:      String(authorization.CreateCollections),
							Collections: &models.PermissionCollections{},
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
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			policies, err := conv.RolesToPolicies(tt.params.Body)
			require.Nil(t, err)

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(*tt.params.Body.Name)[0]).Return(nil)
			controller.On("GetRoles", *tt.params.Body.Name).Return(map[string][]authorization.Policy{}, nil)
			controller.On("CreateRolesPermissions", policies).Return(tt.upsertErr)

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

func TestCreateRoleUnprocessableRegexp(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	params := authz.CreateRoleParams{
		HTTPRequest: req,
		Body: &models.Role{
			Name: String("newRole"),
			Permissions: []*models.Permission{
				{
					Action:      String(authorization.CreateCollections),
					Collections: &models.PermissionCollections{Collection: String("/[a-z+/")},
				},
			},
		},
	}

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.createRole(params, &models.Principal{Username: "user1"})
	_, ok := res.(*authz.CreateRoleUnprocessableEntity)
	assert.True(t, ok)
	assert.Contains(t, res.(*authz.CreateRoleUnprocessableEntity).Payload.Error[0].Message, "role permissions are invalid")
}

func String(s string) *string {
	return &s
}
