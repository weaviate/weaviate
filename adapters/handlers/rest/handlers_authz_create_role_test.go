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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestCreateRoleSuccess(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.CreateRoleParams{
		Body: &models.Role{
			Name: String("newRole"),
			Permissions: []*models.Permission{
				{
					Action: String("manage_roles"),
				},
			},
		},
	}
	policies, err := conv.RolesToPolicies(params.Body)
	require.Nil(t, err)

	authorizer.On("Authorize", principal, authorization.CREATE, authorization.Roles()[0]).Return(nil)
	controller.On("GetRoles", *params.Body.Name).Return(map[string][]authorization.Policy{}, nil)
	controller.On("UpsertRolesPermissions", policies).Return(nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.createRole(params, principal)
	parsed, ok := res.(*authz.CreateRoleCreated)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
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
					Action: String("manage_roles"),
				},
			},
		},
	}
	authorizer.On("Authorize", principal, authorization.CREATE, authorization.Roles()[0]).Return(nil)
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
		principal     *models.Principal
		authorizeErr  error
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "role name is required",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String(""),
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
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
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role has to have at least 1 permission",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			policies, err := conv.RolesToPolicies(tt.params.Body)
			require.Nil(t, err)

			authorizer.On("Authorize", tt.principal, authorization.CREATE, authorization.Roles()[0]).Return(tt.authorizeErr)
			if tt.expectedError == "" {
				controller.On("GetRoles", *tt.params.Body.Name).Return([]*models.Role{}, nil)
				controller.On("UpsertRolesPermissions", policies).Return(tt.upsertErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.createRole(tt.params, tt.principal)
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
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "cannot create role with the same name as builtin role",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: &authorization.BuiltInRoles[0],
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
						},
					},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "you can not create role with the same name as builtin role",
		},
		{
			name: "authorization error",
			params: authz.CreateRoleParams{
				Body: &models.Role{
					Name: String("newRole"),
					Permissions: []*models.Permission{
						{
							Action: String("manage_roles"),
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

			policies, err := conv.RolesToPolicies(tt.params.Body)
			require.Nil(t, err)

			authorizer.On("Authorize", tt.principal, authorization.CREATE, authorization.Roles()[0]).Return(tt.authorizeErr)
			if tt.expectedError == "" {
				controller.On("GetRoles", *tt.params.Body.Name).Return([]*models.Role{}, nil)
				controller.On("UpsertRolesPermissions", policies).Return(tt.upsertErr)
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
		authorizeErr  error
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
							Action: String("manage_roles"),
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

			authorizer.On("Authorize", tt.principal, authorization.CREATE, authorization.Roles()[0]).Return(tt.authorizeErr)
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
