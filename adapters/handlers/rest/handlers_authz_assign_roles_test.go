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
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestAssignRoleSuccess(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.AssignRoleParams{
		ID: "testUser",
		Body: authz.AssignRoleBody{
			Roles: []string{"testRole"},
		},
	}

	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Roles(params.Body.Roles...)[0]).Return(nil)
	controller.On("GetRoles", params.Body.Roles[0]).Return(map[string][]authorization.Policy{params.Body.Roles[0]: {}}, nil)
	controller.On("AddRolesForUser", params.ID, params.Body.Roles).Return(nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.assignRole(params, principal)
	parsed, ok := res.(*authz.AssignRoleOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestAssignRoleBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleParams
		principal     *models.Principal
		expectedError string
		existedRoles  map[string][]authorization.Policy
		callToGetRole bool
	}

	tests := []testCase{
		{
			name: "user id can not be empty",
			params: authz.AssignRoleParams{
				ID: "",
				Body: authz.AssignRoleBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "user id can not be empty",
		},
		{
			name: "empty role",
			params: authz.AssignRoleParams{
				ID: "testUser",
				Body: authz.AssignRoleBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to assign doesn't exist",
			existedRoles:  map[string][]authorization.Policy{},
			callToGetRole: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			if tt.callToGetRole {
				authorizer.On("Authorize", tt.principal, authorization.UPDATE, mock.Anything, mock.Anything).Return(nil)
				controller.On("GetRoles", tt.params.Body.Roles[0]).Return(tt.existedRoles, nil)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.assignRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAssignRoleForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.AssignRoleParams{
				ID: "testUser",
				Body: authz.AssignRoleBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.assignRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAssignRoleInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleParams
		principal     *models.Principal
		getRolesErr   error
		assignErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error from assigning",
			params: authz.AssignRoleParams{
				ID: "testUser",
				Body: authz.AssignRoleBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			assignErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.AssignRoleParams{
				ID: "testUser",
				Body: authz.AssignRoleBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			getRolesErr:   fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, tt.getRolesErr)
			if tt.getRolesErr == nil {
				controller.On("AddRolesForUser", tt.params.ID, tt.params.Body.Roles).Return(tt.assignErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.assignRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
