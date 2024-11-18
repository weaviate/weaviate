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

func TestRevokeRoleSuccess(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.RevokeRoleParams{
		ID: "testUser",
		Body: authz.RevokeRoleBody{
			Roles: []string{"testRole"},
		},
	}

	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Roles(params.Body.Roles...)[0], authorization.Users(params.ID)[0]).Return(nil)
	controller.On("GetRoles", params.Body.Roles[0]).Return(map[string][]authorization.Policy{params.Body.Roles[0]: {}}, nil)
	controller.On("RevokeRolesForUser", params.ID, params.Body.Roles[0]).Return(nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.revokeRole(params, principal)
	parsed, ok := res.(*authz.RevokeRoleOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestRevokeRoleBadRequest(t *testing.T) {
	type testCase struct {
		name            string
		params          authz.RevokeRoleParams
		principal       *models.Principal
		expectedError   string
		existedRoles    map[string][]authorization.Policy
		noCallToGetRole bool
	}

	tests := []testCase{
		{
			name: "user id can not be empty",
			params: authz.RevokeRoleParams{
				ID: "",
				Body: authz.RevokeRoleBody{
					Roles: []string{"testRole"},
				},
			},
			principal:       &models.Principal{Username: "user1"},
			expectedError:   "user id can not be empty",
			noCallToGetRole: true,
		},
		{
			name: "empty role",
			params: authz.RevokeRoleParams{
				ID: "testUser",
				Body: authz.RevokeRoleBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to revoke doesn't exist",
			existedRoles:  map[string][]authorization.Policy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, mock.Anything, mock.Anything).Return(nil)
			if !tt.noCallToGetRole {
				controller.On("GetRoles", tt.params.Body.Roles[0]).Return(tt.existedRoles, nil)
			}
			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.revokeRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRevokeRoleForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.RevokeRoleParams{
				ID: "testUser",
				Body: authz.RevokeRoleBody{
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0], authorization.Users(tt.params.ID)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.revokeRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRevokeRoleInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleParams
		principal     *models.Principal
		getRolesErr   error
		revokeErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error from revoking",
			params: authz.RevokeRoleParams{
				ID: "testUser",
				Body: authz.RevokeRoleBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			revokeErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.RevokeRoleParams{
				ID: "testUser",
				Body: authz.RevokeRoleBody{
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0], authorization.Users(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, tt.getRolesErr)
			if tt.getRolesErr == nil {
				controller.On("RevokeRolesForUser", tt.params.ID, tt.params.Body.Roles[0]).Return(tt.revokeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.revokeRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
