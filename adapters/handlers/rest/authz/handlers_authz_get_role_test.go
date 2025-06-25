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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestGetRoleSuccess(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.GetRoleParams{
		ID:          "testRole",
		HTTPRequest: req,
	}

	policies := []authorization.Policy{
		{
			Resource: authorization.Collections("ABC")[0],
			Verb:     authorization.READ,
			Domain:   authorization.SchemaDomain,
		},
	}

	expectedPermissions, err := conv.PoliciesToPermission(policies...)
	assert.Nil(t, err)

	returnedPolices := map[string][]authorization.Policy{
		"testRole": policies,
	}
	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles(params.ID)[0]).Return(nil)
	controller.On("GetRoles", params.ID).Return(returnedPolices, nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.getRole(params, principal)
	parsed, ok := res.(*authz.GetRoleOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	assert.Equal(t, params.ID, *parsed.Payload.Name)
	assert.Equal(t, expectedPermissions, parsed.Payload.Permissions)
}

func TestGetRoleForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRoleParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.GetRoleParams{
				HTTPRequest: req,
				ID:          "testRole",
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.ID)[0]).Return(tt.authorizeErr)
			if tt.authorizeErr != nil {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH), authorization.Roles(tt.params.ID)[0]).Return(tt.authorizeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRoleForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestGetRoleNotFound(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRoleParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "role not found",
			params: authz.GetRoleParams{
				HTTPRequest: req,
				ID:          "nonExistentRole",
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.ID).Return(map[string][]authorization.Policy{}, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRole(tt.params, tt.principal)
			_, ok := res.(*authz.GetRoleNotFound)
			assert.True(t, ok)
		})
	}
}

func TestGetRoleInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRoleParams
		principal     *models.Principal
		getRolesErr   error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error from getting role",
			params: authz.GetRoleParams{
				HTTPRequest: req,
				ID:          "testRole",
			},
			principal:     &models.Principal{Username: "user1"},
			getRolesErr:   fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			policies := []authorization.Policy{
				{
					Resource: authorization.Collections("ABC")[0],
					Verb:     authorization.READ,
					Domain:   authorization.SchemaDomain,
				},
			}

			returnedPolices := map[string][]authorization.Policy{
				"testRole": policies,
			}

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			controller.On("GetRoles", tt.params.ID).Return(returnedPolices, tt.getRolesErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRoleInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
