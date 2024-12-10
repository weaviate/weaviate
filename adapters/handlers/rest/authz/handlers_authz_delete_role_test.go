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
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestDeleteRoleSuccess(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.DeleteRoleParams{
		ID: "roleToRemove",
	}
	authorizer.On("Authorize", principal, authorization.DELETE, authorization.Roles("roleToRemove")[0]).Return(nil)
	controller.On("DeleteRoles", params.ID).Return(nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.deleteRole(params, principal)
	parsed, ok := res.(*authz.DeleteRoleNoContent)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestDeleteRoleBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.DeleteRoleParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "update builtin role",
			params: authz.DeleteRoleParams{
				ID: authorization.BuiltInRoles[0],
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "you can not delete builtin role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			h := &authZHandlers{
				controller: controller,
				logger:     logger,
			}
			res := h.deleteRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.DeleteRoleBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestDeleteRoleForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.DeleteRoleParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.DeleteRoleParams{
				ID: "roleToRemove",
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

			authorizer.On("Authorize", tt.principal, authorization.DELETE, authorization.Roles(tt.params.ID)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.deleteRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.DeleteRoleForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestDeleteRoleInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.DeleteRoleParams
		principal     *models.Principal
		upsertErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "remove role error",
			params: authz.DeleteRoleParams{
				ID: "roleToRemove",
			},
			principal:     &models.Principal{Username: "user1"},
			upsertErr:     errors.New("remove error"),
			expectedError: "remove error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.DELETE, authorization.Roles(tt.params.ID)[0]).Return(nil)
			controller.On("DeleteRoles", tt.params.ID).Return(tt.upsertErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.deleteRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.DeleteRoleInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
