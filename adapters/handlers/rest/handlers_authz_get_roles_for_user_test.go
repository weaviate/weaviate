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
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestGetRolesForUserSuccess(t *testing.T) {
	authorizer := mocks.NewAuthorizer(t)
	controller := mocks.NewController(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.GetRolesForUserParams{
		ID: "testUser",
	}

	policies := []authorization.Policy{
		{
			Resource: authorization.Collections("ABC")[0],
			Verb:     authorization.READ,
			Domain:   authorization.CollectionsDomain,
		},
	}

	returnedPolices := map[string][]authorization.Policy{
		"testRole": policies,
	}

	authorizer.On("Authorize", principal, authorization.READ, authorization.Users(params.ID)[0]).Return(nil)
	controller.On("GetRolesForUser", params.ID).Return(returnedPolices, nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.getRolesForUser(params, principal)
	parsed, ok := res.(*authz.GetRolesForUserOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)

	permissions, err := conv.PoliciesToPermission(policies...)
	assert.Nil(t, err)

	roles := []*models.Role{
		{
			Name:        String("testRole"),
			Permissions: permissions,
		},
	}
	expectedRoles := models.RolesListResponse(roles)
	assert.Equal(t, expectedRoles, parsed.Payload)
}

func TestGetRolesForUserBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRolesForUserParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "role name is required",
			params: authz.GetRolesForUserParams{
				ID: "",
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "role name is required",
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
			res := h.getRolesForUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForUserBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestGetRolesForUserForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRolesForUserParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.GetRolesForUserParams{
				ID: "testUser",
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

			authorizer.On("Authorize", tt.principal, authorization.READ, authorization.Users(tt.params.ID)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForUserForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestGetRolesForUserInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRolesForUserParams
		principal     *models.Principal
		getRolesErr   error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error",
			params: authz.GetRolesForUserParams{
				ID: "testUser",
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

			authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			controller.On("GetRolesForUser", tt.params.ID).Return(nil, tt.getRolesErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForUserInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
