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

	"github.com/stretchr/testify/mock"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestGetUsersForRoleSuccess(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "user1"}
	params := authz.GetUsersForRoleParams{
		ID:          "testuser",
		HTTPRequest: req,
	}

	expectedUsers := []string{"user1", "user2"}
	expectedResponse := []*authz.GetUsersForRoleOKBodyItems0{
		{UserID: expectedUsers[0], UserType: models.NewUserTypeOutput(models.UserTypeOutputOidc)},
		{UserID: expectedUsers[1], UserType: models.NewUserTypeOutput(models.UserTypeOutputOidc)},
		{UserID: expectedUsers[0], UserType: models.NewUserTypeOutput(models.UserTypeOutputDbEnvUser)},
		{UserID: expectedUsers[1], UserType: models.NewUserTypeOutput(models.UserTypeOutputDbEnvUser)},
	}

	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles(params.ID)[0]).Return(nil)
	authorizer.On("AuthorizeSilent", mock.Anything, principal, authorization.READ, authorization.Users(expectedUsers...)[1]).Return(nil)
	controller.On("GetUsersForRole", params.ID, models.UserTypeInputDb).Return(expectedUsers, nil)
	controller.On("GetUsersForRole", params.ID, models.UserTypeInputOidc).Return(expectedUsers, nil)
	controller.On("GetUsers", expectedUsers[0], expectedUsers[1]).Return(nil, nil)

	h := &authZHandlers{
		authorizer: authorizer,
		controller: controller,
		logger:     logger,
	}
	res := h.getUsersForRole(params, principal)
	parsed, ok := res.(*authz.GetUsersForRoleOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	assert.Equal(t, expectedResponse, parsed.Payload)
}

func TestGetUsersForRoleForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetUsersForRoleParams
		principal     *models.Principal
		authorizeErr  error
		skipAuthZ     bool
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.GetUsersForRoleParams{
				ID:          "testRole",
				HTTPRequest: req,
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
		{
			name: "root",
			params: authz.GetUsersForRoleParams{
				ID:          "root",
				HTTPRequest: req,
			},
			skipAuthZ:     true,
			principal:     &models.Principal{Username: "user1"},
			expectedError: "modifying 'root' role or changing its assignments is not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			if !tt.skipAuthZ {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.ID)[0]).Return(tt.authorizeErr)
				if tt.authorizeErr != nil {
					authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH), authorization.Roles(tt.params.ID)[0]).Return(tt.authorizeErr)
				}
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getUsersForRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetUsersForRoleForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestGetUsersForRoleInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetUsersForRoleParams
		principal     *models.Principal
		getUsersErr   error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error",
			params: authz.GetUsersForRoleParams{
				ID:          "testRole",
				HTTPRequest: req,
			},
			principal:     &models.Principal{Username: "user1"},
			getUsersErr:   fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.ID)[0]).Return(nil)

			controller.On("GetUsersForRole", tt.params.ID, models.UserTypeInputOidc).Return(nil, tt.getUsersErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getUsersForRole(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetUsersForRoleInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
