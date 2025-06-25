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

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestGetRolesForUserSuccess(t *testing.T) {
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
	userType := models.UserTypeInputDb
	returnedPolices := map[string][]authorization.Policy{
		"testRole": policies,
	}
	truep := true
	falseP := false
	tests := []struct {
		name        string
		params      authz.GetRolesForUserParams
		principal   *models.Principal
		expectAuthz bool
	}{
		{
			name: "success",
			params: authz.GetRolesForUserParams{
				ID:               "testUser",
				UserType:         string(userType),
				HTTPRequest:      req,
				IncludeFullRoles: &truep,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: true,
		},
		{
			name: "success without roles",
			params: authz.GetRolesForUserParams{
				ID:               "testUser",
				UserType:         string(userType),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: true,
		},
		{
			name: "success for own user",
			params: authz.GetRolesForUserParams{
				ID:               "user1",
				UserType:         string(userType),
				IncludeFullRoles: &truep,
				HTTPRequest:      req,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectAuthz {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users(tt.params.ID)[0]).Return(nil)
				if *tt.params.IncludeFullRoles {
					authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(nil)
				}
			}
			controller.On("GetRolesForUser", tt.params.ID, models.UserTypeInputDb).Return(returnedPolices, nil)
			controller.On("GetUsers", tt.params.ID).Return(map[string]*apikey.User{"testUser": {}}, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForUser(tt.params, tt.principal)
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
			if *tt.params.IncludeFullRoles {
				assert.Equal(t, expectedRoles, parsed.Payload)
			} else {
				assert.Nil(t, parsed.Payload[0].Permissions)
			}
			assert.Equal(t, *roles[0].Name, *parsed.Payload[0].Name)
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
	truep := true
	userType := models.UserTypeInputDb
	tests := []testCase{
		{
			name: "authorization error no access to role",
			params: authz.GetRolesForUserParams{
				ID:               "testUser",
				UserType:         string(userType),
				IncludeFullRoles: &truep,
				HTTPRequest:      req,
			},
			principal:     &models.Principal{Username: "user1", UserType: userType},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			returnedPolices := map[string][]authorization.Policy{
				"testRole": {
					{
						Resource: authorization.Collections("ABC")[0],
						Verb:     authorization.READ,
						Domain:   authorization.SchemaDomain,
					},
				},
			}
			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users(tt.params.ID)[0]).Return(nil)

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
			if tt.authorizeErr != nil {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
			}
			controller.On("GetRolesForUser", tt.params.ID, userType).Return(returnedPolices, nil)
			controller.On("GetUsers", tt.params.ID).Return(map[string]*apikey.User{tt.params.ID: {}}, nil)
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

	userType := models.UserTypeInputDb
	tests := []testCase{
		{
			name: "internal server error",
			params: authz.GetRolesForUserParams{
				ID:          "testUser",
				UserType:    string(userType),
				HTTPRequest: req,
			},
			principal:     &models.Principal{Username: "user1", UserType: userType},
			getRolesErr:   fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users(tt.params.ID)[0]).Return(nil)
			controller.On("GetRolesForUser", tt.params.ID, userType).Return(nil, tt.getRolesErr)
			controller.On("GetUsers", tt.params.ID).Return(map[string]*apikey.User{tt.params.ID: {}}, nil)

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

func TestSortRolesByName(t *testing.T) {
	tests := []struct {
		name     string
		input    []*models.Role
		expected []*models.Role
	}{
		{
			name: "already sorted",
			input: []*models.Role{
				{Name: String("admin")},
				{Name: String("editor")},
				{Name: String("user")},
			},
			expected: []*models.Role{
				{Name: String("admin")},
				{Name: String("editor")},
				{Name: String("user")},
			},
		},
		{
			name: "unsorted",
			input: []*models.Role{
				{Name: String("user")},
				{Name: String("admin")},
				{Name: String("editor")},
			},
			expected: []*models.Role{
				{Name: String("admin")},
				{Name: String("editor")},
				{Name: String("user")},
			},
		},
		{
			name: "same name",
			input: []*models.Role{
				{Name: String("admin")},
				{Name: String("admin")},
				{Name: String("editor")},
			},
			expected: []*models.Role{
				{Name: String("admin")},
				{Name: String("admin")},
				{Name: String("editor")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sortByName(tt.input)
			assert.Equal(t, tt.expected, tt.input)
		})
	}
}
