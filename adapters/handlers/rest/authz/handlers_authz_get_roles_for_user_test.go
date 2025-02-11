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
	tests := []struct {
		name        string
		params      authz.GetRolesForUserParams
		principal   *models.Principal
		expectAuthz bool
	}{
		{
			name: "success",
			params: authz.GetRolesForUserParams{
				ID: "testUser",
			},
			principal:   &models.Principal{Username: "user1"},
			expectAuthz: true,
		},
		{
			name: "success for own user",
			params: authz.GetRolesForUserParams{
				ID: "user1",
			},
			principal:   &models.Principal{Username: "user1"},
			expectAuthz: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectAuthz {
				authorizer.On("Authorize", principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(nil)
			}
			controller.On("GetRolesForUser", tt.params.ID).Return(returnedPolices, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForUser(tt.params, principal)
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
			name: "authorization error no access to role",
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

			returnedPolices := map[string][]authorization.Policy{
				"testRole": {
					{
						Resource: authorization.Collections("ABC")[0],
						Verb:     authorization.READ,
						Domain:   authorization.SchemaDomain,
					},
				},
			}

			authorizer.On("Authorize", tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
			if tt.authorizeErr != nil {
				authorizer.On("Authorize", tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
			}
			controller.On("GetRolesForUser", tt.params.ID).Return(returnedPolices, nil)

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
