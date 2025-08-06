//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestGetRolesForGroupSuccess(t *testing.T) {
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
	groupType := models.GroupTypeOidc
	returnedPolices := map[string][]authorization.Policy{
		"testRole": policies,
	}
	truep := true
	falseP := false
	tests := []struct {
		name        string
		params      authz.GetRolesForGroupParams
		principal   *models.Principal
		expectAuthz bool
	}{
		{
			name: "success",
			params: authz.GetRolesForGroupParams{
				ID:               "group1",
				GroupType:        string(groupType),
				IncludeFullRoles: &truep,
				HTTPRequest:      req,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: true,
		},
		{
			name: "success without roles",
			params: authz.GetRolesForGroupParams{
				ID:               "group1",
				GroupType:        string(groupType),
				IncludeFullRoles: &falseP,
				HTTPRequest:      req,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputDb},
			expectAuthz: true,
		},
		{
			name: "success for own group",
			params: authz.GetRolesForGroupParams{
				ID:               "group1",
				GroupType:        string(groupType),
				IncludeFullRoles: &truep,
				HTTPRequest:      req,
			},
			principal:   &models.Principal{Username: "user1", UserType: models.UserTypeInputOidc, Groups: []string{"group1", "group2"}},
			expectAuthz: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectAuthz {
				if tt.expectAuthz {
					authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Groups(tt.params.GroupType, tt.params.ID)[0]).Return(nil)
				}

				if *tt.params.IncludeFullRoles {
					authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(nil)
				}
			}
			controller.On("GetRolesForUserOrGroup", tt.params.ID, models.GroupTypeOidc, true).Return(returnedPolices, nil)
			// controller.On("GetUsers", tt.params.ID).Return(map[string]*apikey.User{"testUser": {}}, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForGroupOK)
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

func TestGetRolesForGroupForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRolesForGroupParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}
	truep := true
	userType := models.UserTypeInputOidc
	tests := []testCase{
		{
			name: "authorization error no access to role",
			params: authz.GetRolesForGroupParams{
				ID:               "testUser",
				GroupType:        string(userType),
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

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Groups(string(models.GroupTypeOidc), tt.params.ID)[0]).Return(nil)

			if tt.authorizeErr != nil {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH), authorization.Roles("testRole")[0]).Return(tt.authorizeErr)
			}
			controller.On("GetRolesForUserOrGroup", tt.params.ID, models.GroupTypeOidc, true).Return(returnedPolices, nil)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForGroupForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestGetRolesForGroupInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.GetRolesForGroupParams
		principal     *models.Principal
		getRolesErr   error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error",
			params: authz.GetRolesForGroupParams{
				ID:          "testGroup",
				GroupType:   string(models.GroupTypeOidc),
				HTTPRequest: req,
			},
			principal:     &models.Principal{Username: "user1", UserType: models.UserTypeInputOidc},
			getRolesErr:   fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Groups(string(models.GroupTypeOidc), tt.params.ID)[0]).Return(nil)
			controller.On("GetRolesForUserOrGroup", tt.params.ID, models.GroupTypeOidc, true).Return(nil, tt.getRolesErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.getRolesForGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.GetRolesForGroupInternalServerError)
			assert.True(t, ok)

			assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
		})
	}
}
