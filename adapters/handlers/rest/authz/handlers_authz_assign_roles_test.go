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

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestAssignRoleToUserSuccess(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	userType := models.UserTypeInputDb
	principal := &models.Principal{Username: "user1"}
	params := authz.AssignRoleToUserParams{
		ID:          "user1",
		HTTPRequest: req,
		Body: authz.AssignRoleToUserBody{
			Roles:    []string{"testRole"},
			UserType: userType,
		},
	}

	authorizer.On("Authorize", mock.Anything, principal, authorization.USER_ASSIGN_AND_REVOKE, authorization.Users(params.ID)[0]).Return(nil)
	controller.On("GetRoles", params.Body.Roles[0]).Return(map[string][]authorization.Policy{params.Body.Roles[0]: {}}, nil)
	controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(params.ID, params.Body.UserType), params.Body.Roles).Return(nil)

	h := &authZHandlers{
		authorizer:     authorizer,
		controller:     controller,
		apiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user1"}},
		logger:         logger,
	}
	res := h.assignRoleToUser(params, principal)
	parsed, ok := res.(*authz.AssignRoleToUserOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestAssignRoleToGroupSuccess(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "root-user"}
	params := authz.AssignRoleToGroupParams{
		ID:          "group1",
		HTTPRequest: req,
		Body: authz.AssignRoleToGroupBody{
			Roles: []string{"testRole"},
		},
	}

	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(params.Body.Roles...)[0]).Return(nil)
	controller.On("GetRoles", params.Body.Roles[0]).Return(map[string][]authorization.Policy{params.Body.Roles[0]: {}}, nil)
	controller.On("AddRolesForUser", conv.PrefixGroupName(params.ID), params.Body.Roles).Return(nil)

	h := &authZHandlers{
		authorizer:     authorizer,
		controller:     controller,
		apiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user1"}},
		logger:         logger,
		rbacconfig: rbacconf.Config{
			RootUsers: []string{"root-user"},
		},
	}
	res := h.assignRoleToGroup(params, principal)
	parsed, ok := res.(*authz.AssignRoleToGroupOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestAssignRoleToUserOrUserNotFound(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToUserParams
		principal     *models.Principal
		existedRoles  map[string][]authorization.Policy
		existedUsers  []string
		callToGetUser bool
	}

	tests := []testCase{
		{
			name: "user not found",
			params: authz.AssignRoleToUserParams{
				ID:          "user_not_exist",
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles: []string{"role1"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			existedRoles:  map[string][]authorization.Policy{"role1": {}},
			existedUsers:  []string{"user1"},
			callToGetUser: true,
		},
		{
			name: "role not found",
			params: authz.AssignRoleToUserParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles: []string{"role1"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			existedRoles:  map[string][]authorization.Policy{},
			existedUsers:  []string{"user1"},
			callToGetUser: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_ASSIGN_AND_REVOKE, mock.Anything, mock.Anything).Return(nil)

			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(tt.existedRoles, nil)
			if tt.callToGetUser {
				controller.On("GetUsers", tt.params.ID).Return(nil, nil)
			}

			h := &authZHandlers{
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: tt.existedUsers},
				logger:         logger,
			}
			res := h.assignRoleToUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleToUserNotFound)
			assert.True(t, ok)
			assert.Contains(t, parsed.Payload.Error[0].Message, "doesn't exist")
		})
	}
}

func TestAssignRoleToGroupOrUserNotFound(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToGroupParams
		principal     *models.Principal
		existedRoles  map[string][]authorization.Policy
		callToGetRole bool
	}

	tests := []testCase{
		{
			name: "role not found",
			params: authz.AssignRoleToGroupParams{
				ID:          "group1",
				HTTPRequest: req,
				Body: authz.AssignRoleToGroupBody{
					Roles: []string{"role1"},
				},
			},
			principal:     &models.Principal{Username: "root-user"},
			existedRoles:  map[string][]authorization.Policy{},
			callToGetRole: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), mock.Anything, mock.Anything).Return(nil)

			if tt.callToGetRole {
				controller.On("GetRoles", tt.params.Body.Roles[0]).Return(tt.existedRoles, nil)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{
					RootUsers: []string{"root-user"},
				},
			}
			res := h.assignRoleToGroup(tt.params, tt.principal)
			_, ok := res.(*authz.AssignRoleToGroupNotFound)
			assert.True(t, ok)
		})
	}
}

func TestAssignRoleToUserBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToUserParams
		principal     *models.Principal
		expectedError string
	}

	tests := []testCase{
		{
			name: "empty role",
			params: authz.AssignRoleToUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to assign is empty",
		},
		{
			name: "no roles",
			params: authz.AssignRoleToUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body:        authz.AssignRoleToUserBody{},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "roles can not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.assignRoleToUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleToUserBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAssignRoleToGroupBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToGroupParams
		principal     *models.Principal
		expectedError string
		callAuthZ     bool
	}

	tests := []testCase{
		{
			name: "empty role",
			params: authz.AssignRoleToGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToGroupBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to assign is empty",
		},
		{
			name: "no roles",
			params: authz.AssignRoleToGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body:        authz.AssignRoleToGroupBody{},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "roles can not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			if tt.callAuthZ {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			}

			res := h.assignRoleToGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleToGroupBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAssignRoleToUserForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToUserParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
		skipAuthZ     bool
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.AssignRoleToUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
		{
			name: "root role",
			params: authz.AssignRoleToUserParams{
				ID:          "someuser",
				HTTPRequest: req,
				Body:        authz.AssignRoleToUserBody{Roles: []string{"root"}},
			},
			skipAuthZ:     true,
			principal:     &models.Principal{Username: "user1"},
			expectedError: "assigning: modifying 'root' role or changing its assignments is not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			if !tt.skipAuthZ {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_ASSIGN_AND_REVOKE, authorization.Users(tt.params.ID)[0]).Return(tt.authorizeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{
					RootUsers: []string{"root-user"},
				},
			}
			res := h.assignRoleToUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleToUserForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAssignRoleToGroupForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToGroupParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.AssignRoleToGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToGroupBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
		{
			name: "root group",
			params: authz.AssignRoleToGroupParams{
				ID:          "root-group",
				HTTPRequest: req,
				Body:        authz.AssignRoleToGroupBody{Roles: []string{"some-role"}},
			},
			principal:     &models.Principal{Username: "root-user"},
			expectedError: "assigning: cannot assign or revoke from root group root-group",
		},
		{
			name: "viewer root group",
			params: authz.AssignRoleToGroupParams{
				ID:          "viewer-root-group",
				HTTPRequest: req,
				Body:        authz.AssignRoleToGroupBody{Roles: []string{"some-role"}},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "assigning: only root users can assign roles to groups",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.Body.Roles...)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{
					RootUsers:    []string{"root-user"},
					RootGroups:   []string{"root-group"},
					ViewerGroups: []string{"viewer-root-group"},
				},
			}
			res := h.assignRoleToGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleToGroupForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAssignRoleToUserInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToUserParams
		principal     *models.Principal
		getRolesErr   error
		assignErr     error
		expectedError string
	}

	userType := models.UserTypeInputDb

	tests := []testCase{
		{
			name: "internal server error from assigning",
			params: authz.AssignRoleToUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles:    []string{"testRole"},
					UserType: userType,
				},
			},
			principal:     &models.Principal{Username: "user1"},
			assignErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.AssignRoleToUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles:    []string{"testRole"},
					UserType: userType,
				},
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

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_ASSIGN_AND_REVOKE, authorization.Users(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, tt.getRolesErr)
			if tt.getRolesErr == nil {
				controller.On("GetUsers", "testUser").Return(map[string]*apikey.User{"testUser": {}}, nil)
				controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(tt.params.ID, tt.params.Body.UserType), tt.params.Body.Roles).Return(tt.assignErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.assignRoleToUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleToUserInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestAssignRoleToGroupInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.AssignRoleToGroupParams
		principal     *models.Principal
		getRolesErr   error
		assignErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error from assigning",
			params: authz.AssignRoleToGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToGroupBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "root-user"},
			assignErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.AssignRoleToGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.AssignRoleToGroupBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "root-user"},
			getRolesErr:   fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.Body.Roles...)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, tt.getRolesErr)
			if tt.getRolesErr == nil {
				controller.On("AddRolesForUser", conv.PrefixGroupName(tt.params.ID), tt.params.Body.Roles).Return(tt.assignErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{
					RootUsers: []string{"root-user"},
				},
			}
			res := h.assignRoleToGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.AssignRoleToGroupInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
