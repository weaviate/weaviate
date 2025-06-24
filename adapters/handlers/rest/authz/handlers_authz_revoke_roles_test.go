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

func TestRevokeRoleFromUserSuccess(t *testing.T) {
	userType := models.UserTypeInputDb
	tests := []struct {
		name              string
		principal         *models.Principal
		params            authz.RevokeRoleFromUserParams
		configuredAdmins  []string
		configuredViewers []string
	}{
		{
			name:      "successful revocation",
			principal: &models.Principal{Username: "user1"},
			params: authz.RevokeRoleFromUserParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
					Roles:    []string{"testRole"},
					UserType: userType,
				},
			},
		},
		{
			name: "revoke another user not configured admin role",
			params: authz.RevokeRoleFromUserParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
					Roles:    []string{"admin"},
					UserType: userType,
				},
			},
			configuredAdmins: []string{"testUser"},
			principal:        &models.Principal{Username: "user1"},
		},
		{
			name: "revoke another user user not configured viewer role",
			params: authz.RevokeRoleFromUserParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
					Roles:    []string{"viewer"},
					UserType: userType,
				},
			},
			configuredViewers: []string{"testUser"},
			principal:         &models.Principal{Username: "user1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_ASSIGN_AND_REVOKE, authorization.Users(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, nil)
			controller.On("RevokeRolesForUser", conv.UserNameWithTypeFromId(tt.params.ID, tt.params.Body.UserType), tt.params.Body.Roles[0]).Return(nil)

			h := &authZHandlers{
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user1"}},
				logger:         logger,
			}
			res := h.revokeRoleFromUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromUserOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestRevokeRoleFromGroupSuccess(t *testing.T) {
	tests := []struct {
		name              string
		principal         *models.Principal
		params            authz.RevokeRoleFromGroupParams
		configuredAdmins  []string
		configuredViewers []string
	}{
		{
			name:      "successful revocation",
			principal: &models.Principal{Username: "root-user"},
			params: authz.RevokeRoleFromGroupParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"testRole"},
				},
			},
		},
		{
			name:      "successful revocation via root group",
			principal: &models.Principal{Username: "not-root-user", Groups: []string{"root-group"}},
			params: authz.RevokeRoleFromGroupParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"testRole"},
				},
			},
		},
		{
			name: "revoke another user not configured admin role",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"admin"},
				},
			},
			configuredAdmins: []string{"testUser"},
			principal:        &models.Principal{Username: "root-user"},
		},
		{
			name: "revoke another user user not configured viewer role",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"viewer"},
				},
			},
			configuredViewers: []string{"testUser"},
			principal:         &models.Principal{Username: "root-user"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.Body.Roles...)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, nil)
			controller.On("RevokeRolesForUser", conv.PrefixGroupName(tt.params.ID), tt.params.Body.Roles[0]).Return(nil)

			h := &authZHandlers{
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user1"}},
				logger:         logger,
				rbacconfig: rbacconf.Config{
					RootUsers: []string{"root-user"}, RootGroups: []string{"root-group"},
				},
			}
			res := h.revokeRoleFromGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromGroupOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestRevokeRoleFromUserBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromUserParams
		principal     *models.Principal
		expectedError string
		existedRoles  map[string][]authorization.Policy
		callAuthZ     bool
	}

	tests := []testCase{
		{
			name: "empty role",
			params: authz.RevokeRoleFromUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to revoke is empty",
			existedRoles:  map[string][]authorization.Policy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			if tt.callAuthZ {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.revokeRoleFromUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromUserBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRevokeRoleFromGroupBadRequest(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromGroupParams
		principal     *models.Principal
		expectedError string
		existedRoles  map[string][]authorization.Policy
		callAuthZ     bool
	}

	tests := []testCase{
		{
			name: "empty role",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to revoke is empty",
			existedRoles:  map[string][]authorization.Policy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			if tt.callAuthZ {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.revokeRoleFromGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromGroupBadRequest)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRevokeRoleFromUserOrUserNotFound(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromUserParams
		principal     *models.Principal
		existedRoles  map[string][]authorization.Policy
		existedUsers  []string
		callToGetUser bool
	}

	tests := []testCase{
		{
			name: "user not found",
			params: authz.RevokeRoleFromUserParams{
				ID:          "user_not_exist",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
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
			params: authz.RevokeRoleFromUserParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
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

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_ASSIGN_AND_REVOKE, authorization.Users(tt.params.ID)[0]).Return(nil)

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
			res := h.revokeRoleFromUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromUserNotFound)
			assert.True(t, ok)
			assert.Contains(t, parsed.Payload.Error[0].Message, "doesn't exist")
		})
	}
}

func TestRevokeRoleFromGroupOrUserNotFound(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromGroupParams
		principal     *models.Principal
		existedRoles  map[string][]authorization.Policy
		existedUsers  []string
		callToGetRole bool
	}

	tests := []testCase{
		{
			name: "role not found",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "user1",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"role1"},
				},
			},
			principal:     &models.Principal{Username: "root-user"},
			existedRoles:  map[string][]authorization.Policy{},
			existedUsers:  []string{"user1"},
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
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: tt.existedUsers},
				logger:         logger,
				rbacconfig: rbacconf.Config{
					RootUsers: []string{"root-user"},
				},
			}
			res := h.revokeRoleFromGroup(tt.params, tt.principal)
			_, ok := res.(*authz.RevokeRoleFromGroupNotFound)
			assert.True(t, ok)
		})
	}
}

func TestRevokeRoleFromUserForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromUserParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
		skipAuthZ     bool
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.RevokeRoleFromUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
		{
			name: "revoke configured root role",
			params: authz.RevokeRoleFromUserParams{
				ID:          "root-user",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"root"},
				},
			},
			skipAuthZ:     true,
			principal:     &models.Principal{Username: "user1"},
			expectedError: "revoking: modifying 'root' role or changing its assignments is not allowed",
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
			res := h.revokeRoleFromUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromUserForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRevokeRoleFromGroupForbidden(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromGroupParams
		principal     *models.Principal
		authorizeErr  error
		expectedError string
		skipAuthZ     bool
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1", Groups: []string{"testGroup"}},
			authorizeErr:  fmt.Errorf("authorization error"),
			expectedError: "authorization error",
		},
		{
			name: "revoke role from root group as root user",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "viewer-root-group",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"something"},
				},
			},
			principal:     &models.Principal{Username: "root-user"},
			expectedError: "revoking: cannot assign or revoke from root group",
		},
		{
			name: "revoke role from root group as non-root user",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "viewer-root-group",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"something"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "revoking: only root users can revoke roles from groups",
		},
		{
			name: "revoke configured root role",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"root"},
				},
			},
			skipAuthZ:     true,
			principal:     &models.Principal{Username: "user1"},
			expectedError: "revoking: modifying 'root' role or changing its assignments is not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			if !tt.skipAuthZ {
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(tt.params.Body.Roles...)[0]).Return(tt.authorizeErr)
			}

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
			res := h.revokeRoleFromGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromGroupForbidden)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRevokeRoleFromUserInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromUserParams
		principal     *models.Principal
		getRolesErr   error
		revokeErr     error
		expectedError string
	}
	userType := models.UserTypeInputDb
	tests := []testCase{
		{
			name: "internal server error from revoking",
			params: authz.RevokeRoleFromUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
					Roles:    []string{"testRole"},
					UserType: userType,
				},
			},
			principal:     &models.Principal{Username: "user1"},
			revokeErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.RevokeRoleFromUserParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromUserBody{
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
				controller.On("RevokeRolesForUser", conv.UserNameWithTypeFromId(tt.params.ID, tt.params.Body.UserType), tt.params.Body.Roles[0]).Return(tt.revokeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
			}
			res := h.revokeRoleFromUser(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromUserInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}

func TestRevokeRoleFromGroupInternalServerError(t *testing.T) {
	type testCase struct {
		name          string
		params        authz.RevokeRoleFromGroupParams
		principal     *models.Principal
		getRolesErr   error
		revokeErr     error
		expectedError string
	}

	tests := []testCase{
		{
			name: "internal server error from revoking",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "root-user"},
			revokeErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.RevokeRoleFromGroupParams{
				ID:          "testUser",
				HTTPRequest: req,
				Body: authz.RevokeRoleFromGroupBody{
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
				controller.On("RevokeRolesForUser", conv.PrefixGroupName(tt.params.ID), tt.params.Body.Roles[0]).Return(tt.revokeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{
					RootUsers: []string{"root-user"},
				},
			}
			res := h.revokeRoleFromGroup(tt.params, tt.principal)
			parsed, ok := res.(*authz.RevokeRoleFromGroupInternalServerError)
			assert.True(t, ok)

			if tt.expectedError != "" {
				assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedError)
			}
		})
	}
}
