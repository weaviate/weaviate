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
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestRevokeRoleFromUserSuccess(t *testing.T) {
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
				ID: "user1",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"testRole"},
				},
			},
		},
		{
			name: "revoke another user not configured admin role",
			params: authz.RevokeRoleFromUserParams{
				ID: "user1",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"admin"},
				},
			},
			configuredAdmins: []string{"testUser"},
			principal:        &models.Principal{Username: "user1"},
		},
		{
			name: "revoke another user user not configured viewer role",
			params: authz.RevokeRoleFromUserParams{
				ID: "user1",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"viewer"},
				},
			},
			configuredViewers: []string{"testUser"},
			principal:         &models.Principal{Username: "user1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, nil)
			controller.On("RevokeRolesForUser", conv.PrefixUserName(tt.params.ID), tt.params.Body.Roles[0]).Return(nil)

			h := &authZHandlers{
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.APIKey{Enabled: true, Users: []string{"user1"}},
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
			principal: &models.Principal{Username: "user1"},
			params: authz.RevokeRoleFromGroupParams{
				ID: "user1",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"testRole"},
				},
			},
		},
		{
			name: "revoke another user not configured admin role",
			params: authz.RevokeRoleFromGroupParams{
				ID: "user1",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"admin"},
				},
			},
			configuredAdmins: []string{"testUser"},
			principal:        &models.Principal{Username: "user1"},
		},
		{
			name: "revoke another user user not configured viewer role",
			params: authz.RevokeRoleFromGroupParams{
				ID: "user1",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"viewer"},
				},
			},
			configuredViewers: []string{"testUser"},
			principal:         &models.Principal{Username: "user1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, nil)
			controller.On("RevokeRolesForUser", conv.PrefixGroupName(tt.params.ID), tt.params.Body.Roles[0]).Return(nil)

			h := &authZHandlers{
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.APIKey{Enabled: true, Users: []string{"user1"}},
				logger:         logger,
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
		admins        []string
		viewers       []string
	}

	tests := []testCase{
		{
			name: "empty role",
			params: authz.RevokeRoleFromUserParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to revoke is empty",
			existedRoles:  map[string][]authorization.Policy{},
		},
		{
			name: "revoke configured root role",
			params: authz.RevokeRoleFromUserParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"root"},
				},
			},
			callAuthZ:     false,
			admins:        []string{"testUser"},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "revoking: modifying 'root' role or changing its assignments is not allowed",
		},
		{
			name: "revoke configured viewer role",
			params: authz.RevokeRoleFromUserParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"viewer"},
				},
			},
			callAuthZ:     true,
			viewers:       []string{"testUser"},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "you can not revoke role viewer when configured via AUTHORIZATION_VIEWER_USERS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			if tt.callAuthZ {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				rbacconfig: rbacconf.Config{
					Admins:  tt.admins,
					Viewers: tt.viewers,
				},
				logger: logger,
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
		admins        []string
		viewers       []string
	}

	tests := []testCase{
		{
			name: "empty role",
			params: authz.RevokeRoleFromGroupParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{""},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "one or more of the roles you want to revoke is empty",
			existedRoles:  map[string][]authorization.Policy{},
		},
		{
			name: "revoke configured root role",
			params: authz.RevokeRoleFromGroupParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"root"},
				},
			},
			callAuthZ:     false,
			admins:        []string{"testUser"},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "revoking: modifying 'root' role or changing its assignments is not allowed",
		},
		{
			name: "revoke configured viewer role",
			params: authz.RevokeRoleFromGroupParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"viewer"},
				},
			},
			callAuthZ:     true,
			viewers:       []string{"testUser"},
			principal:     &models.Principal{Username: "user1"},
			expectedError: "you can not revoke configured role viewer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			if tt.callAuthZ {
				authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				rbacconfig: rbacconf.Config{
					Admins:  tt.admins,
					Viewers: tt.viewers,
				},
				logger: logger,
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
		callToGetRole bool
	}

	tests := []testCase{
		{
			name: "user not found",
			params: authz.RevokeRoleFromUserParams{
				ID: "user_not_exist",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"role1"},
				},
			},
			principal:    &models.Principal{Username: "user1"},
			existedRoles: map[string][]authorization.Policy{},
			existedUsers: []string{"user1"},
		},
		{
			name: "role not found",
			params: authz.RevokeRoleFromUserParams{
				ID: "user1",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"role1"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			existedRoles:  map[string][]authorization.Policy{},
			existedUsers:  []string{"user1"},
			callToGetRole: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, mock.Anything, mock.Anything).Return(nil)

			if tt.callToGetRole {
				controller.On("GetRoles", tt.params.Body.Roles[0]).Return(tt.existedRoles, nil)
			}

			h := &authZHandlers{
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.APIKey{Enabled: true, Users: tt.existedUsers},
				logger:         logger,
			}
			res := h.revokeRoleFromUser(tt.params, tt.principal)
			_, ok := res.(*authz.RevokeRoleFromUserNotFound)
			assert.True(t, ok)
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
				ID: "user1",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"role1"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			existedRoles:  map[string][]authorization.Policy{},
			existedUsers:  []string{"user1"},
			callToGetRole: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewAuthorizer(t)
			controller := mocks.NewController(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, mock.Anything, mock.Anything).Return(nil)

			if tt.callToGetRole {
				controller.On("GetRoles", tt.params.Body.Roles[0]).Return(tt.existedRoles, nil)
			}

			h := &authZHandlers{
				authorizer:     authorizer,
				controller:     controller,
				apiKeysConfigs: config.APIKey{Enabled: true, Users: tt.existedUsers},
				logger:         logger,
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
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.RevokeRoleFromUserParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromUserBody{
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
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
	}

	tests := []testCase{
		{
			name: "authorization error",
			params: authz.RevokeRoleFromGroupParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromGroupBody{
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
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

	tests := []testCase{
		{
			name: "internal server error from revoking",
			params: authz.RevokeRoleFromUserParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromUserBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			revokeErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.RevokeRoleFromUserParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromUserBody{
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, tt.getRolesErr)
			if tt.getRolesErr == nil {
				controller.On("RevokeRolesForUser", conv.PrefixUserName(tt.params.ID), tt.params.Body.Roles[0]).Return(tt.revokeErr)
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
				ID: "testUser",
				Body: authz.RevokeRoleFromGroupBody{
					Roles: []string{"testRole"},
				},
			},
			principal:     &models.Principal{Username: "user1"},
			revokeErr:     fmt.Errorf("internal server error"),
			expectedError: "internal server error",
		},
		{
			name: "internal server error from getting role",
			params: authz.RevokeRoleFromGroupParams{
				ID: "testUser",
				Body: authz.RevokeRoleFromGroupBody{
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

			authorizer.On("Authorize", tt.principal, authorization.UPDATE, authorization.Roles(tt.params.Body.Roles...)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, tt.getRolesErr)
			if tt.getRolesErr == nil {
				controller.On("RevokeRolesForUser", conv.PrefixGroupName(tt.params.ID), tt.params.Body.Roles[0]).Return(tt.revokeErr)
			}

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
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
