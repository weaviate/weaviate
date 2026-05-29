//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"fmt"
	"slices"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

	authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(params.ID)[0]).Return(nil)
	controller.On("GetRoles", params.Body.Roles[0]).Return(map[string][]authorization.Policy{params.Body.Roles[0]: {}}, nil)
	controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(params.ID, authentication.AuthType(params.Body.UserType)), params.Body.Roles).Return(nil)

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
			Roles:     []string{"testRole"},
			GroupType: models.GroupTypeOidc,
		},
	}

	authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(authentication.AuthType(params.Body.GroupType), params.ID)[0]).Return(nil)
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

// TestAssignEmptyRoleSkipsPlaceholderPolicy: empty roles surface from GetRoles
// as a single InternalPlaceHolder policy; the assign guard must skip it or no
// caller below root can assign an empty role.
func TestAssignEmptyRoleSkipsPlaceholderPolicy(t *testing.T) {
	emptyRolePolicies := map[string][]authorization.Policy{
		"emptyRole": {{Resource: conv.InternalPlaceHolder}},
	}

	t.Run("assign to user", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		controller := NewMockControllerAndGetUsers(t)
		logger, _ := test.NewNullLogger()

		userType := models.UserTypeInputDb
		principal := &models.Principal{Username: "user1"}
		params := authz.AssignRoleToUserParams{
			ID:          "user1",
			HTTPRequest: req,
			Body:        authz.AssignRoleToUserBody{Roles: []string{"emptyRole"}, UserType: userType},
		}

		authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(params.ID)[0]).Return(nil)
		controller.On("GetRoles", params.Body.Roles[0]).Return(emptyRolePolicies, nil)
		controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(params.ID, authentication.AuthType(params.Body.UserType)), params.Body.Roles).Return(nil)
		// AuthorizeSilent intentionally unmocked: the guard must skip the placeholder.

		h := &authZHandlers{
			authorizer:     authorizer,
			controller:     controller,
			apiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user1"}},
			logger:         logger,
		}
		res := h.assignRoleToUser(params, principal)
		_, ok := res.(*authz.AssignRoleToUserOK)
		assert.True(t, ok, "expected OK, got %T", res)
	})

	t.Run("assign to group", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		controller := NewMockControllerAndGetUsers(t)
		logger, _ := test.NewNullLogger()

		principal := &models.Principal{Username: "root-user"}
		params := authz.AssignRoleToGroupParams{
			ID:          "group1",
			HTTPRequest: req,
			Body:        authz.AssignRoleToGroupBody{Roles: []string{"emptyRole"}, GroupType: models.GroupTypeOidc},
		}

		authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(authentication.AuthType(params.Body.GroupType), params.ID)[0]).Return(nil)
		controller.On("GetRoles", params.Body.Roles[0]).Return(emptyRolePolicies, nil)
		controller.On("AddRolesForUser", conv.PrefixGroupName(params.ID), params.Body.Roles).Return(nil)

		h := &authZHandlers{
			authorizer: authorizer,
			controller: controller,
			logger:     logger,
			rbacconfig: rbacconf.Config{RootUsers: []string{"root-user"}},
		}
		res := h.assignRoleToGroup(params, principal)
		_, ok := res.(*authz.AssignRoleToGroupOK)
		assert.True(t, ok, "expected OK, got %T", res)
	})
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

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, mock.Anything, mock.Anything).Return(nil)

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
					Roles: []string{"role1"}, GroupType: models.GroupTypeOidc,
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

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(authentication.AuthType(tt.params.Body.GroupType), tt.params.ID)[0]).Return(nil)

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
				authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(tt.params.ID)[0]).Return(tt.authorizeErr)
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
					Roles:     []string{"testRole"},
					GroupType: models.GroupTypeOidc,
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
				Body:        authz.AssignRoleToGroupBody{Roles: []string{"some-role"}, GroupType: models.GroupTypeOidc},
			},
			principal:     &models.Principal{Username: "root-user"},
			expectedError: "assigning: cannot assign or revoke from root group root-group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(authentication.AuthType(tt.params.Body.GroupType), tt.params.ID)[0]).Return(tt.authorizeErr)

			h := &authZHandlers{
				authorizer: authorizer,
				controller: controller,
				logger:     logger,
				rbacconfig: rbacconf.Config{
					RootUsers:      []string{"root-user"},
					RootGroups:     []string{"root-group"},
					ReadOnlyGroups: []string{"viewer-root-group"},
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

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(tt.params.ID)[0]).Return(nil)
			controller.On("GetRoles", tt.params.Body.Roles[0]).Return(map[string][]authorization.Policy{tt.params.Body.Roles[0]: {}}, tt.getRolesErr)
			if tt.getRolesErr == nil {
				controller.On("GetUsers", "testUser").Return(map[string]*apikey.User{"testUser": {}}, nil)
				controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(tt.params.ID, authentication.AuthType(tt.params.Body.UserType)), tt.params.Body.Roles).Return(tt.assignErr)
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

func TestAssignRoleToUserBuiltInRoleNamespacesEnabled(t *testing.T) {
	type testCase struct {
		name              string
		role              string
		userID            string
		namespacesEnabled bool
		staticAPIKeyUsers []string
		wantForbidden     bool
	}

	tests := []testCase{
		{
			name:              "namespaces enabled, dynamic DB user, admin role allowed (narrowed)",
			role:              authorization.Admin,
			userID:            "customer1:alice",
			namespacesEnabled: true,
		},
		{
			name:              "namespaces enabled, dynamic DB user, viewer role allowed (narrowed)",
			role:              authorization.Viewer,
			userID:            "customer1:alice",
			namespacesEnabled: true,
		},
		{
			name:              "namespaces enabled, dynamic DB user, custom role allowed",
			role:              "customRole",
			userID:            "customer1:alice",
			namespacesEnabled: true,
		},
		{
			name:              "namespaces enabled, static API-key user, admin role allowed",
			role:              authorization.Admin,
			userID:            "static-user",
			namespacesEnabled: true,
			staticAPIKeyUsers: []string{"static-user"},
		},
		{
			name:              "namespaces disabled, dynamic DB user, admin role allowed",
			role:              authorization.Admin,
			userID:            "customer1:alice",
			namespacesEnabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			principal := &models.Principal{Username: "admin-user"}
			params := authz.AssignRoleToUserParams{
				ID:          tt.userID,
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles:    []string{tt.role},
					UserType: models.UserTypeInputDb,
				},
			}

			authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(tt.userID)[0]).Return(nil)
			controller.On("GetRoles", tt.role).Return(map[string][]authorization.Policy{tt.role: {}}, nil)
			isStatic := slices.Contains(tt.staticAPIKeyUsers, tt.userID)
			if !isStatic {
				controller.On("GetUsers", tt.userID).Return(map[string]*apikey.User{tt.userID: {}}, nil)
			}
			if !tt.wantForbidden {
				controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(tt.userID, authentication.AuthTypeDb), params.Body.Roles).Return(nil)
			}

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				apiKeysConfigs:    config.StaticAPIKey{Enabled: true, Users: tt.staticAPIKeyUsers},
				namespacesEnabled: tt.namespacesEnabled,
				logger:            logger,
			}
			res := h.assignRoleToUser(params, principal)
			if tt.wantForbidden {
				parsed, ok := res.(*authz.AssignRoleToUserForbidden)
				assert.True(t, ok, "expected Forbidden, got %T", res)
				if ok {
					assert.Contains(t, parsed.Payload.Error[0].Message, "reserved for global/operator principals")
				}
			} else {
				_, ok := res.(*authz.AssignRoleToUserOK)
				assert.True(t, ok, "expected OK, got %T", res)
			}
		})
	}
}

// TestAssignRoleToUserBuiltInRoleNamespacesEnabled_Allowed asserts that
// only Root/ReadOnly are blocked from API assignment on NS-enabled
// clusters. Admin/Viewer are API-assignable (narrowed at registration).
func TestAssignRoleToUserBuiltInRoleNamespacesEnabled_Allowed(t *testing.T) {
	type testCase struct {
		name             string
		role             string
		wantForbidden    bool
		expectedErrorMsg string
	}

	tests := []testCase{
		{
			name: "namespaced DB user, admin allowed (narrowed)",
			role: authorization.Admin,
		},
		{
			name: "namespaced DB user, viewer allowed (narrowed)",
			role: authorization.Viewer,
		},
		{
			name:             "namespaced DB user, root rejected by env-var guard",
			role:             authorization.Root,
			wantForbidden:    true,
			expectedErrorMsg: "modifying 'root' role or changing its assignments is not allowed",
		},
		{
			name:             "namespaced DB user, read-only rejected by env-var guard",
			role:             authorization.ReadOnly,
			wantForbidden:    true,
			expectedErrorMsg: "modifying 'read-only' role or changing its assignments is not allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			principal := &models.Principal{Username: "admin-user"}
			userID := "customer1:alice"
			params := authz.AssignRoleToUserParams{
				ID:          userID,
				HTTPRequest: req,
				Body: authz.AssignRoleToUserBody{
					Roles:    []string{tt.role},
					UserType: models.UserTypeInputDb,
				},
			}

			if !tt.wantForbidden {
				authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(userID)[0]).Return(nil)
				controller.On("GetRoles", tt.role).Return(map[string][]authorization.Policy{tt.role: {}}, nil)
				controller.On("GetUsers", userID).Return(map[string]*apikey.User{userID: {}}, nil)
				controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(userID, authentication.AuthTypeDb), params.Body.Roles).Return(nil)
			}

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				apiKeysConfigs:    config.StaticAPIKey{Enabled: true},
				namespacesEnabled: true,
				logger:            logger,
			}
			res := h.assignRoleToUser(params, principal)
			if tt.wantForbidden {
				parsed, ok := res.(*authz.AssignRoleToUserForbidden)
				assert.True(t, ok, "expected Forbidden, got %T", res)
				if ok && tt.expectedErrorMsg != "" {
					assert.Contains(t, parsed.Payload.Error[0].Message, tt.expectedErrorMsg)
				}
			} else {
				_, ok := res.(*authz.AssignRoleToUserOK)
				assert.True(t, ok, "expected OK, got %T", res)
			}
		})
	}
}

// TestUserIDNamespacePrefixRequiredOnNSEnabled asserts assign/revoke/read
// reject bare `params.ID` for OIDC and dynamic-DB users on NS-enabled
// clusters. Static API-key users pass through.
func TestUserIDNamespacePrefixRequiredOnNSEnabled(t *testing.T) {
	type op string
	const (
		opAssign       op = "assign"
		opRevoke       op = "revoke"
		opGetRolesUser op = "getRoles"
	)

	type wantStatus int
	const (
		wantBadRequest wantStatus = iota
		wantOK
	)

	tests := []struct {
		name              string
		operation         op
		namespacesEnabled bool
		userID            string
		userType          string
		staticAPIKeyUsers []string
		want              wantStatus
	}{
		{
			name:              "assign, NS-enabled, oidc, bare ID — 400",
			operation:         opAssign,
			namespacesEnabled: true,
			userID:            "alice",
			userType:          string(models.UserTypeInputOidc),
			want:              wantBadRequest,
		},
		{
			name:              "assign, NS-enabled, oidc, prefixed ID — 200",
			operation:         opAssign,
			namespacesEnabled: true,
			userID:            "customer1:alice",
			userType:          string(models.UserTypeInputOidc),
			want:              wantOK,
		},
		{
			name:              "assign, NS-disabled, oidc, bare ID — 200 (backward compat)",
			operation:         opAssign,
			namespacesEnabled: false,
			userID:            "alice",
			userType:          string(models.UserTypeInputOidc),
			want:              wantOK,
		},
		{
			name:              "assign, NS-enabled, db, bare ID — 400",
			operation:         opAssign,
			namespacesEnabled: true,
			userID:            "alice",
			userType:          string(models.UserTypeInputDb),
			want:              wantBadRequest,
		},
		{
			name:              "assign, NS-enabled, db, prefixed ID — 200",
			operation:         opAssign,
			namespacesEnabled: true,
			userID:            "customer1:alice",
			userType:          string(models.UserTypeInputDb),
			want:              wantOK,
		},
		{
			name:              "assign, NS-enabled, static API-key bare ID — 200 (intentionally global)",
			operation:         opAssign,
			namespacesEnabled: true,
			userID:            "static-user",
			userType:          string(models.UserTypeInputDb),
			staticAPIKeyUsers: []string{"static-user"},
			want:              wantOK,
		},
		{
			name:              "revoke, NS-enabled, oidc, prefixed ID — 200",
			operation:         opRevoke,
			namespacesEnabled: true,
			userID:            "customer1:alice",
			userType:          string(models.UserTypeInputOidc),
			want:              wantOK,
		},
		{
			name:              "revoke, NS-enabled, oidc, bare ID — 400",
			operation:         opRevoke,
			namespacesEnabled: true,
			userID:            "alice",
			userType:          string(models.UserTypeInputOidc),
			want:              wantBadRequest,
		},
		{
			name:              "revoke, NS-enabled, db, bare ID — 400",
			operation:         opRevoke,
			namespacesEnabled: true,
			userID:            "alice",
			userType:          string(models.UserTypeInputDb),
			want:              wantBadRequest,
		},
		{
			name:              "getRolesForUser, NS-enabled, oidc, bare ID — 400",
			operation:         opGetRolesUser,
			namespacesEnabled: true,
			userID:            "alice",
			userType:          string(models.UserTypeInputOidc),
			want:              wantBadRequest,
		},
		{
			name:              "getRolesForUser, NS-enabled, db, bare ID — 400",
			operation:         opGetRolesUser,
			namespacesEnabled: true,
			userID:            "alice",
			userType:          string(models.UserTypeInputDb),
			want:              wantBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			principal := &models.Principal{Username: "admin"}

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				apiKeysConfigs:    config.StaticAPIKey{Enabled: true, Users: tt.staticAPIKeyUsers},
				oidcConfigs:       config.OIDC{Enabled: true},
				namespacesEnabled: tt.namespacesEnabled,
				logger:            logger,
			}

			isStatic := slices.Contains(tt.staticAPIKeyUsers, tt.userID)

			switch tt.operation {
			case opAssign:
				if tt.want == wantOK {
					authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(tt.userID)[0]).Return(nil)
					controller.On("GetRoles", "customRole").Return(map[string][]authorization.Policy{"customRole": {}}, nil)
					if tt.userType == string(models.UserTypeInputDb) && !isStatic {
						controller.On("GetUsers", tt.userID).Return(map[string]*apikey.User{tt.userID: {}}, nil)
					}
					controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(tt.userID, authentication.AuthType(tt.userType)), []string{"customRole"}).Return(nil)
				}
				params := authz.AssignRoleToUserParams{
					ID:          tt.userID,
					HTTPRequest: req,
					Body: authz.AssignRoleToUserBody{
						Roles:    []string{"customRole"},
						UserType: models.UserTypeInput(tt.userType),
					},
				}
				res := h.assignRoleToUser(params, principal)
				switch tt.want {
				case wantBadRequest:
					br, ok := res.(*authz.AssignRoleToUserBadRequest)
					require.True(t, ok, "expected BadRequest, got %T", res)
					assert.Contains(t, br.Payload.Error[0].Message, "namespace-prefixed")
				case wantOK:
					_, ok := res.(*authz.AssignRoleToUserOK)
					assert.True(t, ok, "expected OK, got %T", res)
				}
			case opRevoke:
				if tt.want == wantOK {
					authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(tt.userID)[0]).Return(nil)
					controller.On("GetRoles", "customRole").Return(map[string][]authorization.Policy{"customRole": {}}, nil)
					if tt.userType == string(models.UserTypeInputDb) && !isStatic {
						controller.On("GetUsers", tt.userID).Return(map[string]*apikey.User{tt.userID: {}}, nil)
					}
					controller.On("RevokeRolesForUser", conv.UserNameWithTypeFromId(tt.userID, authentication.AuthType(tt.userType)), "customRole").Return(nil)
				}
				params := authz.RevokeRoleFromUserParams{
					ID:          tt.userID,
					HTTPRequest: req,
					Body: authz.RevokeRoleFromUserBody{
						Roles:    []string{"customRole"},
						UserType: models.UserTypeInput(tt.userType),
					},
				}
				res := h.revokeRoleFromUser(params, principal)
				switch tt.want {
				case wantBadRequest:
					br, ok := res.(*authz.RevokeRoleFromUserBadRequest)
					require.True(t, ok, "expected BadRequest, got %T", res)
					assert.Contains(t, br.Payload.Error[0].Message, "namespace-prefixed")
				case wantOK:
					_, ok := res.(*authz.RevokeRoleFromUserOK)
					assert.True(t, ok, "expected OK, got %T", res)
				}
			case opGetRolesUser:
				params := authz.GetRolesForUserParams{
					ID:          tt.userID,
					UserType:    tt.userType,
					HTTPRequest: req,
				}
				res := h.getRolesForUser(params, principal)
				if tt.want == wantBadRequest {
					br, ok := res.(*authz.GetRolesForUserBadRequest)
					require.True(t, ok, "expected BadRequest, got %T", res)
					assert.Contains(t, br.Payload.Error[0].Message, "namespace-prefixed")
				}
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
					Roles: []string{"testRole"}, GroupType: models.GroupTypeOidc,
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
					Roles: []string{"testRole"}, GroupType: models.GroupTypeOidc,
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

			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(authentication.AuthType(tt.params.Body.GroupType), tt.params.ID)[0]).Return(nil)
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

// TestAssignRoleToUser_Namespaces — namespaced short → 403-at-authz on
// the qualified resource (no AssignAndRevokeUsers grant); global op
// qualified → 200.
func TestAssignRoleToUser_Namespaces(t *testing.T) {
	userType := models.UserTypeInputDb
	roles := []string{"testRole"}

	tests := []struct {
		name             string
		userID           string
		principalNS      string
		isGlobalOperator bool
		authzKey         string
		wantStatus       any
	}{
		{
			name:        "namespaced caller short name no grant returns 403",
			userID:      "bob",
			principalNS: "customer1",
			authzKey:    "customer1:bob",
			wantStatus:  &authz.AssignRoleToUserForbidden{},
		},
		{
			name:             "global operator qualified passthrough succeeds",
			userID:           "customer1:bob",
			isGlobalOperator: true,
			authzKey:         "customer1:bob",
			wantStatus:       &authz.AssignRoleToUserOK{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{
				Namespace:        tt.principalNS,
				IsGlobalOperator: tt.isGlobalOperator,
			}
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			switch tt.wantStatus.(type) {
			case *authz.AssignRoleToUserForbidden:
				authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(tt.authzKey)[0]).Return(fmt.Errorf("not allowed"))
			case *authz.AssignRoleToUserOK:
				authorizer.On("Authorize", mock.Anything, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(tt.authzKey)[0]).Return(nil)
				controller.On("GetRoles", roles[0]).Return(map[string][]authorization.Policy{roles[0]: {}}, nil)
				controller.On("GetUsers", tt.authzKey).Return(map[string]*apikey.User{tt.authzKey: {}}, nil)
				controller.On("AddRolesForUser", conv.UserNameWithTypeFromId(tt.authzKey, authentication.AuthType(userType)), roles).Return(nil)
			}

			h := &authZHandlers{
				authorizer:        authorizer,
				controller:        controller,
				logger:            logger,
				namespacesEnabled: true,
			}
			res := h.assignRoleToUser(authz.AssignRoleToUserParams{
				ID:          tt.userID,
				HTTPRequest: req,
				Body:        authz.AssignRoleToUserBody{Roles: roles, UserType: userType},
			}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}
