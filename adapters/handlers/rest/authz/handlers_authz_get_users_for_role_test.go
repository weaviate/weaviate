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
	"errors"
	"fmt"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/stretchr/testify/mock"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
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
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeDb, false).Return(expectedUsers, nil)
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeOIDC, false).Return(expectedUsers, nil)
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

// TestGetUsersForRoleDeSlotsGlobalOIDC pins that a global OIDC subject stored
// with the empty-namespace slot (":carol") is de-slotted to "carol" in the
// response on an NS-enabled cluster.
func TestGetUsersForRoleDeSlotsGlobalOIDC(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "op", UserType: models.UserTypeInputOidc, IsGlobalOperator: true}
	params := authz.GetUsersForRoleParams{ID: "viewer", HTTPRequest: req}

	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()[0]).Return(nil).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, principal, authorization.READ, mock.Anything).Return(nil).Maybe()
	controller.On("GetRoles", params.ID).Return(map[string][]authorization.Policy{params.ID: {collPolicy(authorization.READ, "Movies")}}, nil).Maybe()
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeOIDC, false).Return([]string{":carol"}, nil)
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeDb, false).Return([]string{}, nil)
	controller.On("GetUsers").Return(map[string]apikey.UserView{}, nil).Maybe()

	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
	res := h.getUsersForRole(params, principal)
	parsed, ok := res.(*authz.GetUsersForRoleOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	assert.Equal(t, "carol", parsed.Payload[0].UserID)
	assert.Equal(t, models.UserTypeOutputOidc, *parsed.Payload[0].UserType)
}

// TestGetUsersForRoleKeepsColonIDWhenNamespacesDisabled pins that the de-slot
// only runs on NS-enabled clusters: with namespaces off, a stored OIDC id that
// happens to start with ':' is returned verbatim, not stripped to a bare name.
func TestGetUsersForRoleKeepsColonIDWhenNamespacesDisabled(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "op", UserType: models.UserTypeInputOidc}
	params := authz.GetUsersForRoleParams{ID: "viewer", HTTPRequest: req}

	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles(params.ID)[0]).Return(nil).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, principal, authorization.READ, mock.Anything).Return(nil).Maybe()
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeOIDC, false).Return([]string{":carol"}, nil)
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeDb, false).Return([]string{}, nil)
	controller.On("GetUsers").Return(map[string]apikey.UserView{}, nil).Maybe()

	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: false}
	res := h.getUsersForRole(params, principal)
	parsed, ok := res.(*authz.GetUsersForRoleOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	assert.Equal(t, ":carol", parsed.Payload[0].UserID)
	assert.Equal(t, models.UserTypeOutputOidc, *parsed.Payload[0].UserType)
}

// TestGetUsersForRoleSelfInclusionGlobalOIDC pins that the slot is stripped
// before the self-check, so a global OIDC caller stored as ":carol" matches its
// own principal.Username and appears in its role's member list even without an
// explicit read on its own user id. AuthorizeSilent is wired to deny, so
// inclusion can only come from the self-check; if de-slotting ran after it, the
// caller would silently drop from the list and this test would fail.
func TestGetUsersForRoleSelfInclusionGlobalOIDC(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)
	logger, _ := test.NewNullLogger()

	principal := &models.Principal{Username: "carol", UserType: models.UserTypeInputOidc, IsGlobalOperator: true}
	params := authz.GetUsersForRoleParams{ID: "viewer", HTTPRequest: req}

	authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()[0]).Return(nil).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, principal, authorization.READ, mock.Anything).Return(errors.New("no read on user")).Maybe()
	controller.On("GetRoles", params.ID).Return(map[string][]authorization.Policy{params.ID: {collPolicy(authorization.READ, "Movies")}}, nil).Maybe()
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeOIDC, false).Return([]string{":carol"}, nil)
	controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeDb, false).Return([]string{}, nil)
	controller.On("GetUsers").Return(map[string]apikey.UserView{}, nil).Maybe()

	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
	res := h.getUsersForRole(params, principal)
	parsed, ok := res.(*authz.GetUsersForRoleOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	assert.Equal(t, "carol", parsed.Payload[0].UserID)
	assert.Equal(t, models.UserTypeOutputOidc, *parsed.Payload[0].UserType)
}

// TestGetUsersForRoleSlotAwareSelfInclusion pins that self-inclusion is
// slot-aware. After de-slotting, a global OIDC subject (":customer1:carol") and a
// namespaced one ("customer1:carol") share a display id but are different
// subjects. A global caller must self-include only its own slotted subject, not a
// namespaced subject of the same-looking id; AuthorizeSilent is wired to deny, so
// inclusion can only come from the self-check.
func TestGetUsersForRoleSlotAwareSelfInclusion(t *testing.T) {
	tests := []struct {
		name       string
		callerType models.UserTypeInput
		storedOIDC []string // OIDC subjects stored on the role
		storedDB   []string // DB subjects stored on the role
		wantUsers  []string
	}{
		{
			name:       "namespaced member is not self for a global colon-name caller",
			callerType: models.UserTypeInputOidc,
			storedOIDC: []string{"customer1:carol"},
			wantUsers:  nil,
		},
		{
			name:       "own global subject stays self-included despite denied read",
			callerType: models.UserTypeInputOidc,
			storedOIDC: []string{":customer1:carol"},
			wantUsers:  []string{"customer1:carol"},
		},
		{
			// A DB caller must not self-match an OIDC member of the same name:
			// with the read denied, the cross-type member stays hidden.
			name:       "same-name oidc member is not self for a db caller",
			callerType: models.UserTypeInputDb,
			storedOIDC: []string{"customer1:carol"},
			wantUsers:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			controller := NewMockControllerAndGetUsers(t)
			logger, _ := test.NewNullLogger()

			principal := &models.Principal{Username: "customer1:carol", UserType: tt.callerType, IsGlobalOperator: true}
			params := authz.GetUsersForRoleParams{ID: "viewer", HTTPRequest: req}

			authorizer.On("Authorize", mock.Anything, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()[0]).Return(nil).Maybe()
			authorizer.On("AuthorizeSilent", mock.Anything, principal, authorization.READ, mock.Anything).Return(errors.New("no read on user")).Maybe()
			controller.On("GetRoles", params.ID).Return(map[string][]authorization.Policy{params.ID: {collPolicy(authorization.READ, "Movies")}}, nil).Maybe()
			controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeOIDC, false).Return(tt.storedOIDC, nil)
			controller.On("GetUsersOrGroupForRole", params.ID, authentication.AuthTypeDb, false).Return(tt.storedDB, nil)
			controller.On("GetUsers").Return(map[string]apikey.UserView{}, nil).Maybe()

			h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
			res := h.getUsersForRole(params, principal)
			parsed, ok := res.(*authz.GetUsersForRoleOK)
			require.True(t, ok, "got %T", res)

			got := make([]string, len(parsed.Payload))
			for i, p := range parsed.Payload {
				got[i] = p.UserID
			}
			assert.ElementsMatch(t, tt.wantUsers, got)
		})
	}
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

			controller.On("GetUsersOrGroupForRole", tt.params.ID, authentication.AuthTypeOIDC, false).Return(nil, tt.getUsersErr)

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
