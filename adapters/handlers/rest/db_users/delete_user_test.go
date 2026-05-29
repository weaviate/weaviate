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

package db_users

import (
	"errors"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestDeleteSuccess(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users("user")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetRolesForUserOrGroup", "user", authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{"role": {}}, nil)
	dynUser.On("RevokeRolesForUser", conv.UserNameWithTypeFromId("user", authentication.AuthType(models.UserTypeInputDb)), "role").Return(nil)
	dynUser.On("DeleteUser", mock.Anything, "user").Return(nil)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {}}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user", HTTPRequest: req}, principal)
	parsed, ok := res.(*users.DeleteUserNoContent)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestDeleteForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeleteUserForbidden)
	assert.True(t, ok)
}

func TestDeleteUnprocessableEntityStaticUser(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users("user")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,

		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user"}, AllowedKeys: []string{"key"}},
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeleteUserUnprocessableEntity)
	assert.True(t, ok)
}

func TestDeleteUnprocessableEntitySelf(t *testing.T) {
	user := "myself"
	principal := &models.Principal{Username: user}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users(user)[0]).Return(nil)

	h := dynUserHandler{
		authorizer:    authorizer,
		dbUserEnabled: true,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: user, HTTPRequest: req}, principal)
	_, ok := res.(*users.DeleteUserUnprocessableEntity)
	assert.True(t, ok)
}

func TestDeleteUnprocessableEntityDeletingRootUser(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users("user-root")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer,
		rbacConfig: rbacconf.Config{RootUsers: []string{"user-root"}}, dbUserEnabled: true,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user-root", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeleteUserUnprocessableEntity)
	assert.True(t, ok)
}

func TestDeleteNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeleteUserUnprocessableEntity)
	assert.True(t, ok)
}

// TestDeleteUser_Namespaces pins the self-delete bypass closure for
// namespaced callers (self-target / root / static guards on the resolved key).
func TestDeleteUser_Namespaces(t *testing.T) {
	tests := []struct {
		name             string
		userID           string
		principalName    string // principal.Username — only consulted by the self-target guard
		principalNS      string
		isGlobalOperator bool
		rootUsers        []string
		authzKey         string
		wantStatus       any
	}{
		{
			name:        "namespaced principal own user succeeds",
			userID:      "bob",
			principalNS: "customer1",
			authzKey:    "customer1:bob",
			wantStatus:  &users.DeleteUserNoContent{},
		},
		{
			name:          "namespaced principal self-delete on resolved key returns 422",
			userID:        "alice",
			principalName: "customer1:alice",
			principalNS:   "customer1",
			authzKey:      "customer1:alice",
			wantStatus:    &users.DeleteUserUnprocessableEntity{},
		},
		{
			name:        "namespaced principal root on resolved key returns 422",
			userID:      "boss",
			principalNS: "customer1",
			rootUsers:   []string{"customer1:boss"},
			authzKey:    "customer1:boss",
			wantStatus:  &users.DeleteUserUnprocessableEntity{},
		},
		{
			name:        "namespaced principal foreign short returns 404",
			userID:      "bob",
			principalNS: "customer2",
			authzKey:    "customer2:bob",
			wantStatus:  &users.DeleteUserNotFound{},
		},
		{
			name:             "global operator qualified passes through",
			userID:           "customer1:bob",
			isGlobalOperator: true,
			authzKey:         "customer1:bob",
			wantStatus:       &users.DeleteUserNoContent{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{
				Username:         tt.principalName,
				IsGlobalOperator: tt.isGlobalOperator,
				Namespace:        tt.principalNS,
			}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users(tt.authzKey)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			if _, ok := tt.wantStatus.(*users.DeleteUserNoContent); ok {
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]*apikey.User{tt.authzKey: {}}, nil)
				dynUser.On("GetRolesForUserOrGroup", tt.authzKey, authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{}, nil)
				dynUser.On("DeleteUser", mock.Anything, tt.authzKey).Return(nil)
			} else if _, ok := tt.wantStatus.(*users.DeleteUserNotFound); ok {
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]*apikey.User{}, nil)
			}

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
				rbacConfig:        rbacconf.Config{RootUsers: tt.rootUsers},
			}

			res := h.deleteUser(users.DeleteUserParams{UserID: tt.userID, HTTPRequest: req}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

// TestDeleteUser_ResolveThenAuthorize — authz mocked only on the qualified key;
// a pre-resolution call on the raw short name would fail as an unexpected mock invocation.
func TestDeleteUser_ResolveThenAuthorize(t *testing.T) {
	principal := &models.Principal{Namespace: "customer1"}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.DELETE, authorization.Users("customer1:bob")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "customer1:bob").Return(map[string]*apikey.User{"customer1:bob": {}}, nil)
	dynUser.On("GetRolesForUserOrGroup", "customer1:bob", authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{}, nil)
	dynUser.On("DeleteUser", mock.Anything, "customer1:bob").Return(nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "bob", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeleteUserNoContent)
	require.True(t, ok, "expected 204, got %T", res)
}
