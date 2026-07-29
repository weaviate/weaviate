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
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestSuccessDeactivate(t *testing.T) {
	tests := []struct {
		revokeKey bool
	}{
		{false}, {true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.revokeKey), func(t *testing.T) {
			principal := &models.Principal{}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(map[string]apikey.UserView{"user": {Id: "user", Active: true}}, nil)
			dynUser.On("DeactivateUser", mock.Anything, "user", test.revokeKey).Return(nil)

			h := dynUserHandler{
				dbUsers:    dynUser,
				authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.deactivateUser(users.DeactivateUserParams{UserID: "user", HTTPRequest: req, Body: users.DeactivateUserBody{RevokeKey: &test.revokeKey}}, principal)
			_, ok := res.(*users.DeactivateUserOK)
			assert.True(t, ok)
		})
	}
}

func TestDeactivateNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]apikey.UserView{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.deactivateUser(users.DeactivateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeactivateUserNotFound)
	assert.True(t, ok)
}

func TestDeactivateBadParameters(t *testing.T) {
	tests := []struct {
		name      string
		user      string
		principal string
	}{
		{name: "static user", user: "static-user", principal: "admin"},
		{name: "root user", user: "root-user", principal: "admin"},
		{name: "own user", user: "myself", principal: "myself"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.name), func(t *testing.T) {
			principal := &models.Principal{Username: test.principal}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(test.user)[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			if test.user == "static-user" {
				dynUser.On("GetUsers", test.user).Return(nil, nil)
			}

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static-user"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}, dbUserEnabled: true,
			}

			res := h.deactivateUser(users.DeactivateUserParams{UserID: test.user, HTTPRequest: req}, principal)
			_, ok := res.(*users.DeactivateUserUnprocessableEntity)
			assert.True(t, ok)
		})
	}
}

func TestDoubleDeactivate(t *testing.T) {
	user := "deactivated-user"
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(user)[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", user).Return(map[string]apikey.UserView{user: {Id: user, Active: false}}, nil)

	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static-user"}},
		rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}, dbUserEnabled: true,
	}

	res := h.deactivateUser(users.DeactivateUserParams{UserID: user, HTTPRequest: req}, principal)
	_, ok := res.(*users.DeactivateUserConflict)
	assert.True(t, ok)
}

func TestSuspendNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.deactivateUser(users.DeactivateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeactivateUserUnprocessableEntity)
	assert.True(t, ok)
}

// TestDeactivateUser_Namespaces pins the self-deactivate bypass closure
// (self-target / root guards on the resolved key).
func TestDeactivateUser_Namespaces(t *testing.T) {
	tests := []struct {
		name             string
		userID           string
		principalName    string
		principalNS      string
		isGlobalOperator bool
		rootUsers        []string
		authzKey         string
		wantStatus       any
	}{
		{
			name:          "namespaced principal own user succeeds",
			userID:        "bob",
			principalName: "customer1:alice",
			principalNS:   "customer1",
			authzKey:      "customer1:bob",
			wantStatus:    &users.DeactivateUserOK{},
		},
		{
			name:          "namespaced principal self-deactivate on resolved key returns 422",
			userID:        "alice",
			principalName: "customer1:alice",
			principalNS:   "customer1",
			authzKey:      "customer1:alice",
			wantStatus:    &users.DeactivateUserUnprocessableEntity{},
		},
		{
			name:          "namespaced principal root on resolved key returns 422",
			userID:        "boss",
			principalName: "customer1:alice",
			principalNS:   "customer1",
			rootUsers:     []string{"customer1:boss"},
			authzKey:      "customer1:boss",
			wantStatus:    &users.DeactivateUserUnprocessableEntity{},
		},
		{
			name:          "namespaced principal foreign short returns 404",
			userID:        "bob",
			principalName: "customer2:alice",
			principalNS:   "customer2",
			authzKey:      "customer2:bob",
			wantStatus:    &users.DeactivateUserNotFound{},
		},
		{
			name:             "global operator qualified passes through",
			userID:           "customer1:bob",
			principalName:    "root",
			isGlobalOperator: true,
			authzKey:         "customer1:bob",
			wantStatus:       &users.DeactivateUserOK{},
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
			authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(tt.authzKey)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			// Self-target / root guards short-circuit before GetUsers.
			switch tt.wantStatus.(type) {
			case *users.DeactivateUserOK:
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{tt.authzKey: {Id: tt.authzKey, Active: true}}, nil)
				dynUser.On("DeactivateUser", mock.Anything, tt.authzKey, mock.Anything).Return(nil)
			case *users.DeactivateUserNotFound:
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{}, nil)
			}

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
				rbacConfig:        rbacconf.Config{RootUsers: tt.rootUsers},
			}

			res := h.deactivateUser(users.DeactivateUserParams{UserID: tt.userID, HTTPRequest: req}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

// TestDeactivateUser_ResolveThenAuthorize — authz mocked only on the qualified key;
// a pre-resolution call on the raw short name would fail as an unexpected mock invocation.
func TestDeactivateUser_ResolveThenAuthorize(t *testing.T) {
	principal := &models.Principal{Namespace: "customer1"}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("customer1:bob")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "customer1:bob").Return(map[string]apikey.UserView{"customer1:bob": {Active: true}}, nil)
	dynUser.On("DeactivateUser", mock.Anything, "customer1:bob", mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.deactivateUser(users.DeactivateUserParams{UserID: "bob", HTTPRequest: req}, principal)
	_, ok := res.(*users.DeactivateUserOK)
	require.True(t, ok, "expected 200, got %T", res)
}
