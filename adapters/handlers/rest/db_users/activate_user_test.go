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
	"net/http"
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

var req, _ = http.NewRequest("POST", "/activate", nil)

func TestSuccessActivate(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {Id: "user", Active: false}}, nil)
	dynUser.On("ActivateUser", mock.Anything, "user").Return(nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}
	res := h.activateUser(users.ActivateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.ActivateUserOK)
	assert.True(t, ok)
}

func TestActivateNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.activateUser(users.ActivateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.ActivateUserNotFound)
	assert.True(t, ok)
}

func TestActivateBadParameters(t *testing.T) {
	tests := []struct {
		name string
		user string
	}{
		{name: "static user", user: "static-user"},
		{name: "root user", user: "root-user"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.name), func(t *testing.T) {
			principal := &models.Principal{}
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

			res := h.activateUser(users.ActivateUserParams{UserID: test.user, HTTPRequest: req}, principal)
			_, ok := res.(*users.ActivateUserUnprocessableEntity)
			assert.True(t, ok)
		})
	}
}

func TestDoubleActivate(t *testing.T) {
	user := "active-user"
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(user)[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", user).Return(map[string]*apikey.User{user: {Id: user, Active: true}}, nil)

	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static-user"}},
		rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}, dbUserEnabled: true,
	}

	res := h.activateUser(users.ActivateUserParams{UserID: user, HTTPRequest: req}, principal)
	_, ok := res.(*users.ActivateUserConflict)
	assert.True(t, ok)
}

func TestActivateNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.activateUser(users.ActivateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.ActivateUserUnprocessableEntity)
	assert.True(t, ok)
}

// TestActivateUser_Namespaces — resolved-key root guard; no self-target
// guard (activate has none by design).
func TestActivateUser_Namespaces(t *testing.T) {
	tests := []struct {
		name             string
		userID           string
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
			wantStatus:  &users.ActivateUserOK{},
		},
		{
			name:        "namespaced principal root on resolved key returns 422",
			userID:      "boss",
			principalNS: "customer1",
			rootUsers:   []string{"customer1:boss"},
			authzKey:    "customer1:boss",
			wantStatus:  &users.ActivateUserUnprocessableEntity{},
		},
		{
			name:        "namespaced principal foreign short returns 404",
			userID:      "bob",
			principalNS: "customer2",
			authzKey:    "customer2:bob",
			wantStatus:  &users.ActivateUserNotFound{},
		},
		{
			name:             "global operator qualified passes through",
			userID:           "customer1:bob",
			isGlobalOperator: true,
			authzKey:         "customer1:bob",
			wantStatus:       &users.ActivateUserOK{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{
				IsGlobalOperator: tt.isGlobalOperator,
				Namespace:        tt.principalNS,
			}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(tt.authzKey)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			switch tt.wantStatus.(type) {
			case *users.ActivateUserOK:
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]*apikey.User{tt.authzKey: {Id: tt.authzKey, Active: false}}, nil)
				dynUser.On("ActivateUser", mock.Anything, tt.authzKey).Return(nil)
			case *users.ActivateUserNotFound:
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]*apikey.User{}, nil)
			}

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
				rbacConfig:        rbacconf.Config{RootUsers: tt.rootUsers},
			}

			res := h.activateUser(users.ActivateUserParams{UserID: tt.userID, HTTPRequest: req}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

// TestActivateUser_ResolveThenAuthorize — authz mocked only on the qualified key;
// a pre-resolution call on the raw short name would fail as an unexpected mock invocation.
func TestActivateUser_ResolveThenAuthorize(t *testing.T) {
	principal := &models.Principal{Namespace: "customer1"}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("customer1:bob")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "customer1:bob").Return(map[string]*apikey.User{"customer1:bob": {Active: false}}, nil)
	dynUser.On("ActivateUser", mock.Anything, "customer1:bob").Return(nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.activateUser(users.ActivateUserParams{UserID: "bob", HTTPRequest: req}, principal)
	_, ok := res.(*users.ActivateUserOK)
	require.True(t, ok, "expected 200, got %T", res)
}
