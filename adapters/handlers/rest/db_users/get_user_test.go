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

package db_users

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/db_users/mocks"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestSuccessList(t *testing.T) {
	tests := []struct {
		name     string
		userId   string
		userType models.UserTypeOutput
	}{
		{name: "dynamic user", userId: "dynamic", userType: models.UserTypeOutputDbUser},
		{name: "static user", userId: "static", userType: models.UserTypeOutputDbEnvUser},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			principal := &models.Principal{Username: "root"}
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", principal, authorization.READ, authorization.Users(test.userId)[0]).Return(nil)
			dynUser := mocks.NewDbUserAndRolesGetter(t)
			if test.userType == models.UserTypeOutputDbUser {
				dynUser.On("GetUsers", test.userId).Return(map[string]*apikey.User{test.userId: {Id: test.userId}}, nil)
			}
			dynUser.On("GetRolesForUser", test.userId, models.UserTypeInputDb).Return(
				map[string][]authorization.Policy{"role": {}}, nil)

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static"}, AllowedKeys: []string{"static"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}}, dbUserEnabled: true,
			}

			res := h.getUser(users.GetUserInfoParams{UserID: test.userId}, principal)
			parsed, ok := res.(*users.GetUserInfoOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			require.Equal(t, *parsed.Payload.UserID, test.userId)
			require.Equal(t, parsed.Payload.Roles, []string{"role"})
			require.Equal(t, *parsed.Payload.DbUserType, string(test.userType))
		})
	}
}

func TestNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.READ, authorization.Users("static")[0]).Return(nil)
	dynUser := mocks.NewDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "static").Return(map[string]*apikey.User{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,

		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static"}, AllowedKeys: []string{"static"}},
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "static"}, principal)
	_, ok := res.(*users.GetUserInfoNotFound)
	assert.True(t, ok)
}

func TestNotFoundStatic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.READ, authorization.Users("user")[0]).Return(nil)
	dynUser := mocks.NewDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "user"}, principal)
	_, ok := res.(*users.GetUserInfoNotFound)
	assert.True(t, ok)
}

func TestGetUserInternalServerError(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name               string
		GetUserReturnErr   error
		GetUserReturnValue map[string]*apikey.User
		GetRolesReturn     error
	}{
		{name: "get user error", GetUserReturnErr: errors.New("some error"), GetUserReturnValue: nil},
		{name: "create user error", GetUserReturnErr: nil, GetUserReturnValue: map[string]*apikey.User{"user": {Id: "user"}}, GetRolesReturn: errors.New("some error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", principal, authorization.READ, authorization.Users("user")[0]).Return(nil)
			dynUser := mocks.NewDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(tt.GetUserReturnValue, tt.GetUserReturnErr)
			if tt.GetUserReturnErr == nil {
				dynUser.On("GetRolesForUser", "user", models.UserTypeInputDb).Return(nil, tt.GetRolesReturn)
			}

			h := dynUserHandler{
				dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.getUser(users.GetUserInfoParams{UserID: "user"}, principal)
			parsed, ok := res.(*users.GetUserInfoInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestListForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.READ, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := mocks.NewDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "user"}, principal)
	_, ok := res.(*users.GetUserInfoForbidden)
	assert.True(t, ok)
}
