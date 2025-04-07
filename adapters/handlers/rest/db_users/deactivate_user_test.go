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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/db_users/mocks"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
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
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
			dynUser := mocks.NewDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {Id: "user", Active: true}}, nil)
			dynUser.On("DeactivateUser", "user", test.revokeKey).Return(nil)

			h := dynUserHandler{
				dbUsers:    dynUser,
				authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.deactivateUser(users.DeactivateUserParams{UserID: "user", Body: users.DeactivateUserBody{RevokeKey: &test.revokeKey}}, principal)
			_, ok := res.(*users.DeactivateUserOK)
			assert.True(t, ok)
		})
	}
}

func TestDeactivateNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := mocks.NewDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.deactivateUser(users.DeactivateUserParams{UserID: "user"}, principal)
	_, ok := res.(*users.DeactivateUserNotFound)
	assert.True(t, ok)
}

func TestDeactivateBadParameters(t *testing.T) {
	tests := []struct {
		name          string
		user          string
		principal     string
		getUserReturn map[string]*apikey.User
	}{
		{name: "static user", user: "static-user", principal: "admin"},
		{name: "root user", user: "root-user", principal: "admin"},
		{name: "own user", user: "myself", principal: "myself"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.name), func(t *testing.T) {
			principal := &models.Principal{Username: test.principal}
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users(test.user)[0]).Return(nil)
			dynUser := mocks.NewDbUserAndRolesGetter(t)
			if test.getUserReturn != nil {
				dynUser.On("GetUsers", test.user).Return(test.getUserReturn, nil)
			}

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static-user"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}, dbUserEnabled: true,
			}

			res := h.deactivateUser(users.DeactivateUserParams{UserID: test.user}, principal)
			_, ok := res.(*users.DeactivateUserUnprocessableEntity)
			assert.True(t, ok)
		})
	}
}

func TestDoubleDeactivate(t *testing.T) {
	user := "deactivated-user"
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users(user)[0]).Return(nil)
	dynUser := mocks.NewDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", user).Return(map[string]*apikey.User{user: {Id: user, Active: false}}, nil)

	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static-user"}},
		rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}, dbUserEnabled: true,
	}

	res := h.deactivateUser(users.DeactivateUserParams{UserID: user}, principal)
	_, ok := res.(*users.DeactivateUserConflict)
	assert.True(t, ok)
}

func TestSuspendNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       mocks.NewDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.deactivateUser(users.DeactivateUserParams{UserID: "user"}, principal)
	_, ok := res.(*users.DeactivateUserUnprocessableEntity)
	assert.True(t, ok)
}
