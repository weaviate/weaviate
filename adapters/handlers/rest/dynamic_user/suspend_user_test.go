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

package dynamic_user

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/dynamic_user/mocks"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestSuccessSuspend(t *testing.T) {
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
			dynUser := mocks.NewDynamicUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {Id: "user", Active: true}}, nil)
			dynUser.On("SuspendUser", "user", test.revokeKey).Return(nil)

			h := dynUserHandler{
				dynamicUser: dynUser,
				authorizer:  authorizer,
			}

			res := h.suspendUser(users.SuspendUserParams{UserID: "user", Body: users.SuspendUserBody{DeactivateKey: &test.revokeKey}}, principal)
			_, ok := res.(*users.SuspendUserOK)
			assert.True(t, ok)
		})
	}
}

func TestSuspendNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := mocks.NewDynamicUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
	}

	res := h.suspendUser(users.SuspendUserParams{UserID: "user"}, principal)
	_, ok := res.(*users.SuspendUserNotFound)
	assert.True(t, ok)
}

func TestSuspendBadParameters(t *testing.T) {
	tests := []struct {
		name          string
		user          string
		getUserReturn map[string]*apikey.User
	}{
		{name: "static user", user: "static-user"},
		{name: "root user", user: "root-user"},
		{name: "suspended user", user: "suspended-user", getUserReturn: map[string]*apikey.User{"suspended-user": {Id: "suspended-user", Active: false}}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.name), func(t *testing.T) {
			principal := &models.Principal{}
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users(test.user)[0]).Return(nil)
			dynUser := mocks.NewDynamicUserAndRolesGetter(t)
			if test.getUserReturn != nil {
				dynUser.On("GetUsers", test.user).Return(test.getUserReturn, nil)
			}

			h := dynUserHandler{
				dynamicUser:          dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.APIKey{Enabled: true, Users: []string{"static-user"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}},
			}

			res := h.suspendUser(users.SuspendUserParams{UserID: test.user}, principal)
			_, ok := res.(*users.SuspendUserUnprocessableEntity)
			assert.True(t, ok)
		})
	}
}
