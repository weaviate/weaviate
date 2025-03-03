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
	"errors"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"

	"github.com/weaviate/weaviate/usecases/config"

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/weaviate/weaviate/adapters/handlers/rest/dynamic_user/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestCreateUnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name   string
		userId string
	}{
		{name: "too long", userId: strings.Repeat("A", 100)},
		{name: "invalid characters", userId: "#a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authzMocks.NewAuthorizer(t)
			dynUser := mocks.NewDynamicUserAndRolesGetter(t)

			h := dynUserHandler{
				dynamicUser: dynUser,
				authorizer:  authorizer,
			}

			res := h.createUser(users.CreateUserParams{UserID: tt.userId}, principal)
			parsed, ok := res.(*users.CreateUserUnprocessableEntity)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateInternalServerError(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name                                 string
		GetUserReturn                        error
		CheckUserIdentifierExistsErrorReturn error
		CheckUserIdentifierExistsValueReturn bool
		CreateUserReturn                     error
	}{
		{name: "get user error", GetUserReturn: errors.New("some error")},
		{name: "check identifier exists, error", GetUserReturn: nil, CheckUserIdentifierExistsErrorReturn: errors.New("some error")},
		{name: "check identifier exists, repeated collision", GetUserReturn: nil, CheckUserIdentifierExistsErrorReturn: nil, CheckUserIdentifierExistsValueReturn: true},
		{name: "create user error", GetUserReturn: nil, CheckUserIdentifierExistsErrorReturn: nil, CreateUserReturn: errors.New("some error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

			dynUser := mocks.NewDynamicUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(nil, tt.GetUserReturn)
			if tt.GetUserReturn == nil {
				dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(tt.CheckUserIdentifierExistsValueReturn, tt.CheckUserIdentifierExistsErrorReturn)
			}
			if tt.CheckUserIdentifierExistsErrorReturn == nil && !tt.CheckUserIdentifierExistsValueReturn && tt.GetUserReturn == nil {
				dynUser.On("CreateUser", "user", mock.Anything, mock.Anything).Return(tt.CreateUserReturn)
			}

			h := dynUserHandler{
				dynamicUser: dynUser,
				authorizer:  authorizer,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user"}, principal)
			parsed, ok := res.(*users.CreateUserInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateConflict(t *testing.T) {
	tests := []struct {
		name     string
		rbacConf config.APIKey
	}{
		{name: "no rbac conf", rbacConf: config.APIKey{}},
		{name: "enabled rbac conf", rbacConf: config.APIKey{Enabled: true, Users: []string{"user"}, AllowedKeys: []string{"key"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{}

			authorizer := authzMocks.NewAuthorizer(t)
			dynUser := mocks.NewDynamicUserAndRolesGetter(t)
			authorizer.On("Authorize", principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)
			if !tt.rbacConf.Enabled {
				dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {}}, nil)
			}

			h := dynUserHandler{
				dynamicUser:          dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: tt.rbacConf,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user"}, principal)
			parsed, ok := res.(*users.CreateUserConflict)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateSuccess(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

	dynUser := mocks.NewDynamicUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)
	dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
	dynUser.On("CreateUser", "user", mock.Anything, mock.Anything).Return(nil)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user"}, principal)
	parsed, ok := res.(*users.CreateUserCreated)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestCreateForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.CREATE, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := mocks.NewDynamicUserAndRolesGetter(t)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user"}, principal)
	_, ok := res.(*users.CreateUserForbidden)
	assert.True(t, ok)
}

func TestCreateUnprocessableEntityCreatingRootUser(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.CREATE, authorization.Users("user-root")[0]).Return(nil)

	dynUser := mocks.NewDynamicUserAndRolesGetter(t)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
		rbacConfig:  rbacconf.Config{RootUsers: []string{"user-root"}},
	}

	res := h.createUser(users.CreateUserParams{UserID: "user-root"}, principal)
	_, ok := res.(*users.CreateUserUnprocessableEntity)
	assert.True(t, ok)
}
