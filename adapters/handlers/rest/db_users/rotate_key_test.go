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

	"github.com/weaviate/weaviate/usecases/config"

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/db_users/mocks"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestSuccessRotate(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := mocks.NewDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {Id: "user"}}, nil)
	dynUser.On("RotateKey", "user", mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:       dynUser,
		authorizer:    authorizer,
		dbUserEnabled: true,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user"}, principal)
	parsed, ok := res.(*users.RotateUserAPIKeyOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)

	require.Len(t, *parsed.Payload.Apikey, 88)
}

func TestRotateInternalServerError(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name               string
		GetUserReturnErr   error
		GetUserReturnValue map[string]*apikey.User
		RotateKeyError     error
	}{
		{name: "get user error", GetUserReturnErr: errors.New("some error"), GetUserReturnValue: nil},
		{name: "rotate key error", GetUserReturnErr: nil, GetUserReturnValue: map[string]*apikey.User{"user": {Id: "user", InternalIdentifier: "abc"}}, RotateKeyError: errors.New("some error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
			dynUser := mocks.NewDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(tt.GetUserReturnValue, tt.GetUserReturnErr)
			if tt.GetUserReturnErr == nil {
				dynUser.On("RotateKey", "user", mock.Anything).Return(tt.RotateKeyError)
			}

			h := dynUserHandler{
				dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user"}, principal)
			parsed, ok := res.(*users.RotateUserAPIKeyInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestRotateNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := mocks.NewDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user"}, principal)
	_, ok := res.(*users.RotateUserAPIKeyNotFound)
	assert.True(t, ok)
}

func TestRotateForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := mocks.NewDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user"}, principal)
	_, ok := res.(*users.RotateUserAPIKeyForbidden)
	assert.True(t, ok)
}

func TestRotateUnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)

	dynUser := mocks.NewDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,

		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user"}, AllowedKeys: []string{"key"}},
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user"}, principal)
	_, ok := res.(*users.RotateUserAPIKeyUnprocessableEntity)
	assert.True(t, ok)
}

func TestRotateNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       mocks.NewDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user"}, principal)
	_, ok := res.(*users.RotateUserAPIKeyUnprocessableEntity)
	assert.True(t, ok)
}
