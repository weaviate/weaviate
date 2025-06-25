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

	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/stretchr/testify/assert"
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
	dynUser.On("GetRolesForUser", "user", models.UserTypeInputDb).Return(map[string][]authorization.Policy{"role": {}}, nil)
	dynUser.On("RevokeRolesForUser", conv.UserNameWithTypeFromId("user", models.UserTypeInputDb), "role").Return(nil)
	dynUser.On("DeleteUser", "user").Return(nil)
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
