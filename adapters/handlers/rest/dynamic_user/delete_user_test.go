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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/dynamic_user/mocks"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestDeleteSuccess(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.DELETE, authorization.Users("user")[0]).Return(nil)

	dynUser := mocks.NewDynamicUserAndRolesGetter(t)
	dynUser.On("GetRolesForUser", "user").Return(map[string][]authorization.Policy{"role": {}}, nil)
	dynUser.On("RevokeRolesForUser", conv.PrefixUserName("user"), "role").Return(nil)
	dynUser.On("DeleteUser", "user").Return(nil)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user"}, principal)
	parsed, ok := res.(*users.DeleteUserNoContent)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestDeleteForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.DELETE, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := mocks.NewDynamicUserAndRolesGetter(t)

	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user"}, principal)
	_, ok := res.(*users.DeleteUserForbidden)
	assert.True(t, ok)
}

func TestDeleteUnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.DELETE, authorization.Users("user")[0]).Return(nil)

	dynUser := mocks.NewDynamicUserAndRolesGetter(t)

	h := dynUserHandler{
		dynamicUser:          dynUser,
		authorizer:           authorizer,
		staticApiKeysConfigs: config.APIKey{Enabled: true, Users: []string{"user"}, AllowedKeys: []string{"key"}},
	}

	res := h.deleteUser(users.DeleteUserParams{UserID: "user"}, principal)
	_, ok := res.(*users.DeleteUserUnprocessableEntity)
	assert.True(t, ok)
}
