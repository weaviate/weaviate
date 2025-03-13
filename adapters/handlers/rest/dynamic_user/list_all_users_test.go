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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/dynamic_user/mocks"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzMocks "github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestSuccessListAll(t *testing.T) {
	dynamicUser := "user1"
	staticUser := "static"
	tests := []struct {
		name          string
		principal     *models.Principal
		includeStatic bool
	}{
		{
			name:          "only dynamic user",
			principal:     &models.Principal{Username: "not-root"},
			includeStatic: false,
		},
		{
			name:          "dynamic + static user",
			principal:     &models.Principal{Username: "root"},
			includeStatic: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authorizer := authzMocks.NewAuthorizer(t)
			authorizer.On("Authorize", test.principal, authorization.READ, authorization.Users()[0]).Return(nil)
			dynUser := mocks.NewDynamicUserAndRolesGetter(t)
			dynUser.On("GetUsers").Return(map[string]*apikey.User{dynamicUser: {Id: dynamicUser}}, nil)
			dynUser.On("GetRolesForUser", dynamicUser, models.UserTypeDb).Return(
				map[string][]authorization.Policy{"role": {}}, nil)
			if test.includeStatic {
				dynUser.On("GetRolesForUser", staticUser, models.UserTypeDb).Return(
					map[string][]authorization.Policy{"role": {}}, nil)
			}

			h := dynUserHandler{
				dynamicUser:          dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{staticUser}, AllowedKeys: []string{"static"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
			}

			res := h.listUsers(users.ListAllUsersParams{}, test.principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			if test.includeStatic {
				require.Equal(t, len(parsed.Payload), 2)
			} else {
				require.Len(t, parsed.Payload, 1)
			}
		})
	}
}

func TestSuccessListForbidden(t *testing.T) {
	principal := &models.Principal{Username: "not-root"}
	authorizer := authzMocks.NewAuthorizer(t)
	authorizer.On("Authorize", principal, authorization.READ, mock.Anything).Return(errors.New("some error"))
	dynUser := mocks.NewDynamicUserAndRolesGetter(t)
	dynUser.On("GetUsers").Return(map[string]*apikey.User{"test": {Id: "test"}}, nil)

	log, _ := test.NewNullLogger()
	h := dynUserHandler{
		dynamicUser: dynUser,
		authorizer:  authorizer,
		logger:      log,
	}

	// no authorization for anything => response will be empty
	res := h.listUsers(users.ListAllUsersParams{}, principal)
	parsed, ok := res.(*users.ListAllUsersOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	require.Len(t, parsed.Payload, 0)
}
