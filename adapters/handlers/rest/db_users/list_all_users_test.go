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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/usecases/schema"

	"github.com/sirupsen/logrus/hooks/test"
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

func TestSuccessListAll(t *testing.T) {
	dbUser := "user1"
	staticUser := "static"
	tests := []struct {
		name          string
		principal     *models.Principal
		includeStatic bool
	}{
		{
			name:          "only db user",
			principal:     &models.Principal{Username: "not-root"},
			includeStatic: false,
		},
		{
			name:          "db + static user",
			principal:     &models.Principal{Username: "root"},
			includeStatic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, tt.principal, authorization.READ, authorization.Users()[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers").Return(map[string]*apikey.User{dbUser: {Id: dbUser}}, nil)
			dynUser.On("GetRolesForUser", dbUser, models.UserTypeInputDb).Return(
				map[string][]authorization.Policy{"role": {}}, nil)
			if tt.includeStatic {
				dynUser.On("GetRolesForUser", staticUser, models.UserTypeInputDb).Return(
					map[string][]authorization.Policy{"role": {}}, nil)
			}

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{staticUser}, AllowedKeys: []string{"static"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
				dbUserEnabled:        true,
			}

			res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, tt.principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			if tt.includeStatic {
				require.Equal(t, len(parsed.Payload), 2)
			} else {
				require.Len(t, parsed.Payload, 1)
			}
		})
	}
}

func TestSuccessListAllAfterImport(t *testing.T) {
	exStaticUser := "static"
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, &models.Principal{Username: "root"}, authorization.READ, authorization.Users()[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers").Return(map[string]*apikey.User{exStaticUser: {Id: exStaticUser, Active: true}}, nil)
	dynUser.On("GetRolesForUser", exStaticUser, models.UserTypeInputDb).Return(
		map[string][]authorization.Policy{"role": {}}, nil)

	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{exStaticUser}, AllowedKeys: []string{"static"}},
		rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
		dbUserEnabled:        true,
	}

	res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, &models.Principal{Username: "root"})
	parsed, ok := res.(*users.ListAllUsersOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	require.Len(t, parsed.Payload, 1)
	user := parsed.Payload[0]
	require.Equal(t, *user.UserID, exStaticUser)
	require.Equal(t, *user.Active, true)
	require.Equal(t, *user.DbUserType, string(models.UserTypeOutputDbUser))
}

func TestSuccessListAllUserMultiNode(t *testing.T) {
	baseTime := time.Now()

	usersIds := []string{"user1", "user2", "user3", "user4", "user5", "user6"}

	trueptr := true
	tests := []struct {
		name          string
		nodeResponses []map[string]time.Time
		expectedTime  map[string]time.Time
		userIds       []string
	}{
		{name: "single node, single user", nodeResponses: []map[string]time.Time{{}}, expectedTime: map[string]time.Time{usersIds[0]: baseTime}, userIds: usersIds[:1]},
		{name: "single node, multi user", nodeResponses: []map[string]time.Time{{}}, expectedTime: map[string]time.Time{usersIds[0]: baseTime, usersIds[1]: baseTime}, userIds: usersIds[:2]},
		{
			name:          "multi node, latest time local node, single user",
			userIds:       usersIds[:1],
			expectedTime:  map[string]time.Time{usersIds[0]: baseTime},
			nodeResponses: []map[string]time.Time{{usersIds[0]: baseTime.Add(-time.Second)}, {usersIds[0]: baseTime.Add(-time.Second)}},
		},
		{
			name:         "multi node, latest time local node, multi user",
			userIds:      usersIds[:2],
			expectedTime: map[string]time.Time{usersIds[0]: baseTime, usersIds[1]: baseTime},
			nodeResponses: []map[string]time.Time{
				{usersIds[0]: baseTime.Add(-time.Second), usersIds[1]: baseTime.Add(-2 * time.Second)},
				{usersIds[0]: baseTime.Add(-time.Second), usersIds[1]: baseTime.Add(-2 * time.Second)},
			},
		},
		{
			name:          "multi node, latest time other node, single user",
			userIds:       usersIds[:1],
			expectedTime:  map[string]time.Time{usersIds[0]: baseTime.Add(time.Hour)},
			nodeResponses: []map[string]time.Time{{usersIds[0]: baseTime.Add(time.Hour)}, {usersIds[0]: baseTime.Add(time.Minute)}},
		},
		{
			name:         "multi node, latest time other node, multi user",
			userIds:      usersIds[:2],
			expectedTime: map[string]time.Time{usersIds[0]: baseTime.Add(time.Hour), usersIds[1]: baseTime.Add(2 * time.Hour)},
			nodeResponses: []map[string]time.Time{
				{usersIds[0]: baseTime.Add(time.Hour), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[0]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(2 * time.Hour)},
			},
		},
		{
			name:    "six node, six user",
			userIds: usersIds,
			expectedTime: map[string]time.Time{
				usersIds[0]: baseTime.Add(time.Hour),
				usersIds[1]: baseTime.Add(2 * time.Hour),
				usersIds[2]: baseTime.Add(3 * time.Hour),
				usersIds[3]: baseTime.Add(4 * time.Hour),
				usersIds[4]: baseTime.Add(5 * time.Hour),
				usersIds[5]: baseTime.Add(6 * time.Hour),
			},
			nodeResponses: []map[string]time.Time{
				{usersIds[0]: baseTime.Add(time.Hour), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[0]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(2 * time.Hour)},
				{usersIds[2]: baseTime.Add(3 * time.Hour), usersIds[3]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[2]: baseTime.Add(-time.Minute), usersIds[3]: baseTime.Add(4 * time.Hour)},
				{usersIds[4]: baseTime.Add(5 * time.Hour), usersIds[5]: baseTime.Add(time.Minute), usersIds[1]: baseTime.Add(time.Minute)},
				{usersIds[4]: baseTime.Add(-time.Minute), usersIds[5]: baseTime.Add(6 * time.Hour)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{Username: "non-root"}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users()[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			schemaGetter := schema.NewMockSchemaGetter(t)

			usersRet := make(map[string]*apikey.User)
			for _, user := range tt.userIds {
				usersRet[user] = &apikey.User{Id: user, LastUsedAt: baseTime}
			}

			dynUser.On("GetUsers").Return(usersRet, nil)
			for _, user := range tt.userIds {
				dynUser.On("GetRolesForUser", user, models.UserTypeInputDb).Return(map[string][]authorization.Policy{"role": {}}, nil)
			}

			var nodes []string
			for i := range tt.nodeResponses {
				nodes = append(nodes, string(rune(i)))
			}
			schemaGetter.On("Nodes").Return(nodes)

			server := httptest.NewServer(&fakeHandler{t: t, counter: atomic.Int32{}, nodeResponses: tt.nodeResponses})
			defer server.Close()

			remote := clients.NewRemoteUser(&http.Client{}, FakeNodeResolver{path: server.URL})

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static"}, AllowedKeys: []string{"static"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}}, dbUserEnabled: true,
				nodesGetter: schemaGetter,
				remoteUser:  remote,
			}

			res := h.listUsers(users.ListAllUsersParams{IncludeLastUsedTime: &trueptr, HTTPRequest: req}, principal)
			parsed, ok := res.(*users.ListAllUsersOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			for i := range tt.userIds {
				uid := *parsed.Payload[i].UserID
				require.Equal(t, parsed.Payload[i].LastUsedAt.String(), strfmt.DateTime(tt.expectedTime[uid]).String())
			}
		})
	}
}

func TestSuccessListForbidden(t *testing.T) {
	principal := &models.Principal{Username: "not-root"}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, mock.Anything).Return(errors.New("some error"))
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers").Return(map[string]*apikey.User{"test": {Id: "test"}}, nil)

	log, _ := test.NewNullLogger()
	h := dynUserHandler{
		dbUsers:       dynUser,
		authorizer:    authorizer,
		logger:        log,
		dbUserEnabled: true,
	}

	// no authorization for anything => response will be empty
	res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*users.ListAllUsersOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	require.Len(t, parsed.Payload, 0)
}

func TestListNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*users.ListAllUsersOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	require.Len(t, parsed.Payload, 0)
}
