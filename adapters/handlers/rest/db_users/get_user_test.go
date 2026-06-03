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
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/stretchr/testify/mock"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/usecases/schema"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestSuccessGetUser(t *testing.T) {
	tests := []struct {
		name         string
		userId       string
		isRoot       bool
		addLastUsed  bool
		importStatic bool
		userType     models.UserTypeOutput
	}{
		{name: "dynamic user - non-root", userId: "dynamic", userType: models.UserTypeOutputDbUser, isRoot: false},
		{name: "dynamic user - root", userId: "dynamic", userType: models.UserTypeOutputDbUser, isRoot: true},
		{name: "dynamic user with last used - root", userId: "dynamic", userType: models.UserTypeOutputDbUser, isRoot: true, addLastUsed: true},
		{name: "static user", userId: "static", userType: models.UserTypeOutputDbEnvUser, isRoot: true},
		{name: "dynamic user after import - root", userId: "static", userType: models.UserTypeOutputDbUser, isRoot: true, importStatic: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			username := "non-root"
			if test.isRoot {
				username = "root"
			}
			principal := &models.Principal{Username: username}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(test.userId)[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			schemaGetter := schema.NewMockSchemaGetter(t)
			if test.userType == models.UserTypeOutputDbUser {
				dynUser.On("GetUsers", test.userId).Return(map[string]apikey.UserView{test.userId: {Id: test.userId, ApiKeyFirstLetters: "abc"}}, nil)
			} else {
				dynUser.On("GetUsers", test.userId).Return(map[string]apikey.UserView{}, nil)
			}
			dynUser.On("GetRolesForUserOrGroup", test.userId, authentication.AuthTypeDb, false).Return(
				map[string][]authorization.Policy{"role": {}}, nil)

			if test.addLastUsed {
				schemaGetter.On("Nodes").Return([]string{"node1"})
			}

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static"}, AllowedKeys: []string{"static"}},
				rbacConfig:           rbacconf.Config{Enabled: true, RootUsers: []string{"root"}}, dbUserEnabled: true,
				nodesGetter: schemaGetter,
			}

			res := h.getUser(users.GetUserInfoParams{UserID: test.userId, IncludeLastUsedTime: &test.addLastUsed, HTTPRequest: req}, principal)
			parsed, ok := res.(*users.GetUserInfoOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			require.Equal(t, *parsed.Payload.UserID, test.userId)
			require.Equal(t, parsed.Payload.Roles, []string{"role"})
			require.Equal(t, *parsed.Payload.DbUserType, string(test.userType))

			if test.isRoot && test.userType == models.UserTypeOutputDbUser {
				require.Equal(t, parsed.Payload.APIKeyFirstLetters, "abc")
			} else {
				require.Equal(t, parsed.Payload.APIKeyFirstLetters, "")
			}
		})
	}
}

func TestSuccessGetUserMultiNode(t *testing.T) {
	returnedTime := time.Now()

	userId := "user"

	truep := true
	tests := []struct {
		name          string
		nodeResponses []map[string]time.Time
		expectedTime  time.Time
	}{
		{name: "single node", nodeResponses: []map[string]time.Time{{}}, expectedTime: returnedTime},
		{name: "multi node with latest time on local node", expectedTime: returnedTime, nodeResponses: []map[string]time.Time{{userId: returnedTime.Add(-time.Second)}, {userId: returnedTime.Add(-time.Second)}}},
		{name: "multi node with latest time on other node", expectedTime: returnedTime.Add(time.Hour), nodeResponses: []map[string]time.Time{{userId: returnedTime.Add(time.Hour)}, {userId: returnedTime.Add(time.Minute)}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			principal := &models.Principal{Username: "non-root"}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(userId)[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			schemaGetter := schema.NewMockSchemaGetter(t)

			dynUser.On("GetUsers", userId).Return(map[string]apikey.UserView{userId: {Id: userId, LastUsedAt: returnedTime}}, nil)
			dynUser.On("GetRolesForUserOrGroup", userId, authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{"role": {}}, nil)

			var nodes []string
			for i := range test.nodeResponses {
				nodes = append(nodes, string(rune(i)))
			}
			schemaGetter.On("Nodes").Return(nodes)

			server := httptest.NewServer(&fakeHandler{t: t, counter: atomic.Int32{}, nodeResponses: test.nodeResponses})
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

			res := h.getUser(users.GetUserInfoParams{UserID: userId, IncludeLastUsedTime: &truep, HTTPRequest: req}, principal)
			parsed, ok := res.(*users.GetUserInfoOK)
			assert.True(t, ok)
			assert.NotNil(t, parsed)

			require.Equal(t, *parsed.Payload.UserID, userId)
			require.Equal(t, parsed.Payload.LastUsedAt.String(), strfmt.DateTime(test.expectedTime).String())
		})
	}
}

func TestNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("static")[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "static").Return(map[string]apikey.UserView{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,

		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"static"}, AllowedKeys: []string{"static"}},
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "static", HTTPRequest: req}, principal)
	_, ok := res.(*users.GetUserInfoNotFound)
	assert.True(t, ok)
}

func TestNotFoundStatic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("user")[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]apikey.UserView{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.GetUserInfoNotFound)
	assert.True(t, ok)
}

func TestGetUserInternalServerError(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name               string
		GetUserReturnErr   error
		GetUserReturnValue map[string]apikey.UserView
		GetRolesReturn     error
	}{
		{name: "get user error", GetUserReturnErr: errors.New("some error"), GetUserReturnValue: nil},
		{name: "create user error", GetUserReturnErr: nil, GetUserReturnValue: map[string]apikey.UserView{"user": {Id: "user"}}, GetRolesReturn: errors.New("some error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("user")[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(tt.GetUserReturnValue, tt.GetUserReturnErr)
			if tt.GetUserReturnErr == nil {
				dynUser.On("GetRolesForUserOrGroup", "user", authentication.AuthTypeDb, false).Return(nil, tt.GetRolesReturn)
			}

			h := dynUserHandler{
				dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.getUser(users.GetUserInfoParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.GetUserInfoInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestListForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.GetUserInfoForbidden)
	assert.True(t, ok)
}

func TestGetNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.GetUserInfoUnprocessableEntity)
	assert.True(t, ok)
}

func TestGetUserWithNoPrincipal(t *testing.T) {
	var (
		principal *models.Principal
		userID    = "static"
	)
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(userID)[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", userID).Return(map[string]apikey.UserView{userID: {Id: userID, ApiKeyFirstLetters: "abc"}}, nil)
	dynUser.On("GetRolesForUserOrGroup", userID, authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{"role": {}}, nil)

	h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}

	res := h.getUser(users.GetUserInfoParams{UserID: "static", HTTPRequest: req}, principal)
	parsed, ok := res.(*users.GetUserInfoOK)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

// TestGetUser_Namespaces — resolved-key flow + response stripping for a
// namespaced caller; qualified passthrough + namespace field for a global op.
func TestGetUser_Namespaces(t *testing.T) {
	tests := []struct {
		name             string
		userID           string // raw id as the client sends it
		principalNS      string // principal.Namespace ("" = global)
		isGlobalOperator bool
		authzKey         string                  // resolved users/ key authz is asked for
		storedUser       map[string]*apikey.User // GetUsers return; key by internal key
		wantStatus       any
		wantUserID       string // expected response.UserID
		wantNamespace    string // expected response.Namespace
	}{
		{
			name:          "namespaced principal own short name succeeds and strips",
			userID:        "bob",
			principalNS:   "customer1",
			authzKey:      "customer1:bob",
			storedUser:    map[string]*apikey.User{"customer1:bob": {Id: "customer1:bob", Namespace: "customer1", Active: true}},
			wantStatus:    &users.GetUserInfoOK{},
			wantUserID:    "bob",
			wantNamespace: "",
		},
		{
			name:        "namespaced principal foreign short returns 404",
			userID:      "bob",
			principalNS: "customer2",
			authzKey:    "customer2:bob",
			storedUser:  map[string]*apikey.User{},
			wantStatus:  &users.GetUserInfoNotFound{},
		},
		{
			name:             "global operator qualified passes through and sees namespace",
			userID:           "customer1:bob",
			isGlobalOperator: true,
			authzKey:         "customer1:bob",
			storedUser:       map[string]*apikey.User{"customer1:bob": {Id: "customer1:bob", Namespace: "customer1", Active: true}},
			wantStatus:       &users.GetUserInfoOK{},
			wantUserID:       "customer1:bob",
			wantNamespace:    "customer1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{IsGlobalOperator: tt.isGlobalOperator, Namespace: tt.principalNS}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(tt.authzKey)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", tt.authzKey).Return(tt.storedUser, nil)
			if _, ok := tt.wantStatus.(*users.GetUserInfoOK); ok {
				dynUser.On("GetRolesForUserOrGroup", tt.authzKey, authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{}, nil)
			}

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
			}

			res := h.getUser(users.GetUserInfoParams{UserID: tt.userID, HTTPRequest: req}, principal)
			assert.IsType(t, tt.wantStatus, res)
			if ok, _ := res.(*users.GetUserInfoOK); ok != nil {
				require.Equal(t, tt.wantUserID, *ok.Payload.UserID)
				require.Equal(t, tt.wantNamespace, ok.Payload.Namespace)
			}
		})
	}
}

// TestGetUser_ResolveThenAuthorize — authz mocked only on the qualified key;
// a pre-resolution call on the raw short name would fail as an unexpected mock invocation.
func TestGetUser_ResolveThenAuthorize(t *testing.T) {
	principal := &models.Principal{Namespace: "customer1"}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("customer1:bob")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "customer1:bob").Return(map[string]*apikey.User{"customer1:bob": {Id: "customer1:bob", Namespace: "customer1", Active: true}}, nil)
	dynUser.On("GetRolesForUserOrGroup", "customer1:bob", authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{}, nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "bob", HTTPRequest: req}, principal)
	_, ok := res.(*users.GetUserInfoOK)
	require.True(t, ok, "expected 200, got %T", res)
}
