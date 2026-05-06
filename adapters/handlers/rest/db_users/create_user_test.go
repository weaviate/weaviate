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
	"strings"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"

	"github.com/weaviate/weaviate/usecases/config"

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

func TestCreateUnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	tests := []struct {
		name   string
		userId string
	}{
		{name: "too long", userId: strings.Repeat("A", 129)},
		{name: "invalid characters", userId: "#a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			dynUser := NewMockDbUserAndRolesGetter(t)

			h := dynUserHandler{
				dbUsers:    dynUser,
				authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.createUser(users.CreateUserParams{UserID: tt.userId, HTTPRequest: req}, principal)
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
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(nil, tt.GetUserReturn)
			if tt.GetUserReturn == nil {
				dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(tt.CheckUserIdentifierExistsValueReturn, tt.CheckUserIdentifierExistsErrorReturn)
			}
			if tt.CheckUserIdentifierExistsErrorReturn == nil && !tt.CheckUserIdentifierExistsValueReturn && tt.GetUserReturn == nil {
				dynUser.On("CreateUser", "user", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tt.CreateUserReturn)
			}

			h := dynUserHandler{
				dbUsers:    dynUser,
				authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.CreateUserInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateConflict(t *testing.T) {
	tests := []struct {
		name     string
		rbacConf config.StaticAPIKey
	}{
		{name: "no rbac conf", rbacConf: config.StaticAPIKey{}},
		{name: "enabled rbac conf", rbacConf: config.StaticAPIKey{Enabled: true, Users: []string{"user"}, AllowedKeys: []string{"key"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{}

			authorizer := authorization.NewMockAuthorizer(t)
			dynUser := NewMockDbUserAndRolesGetter(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)
			if !tt.rbacConf.Enabled {
				dynUser.On("GetUsers", "user").Return(map[string]*apikey.User{"user": {}}, nil)
			}

			h := dynUserHandler{
				dbUsers:              dynUser,
				authorizer:           authorizer,
				staticApiKeysConfigs: tt.rbacConf,
				dbUserEnabled:        true,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.CreateUserConflict)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateSuccess(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	user := "user@weaviate.io"
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(user)[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", user).Return(map[string]*apikey.User{}, nil)
	dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
	dynUser.On("CreateUser", user, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.createUser(users.CreateUserParams{UserID: user, HTTPRequest: req}, principal)
	parsed, ok := res.(*users.CreateUserCreated)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestCreateSuccessWithKey(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	user := "user@weaviate.io"
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(user)[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("CreateUserWithKey", user, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		dbUserEnabled:        true,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{user}, AllowedKeys: []string{"key"}},
	}
	tp := true

	res := h.createUser(users.CreateUserParams{UserID: user, HTTPRequest: req, Body: users.CreateUserBody{Import: &tp}}, principal)
	parsed, ok := res.(*users.CreateUserCreated)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
	assert.Equal(t, *parsed.Payload.Apikey, "key")
}

func TestCreateNotFoundWithKey(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	user := "user@weaviate.io"
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(user)[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	h := dynUserHandler{
		dbUsers:              dynUser,
		authorizer:           authorizer,
		dbUserEnabled:        true,
		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{user + "false"}, AllowedKeys: []string{"key"}},
	}
	tp := true

	res := h.createUser(users.CreateUserParams{UserID: user, HTTPRequest: req, Body: users.CreateUserBody{Import: &tp}}, principal)
	parsed, ok := res.(*users.CreateUserNotFound)
	assert.True(t, ok)
	assert.NotNil(t, parsed)
}

func TestCreateForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.CreateUserForbidden)
	assert.True(t, ok)
}

func TestCreateUnprocessableEntityCreatingRootUser(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user-root")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer,
		rbacConfig: rbacconf.Config{RootUsers: []string{"user-root"}}, dbUserEnabled: true,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user-root", HTTPRequest: req}, principal)
	_, ok := res.(*users.CreateUserUnprocessableEntity)
	assert.True(t, ok)
}

func TestCreateUnprocessableEntityCreatingAdminlistUser(t *testing.T) {
	tests := []struct {
		name          string
		adminlistConf adminlist.Config
	}{
		{name: "adminlist - read-only user", adminlistConf: adminlist.Config{Enabled: true, ReadOnlyUsers: []string{"user"}}},
		{name: "adminlist - admin user", adminlistConf: adminlist.Config{Enabled: true, Users: []string{"user"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{}

			authorizer := authorization.NewMockAuthorizer(t)
			dynUser := NewMockDbUserAndRolesGetter(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

			h := dynUserHandler{
				dbUsers:         dynUser,
				authorizer:      authorizer,
				adminListConfig: tt.adminlistConf,
				dbUserEnabled:   true,
			}

			res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.CreateUserUnprocessableEntity)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestCreateNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.createUser(users.CreateUserParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.CreateUserUnprocessableEntity)
	assert.True(t, ok)
}

func TestCreateUser_Namespaces(t *testing.T) {
	const userID = "user"
	tp := true

	tests := []struct {
		name              string
		namespacesEnabled bool
		known             []string
		body              users.CreateUserBody
		isGlobalOperator  bool
		wantStatus        any
	}{
		{
			name:              "ns-disabled + namespace set rejects",
			namespacesEnabled: false,
			body:              users.CreateUserBody{Namespace: "ns1"},
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "ns-enabled + missing namespace rejects",
			namespacesEnabled: true,
			known:             []string{"ns1"},
			body:              users.CreateUserBody{},
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "ns-enabled + unknown namespace rejects",
			namespacesEnabled: true,
			known:             []string{"ns1"},
			body:              users.CreateUserBody{Namespace: "ns404"},
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "ns-enabled + import rejects",
			namespacesEnabled: true,
			known:             []string{"ns1"},
			body:              users.CreateUserBody{Import: &tp, Namespace: "ns1"},
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserUnprocessableEntity{},
		},
		{
			name:              "namespace set by non-operator forbidden",
			namespacesEnabled: true,
			known:             []string{"ns1"},
			body:              users.CreateUserBody{Namespace: "ns1"},
			isGlobalOperator:  false,
			wantStatus:        &users.CreateUserForbidden{},
		},
		{
			name:              "ns-enabled + valid namespace + operator succeeds",
			namespacesEnabled: true,
			known:             []string{"ns1"},
			body:              users.CreateUserBody{Namespace: "ns1"},
			isGlobalOperator:  true,
			wantStatus:        &users.CreateUserCreated{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{IsGlobalOperator: tt.isGlobalOperator}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(userID)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			if _, ok := tt.wantStatus.(*users.CreateUserCreated); ok {
				expectedKey := apikey.MakeUserKey(userID, tt.body.Namespace)
				dynUser.On("GetUsers", expectedKey).Return(map[string]*apikey.User{}, nil)
				dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
				dynUser.On("CreateUser", expectedKey, mock.Anything, mock.Anything, mock.Anything, tt.body.Namespace, mock.Anything).Return(nil)
			}

			ns := namespaces.NewMockExister(t)
			known := map[string]struct{}{}
			for _, n := range tt.known {
				known[n] = struct{}{}
			}
			ns.On("Exists", mock.AnythingOfType("string")).Return(func(name string) bool {
				_, ok := known[name]
				return ok
			}).Maybe()

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: tt.namespacesEnabled,
				namespaces:        ns,
			}

			res := h.createUser(users.CreateUserParams{UserID: userID, HTTPRequest: req, Body: tt.body}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

func TestListAndGetUser_NamespaceVisibility(t *testing.T) {
	const userID = "u1"
	storedUser := &apikey.User{Id: userID, Namespace: "ns1", Active: true, ApiKeyFirstLetters: "abc"}

	tests := []struct {
		name             string
		isGlobalOperator bool
		wantNamespace    string
	}{
		{name: "operator sees namespace", isGlobalOperator: true, wantNamespace: "ns1"},
		{name: "non-operator does not see namespace", isGlobalOperator: false, wantNamespace: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{IsGlobalOperator: tt.isGlobalOperator}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users(userID)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", userID).Return(map[string]*apikey.User{userID: storedUser}, nil)
			dynUser.On("GetRolesForUserOrGroup", userID, mock.Anything, false).Return(map[string][]authorization.Policy{}, nil)

			h := dynUserHandler{
				dbUsers:       dynUser,
				authorizer:    authorizer,
				dbUserEnabled: true,
			}

			res := h.getUser(users.GetUserInfoParams{UserID: userID, HTTPRequest: req}, principal)
			parsed, ok := res.(*users.GetUserInfoOK)
			assert.True(t, ok)
			assert.Equal(t, tt.wantNamespace, parsed.Payload.Namespace)
		})
	}
}
