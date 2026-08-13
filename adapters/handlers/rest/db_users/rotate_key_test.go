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
	"testing"

	"github.com/weaviate/weaviate/usecases/config"

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
)

func TestSuccessRotate(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]apikey.UserView{"user": {Id: "user"}}, nil)
	dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
	dynUser.On("RotateKey", mock.Anything, "user", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:       dynUser,
		authorizer:    authorizer,
		dbUserEnabled: true,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user", HTTPRequest: req}, principal)
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
		GetUserReturnValue map[string]apikey.UserView
		RotateKeyError     error
	}{
		{name: "get user error", GetUserReturnErr: errors.New("some error"), GetUserReturnValue: nil},
		{name: "rotate key error", GetUserReturnErr: nil, GetUserReturnValue: map[string]apikey.UserView{"user": {Id: "user", InternalIdentifier: "abc"}}, RotateKeyError: errors.New("some error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
			dynUser := NewMockDbUserAndRolesGetter(t)
			dynUser.On("GetUsers", "user").Return(tt.GetUserReturnValue, tt.GetUserReturnErr)
			if tt.GetUserReturnErr == nil {
				dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
				dynUser.On("RotateKey", mock.Anything, "user", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tt.RotateKeyError)
			}

			h := dynUserHandler{
				dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true,
			}

			res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user", HTTPRequest: req}, principal)
			parsed, ok := res.(*users.RotateUserAPIKeyInternalServerError)
			assert.True(t, ok)
			assert.NotNil(t, parsed)
		})
	}
}

func TestRotateNotFound(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)
	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]apikey.UserView{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.RotateUserAPIKeyNotFound)
	assert.True(t, ok)
}

func TestRotateForbidden(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(errors.New("some error"))

	dynUser := NewMockDbUserAndRolesGetter(t)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.RotateUserAPIKeyForbidden)
	assert.True(t, ok)
}

func TestRotateUnprocessableEntity(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "user").Return(map[string]apikey.UserView{}, nil)

	h := dynUserHandler{
		dbUsers:    dynUser,
		authorizer: authorizer, dbUserEnabled: true,

		staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{"user"}, AllowedKeys: []string{"key"}},
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.RotateUserAPIKeyUnprocessableEntity)
	assert.True(t, ok)
}

func TestRotateNoDynamic(t *testing.T) {
	principal := &models.Principal{}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("user")[0]).Return(nil)

	h := dynUserHandler{
		dbUsers:       NewMockDbUserAndRolesGetter(t),
		authorizer:    authorizer,
		dbUserEnabled: false,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "user", HTTPRequest: req}, principal)
	_, ok := res.(*users.RotateUserAPIKeyUnprocessableEntity)
	assert.True(t, ok)
}

// TestRotateKey_Namespaces — resolved-key flow; self-rotation is allowed
// (no self-target guard).
func TestRotateKey_Namespaces(t *testing.T) {
	tests := []struct {
		name             string
		userID           string
		principalNS      string
		isGlobalOperator bool
		authzKey         string
		wantStatus       any
	}{
		{
			name:        "namespaced principal own user succeeds",
			userID:      "bob",
			principalNS: "customer1",
			authzKey:    "customer1:bob",
			wantStatus:  &users.RotateUserAPIKeyOK{},
		},
		{
			name:        "namespaced principal self-rotation allowed",
			userID:      "alice",
			principalNS: "customer1",
			authzKey:    "customer1:alice",
			wantStatus:  &users.RotateUserAPIKeyOK{},
		},
		{
			name:        "namespaced principal foreign short returns 404",
			userID:      "bob",
			principalNS: "customer2",
			authzKey:    "customer2:bob",
			wantStatus:  &users.RotateUserAPIKeyNotFound{},
		},
		{
			name:             "global operator qualified passes through",
			userID:           "customer1:bob",
			isGlobalOperator: true,
			authzKey:         "customer1:bob",
			wantStatus:       &users.RotateUserAPIKeyOK{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			principal := &models.Principal{
				IsGlobalOperator: tt.isGlobalOperator,
				Namespace:        tt.principalNS,
			}
			authorizer := authorization.NewMockAuthorizer(t)
			authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(tt.authzKey)[0]).Return(nil)

			dynUser := NewMockDbUserAndRolesGetter(t)
			switch tt.wantStatus.(type) {
			case *users.RotateUserAPIKeyOK:
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{tt.authzKey: {InternalIdentifier: "old-id"}}, nil)
				dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
				dynUser.On("RotateKey", mock.Anything, tt.authzKey, mock.Anything, mock.Anything, "old-id", mock.Anything).Return(nil)
			case *users.RotateUserAPIKeyNotFound:
				dynUser.On("GetUsers", tt.authzKey).Return(map[string]apikey.UserView{}, nil)
			}

			h := dynUserHandler{
				dbUsers:           dynUser,
				authorizer:        authorizer,
				dbUserEnabled:     true,
				namespacesEnabled: true,
			}

			res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: tt.userID, HTTPRequest: req}, principal)
			assert.IsType(t, tt.wantStatus, res)
		})
	}
}

// TestRotateKey_ResolveThenAuthorize — authz mocked only on the qualified key;
// a pre-resolution call on the raw short name would fail as an unexpected mock invocation.
func TestRotateKey_ResolveThenAuthorize(t *testing.T) {
	principal := &models.Principal{Namespace: "customer1"}
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users("customer1:bob")[0]).Return(nil)

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "customer1:bob").Return(map[string]apikey.UserView{"customer1:bob": {InternalIdentifier: "old-id"}}, nil)
	dynUser.On("CheckUserIdentifierExists", mock.Anything).Return(false, nil)
	dynUser.On("RotateKey", mock.Anything, "customer1:bob", mock.Anything, mock.Anything, "old-id", mock.Anything).Return(nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.rotateKey(users.RotateUserAPIKeyParams{UserID: "bob", HTTPRequest: req}, principal)
	_, ok := res.(*users.RotateUserAPIKeyOK)
	require.True(t, ok, "expected 200, got %T", res)
}
