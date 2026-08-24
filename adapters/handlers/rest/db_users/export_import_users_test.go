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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/dbuser"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// A well-formed argon2id hash prefix is all the import validation checks.
const strongHash = "$argon2id$v=19$m=65536,t=3,p=2$c29tZXNhbHQ$aGFzaHZhbHVl"

func strptr(s string) *string { return &s }

// activeNsExister reports every namespace as existing and active.
func activeNsExister(t *testing.T) *namespaces.MockExister {
	t.Helper()
	ns := namespaces.NewMockExister(t)
	ns.On("GetNamespace", mock.AnythingOfType("string")).Return(
		func(name string) api.Namespace {
			return api.Namespace{Name: name, HomeNodes: []string{"node-1"}, State: api.NamespaceStateActive}
		},
		func(string) bool { return true },
	).Maybe()
	return ns
}

func TestExportUsersHandler(t *testing.T) {
	principal := &models.Principal{}

	t.Run("forbidden without users read", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(errors.New("denied"))
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(users.ExportUsersParams{HTTPRequest: req}, principal)
		_, ok := res.(*users.ExportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("emits record and sentinel per user", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers").Return(map[string]dbuser.ExportRecord{
			"strong":   {Id: "strong", UserIdentifier: "ident", ApiKeyFirstLetters: "abc", Active: true, Status: dbuser.ExportStatusExported, SecureHash: strptr(strongHash)},
			"imported": {Id: "imported", Status: dbuser.ExportStatusImportedKey},
		}, nil)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(users.ExportUsersParams{HTTPRequest: req}, principal)
		parsed, ok := res.(*users.ExportUsersOK)
		require.True(t, ok)
		require.Len(t, parsed.Payload.Users, 2)

		byID := map[string]*models.DBUserCredential{}
		for _, u := range parsed.Payload.Users {
			byID[*u.UserID] = u
		}
		require.Equal(t, dbuser.ExportStatusExported, byID["strong"].Status)
		require.Equal(t, strongHash, byID["strong"].SecureHash)
		require.Equal(t, dbuser.ExportStatusImportedKey, byID["imported"].Status)
		require.Empty(t, byID["imported"].SecureHash)
	})

	t.Run("response ids are stripped to bare form", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers").Return(map[string]dbuser.ExportRecord{
			"ns1:bob": {Id: "ns1:bob", UserIdentifier: "ident", Namespace: "ns1", Active: true, Status: dbuser.ExportStatusExported, SecureHash: strptr(strongHash)},
		}, nil)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(users.ExportUsersParams{HTTPRequest: req}, principal)
		parsed, ok := res.(*users.ExportUsersOK)
		require.True(t, ok)
		require.Len(t, parsed.Payload.Users, 1)
		require.Equal(t, "bob", *parsed.Payload.Users[0].UserID)
		require.Equal(t, "ns1", parsed.Payload.Users[0].Namespace)
	})

	t.Run("uses a single export call as the roster", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(nil)
		// GetUsers is deliberately not mocked: a second roster query would panic
		// as an unexpected call, pinning that export reads only ExportUsers.
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers").Return(map[string]dbuser.ExportRecord{}, nil)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(users.ExportUsersParams{HTTPRequest: req}, principal)
		_, ok := res.(*users.ExportUsersOK)
		require.True(t, ok)
		dynUser.AssertNotCalled(t, "GetUsers", mock.Anything)
	})
}

func TestImportUsersHandler(t *testing.T) {
	principal := &models.Principal{IsGlobalOperator: true}
	const key = "ns1:bob"

	importOne := func(rec *models.DBUserCredential) users.ImportUsersParams {
		return users.ImportUsersParams{
			HTTPRequest: req,
			Body:        &models.UserImportRequest{Namespace: "ns1", Users: []*models.DBUserCredential{rec}},
		}
	}
	strongRecord := func(active bool) *models.DBUserCredential {
		return &models.DBUserCredential{UserID: strptr("bob"), UserIdentifier: "ident", SecureHash: strongHash, APIKeyFirstLetters: "abc", Active: active}
	}
	firstResult := func(t *testing.T, res interface{}) *models.UserImportResult {
		t.Helper()
		parsed, ok := res.(*users.ImportUsersOK)
		require.True(t, ok)
		require.Len(t, parsed.Payload.Results, 1)
		return parsed.Payload.Results[0]
	}

	t.Run("creates a strong user in the target namespace", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("GetUsers", key).Return(map[string]apikey.UserView{}, nil)
		dynUser.On("CheckUserIdentifierExists", "ident").Return(false, nil)
		dynUser.On("CreateUser", mock.Anything, key, strongHash, "ident", "abc", "ns1", mock.Anything).Return(nil)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusCreated, *result.Status)
	})

	t.Run("forbidden when the caller cannot write the target namespace", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(errors.New("denied"))
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: namespaces.NewMockExister(t)}
		res := h.importUsers(importOne(strongRecord(true)), principal)
		_, ok := res.(*users.ImportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("rejects import into an inactive namespace", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		ns := namespaces.NewMockExister(t)
		ns.On("GetNamespace", mock.AnythingOfType("string")).Return(api.Namespace{}, false).Maybe()

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: ns}
		res := h.importUsers(importOne(strongRecord(true)), principal)
		_, ok := res.(*users.ImportUsersUnprocessableEntity)
		assert.True(t, ok)
	})

	t.Run("rejects a supplied namespace on a non-namespaced cluster", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: false}
		res := h.importUsers(importOne(strongRecord(true)), principal)
		_, ok := res.(*users.ImportUsersUnprocessableEntity)
		assert.True(t, ok)
	})

	t.Run("reports clobber when the identifier maps to a different user", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("GetUsers", key).Return(map[string]apikey.UserView{}, nil)
		dynUser.On("CheckUserIdentifierExists", "ident").Return(true, nil)
		// CreateUser is deliberately not mocked: it must not be called on a clobber.

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "different target user")
		dynUser.AssertNotCalled(t, "CreateUser", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("errors when an existing id holds a different identifier", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("GetUsers", key).Return(map[string]apikey.UserView{key: {Id: key, InternalIdentifier: "other", Active: true}}, nil)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		dynUser.AssertNotCalled(t, "CreateUser", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("reconciles active state for an existing same-identifier user", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		// Stored active; record says inactive → deactivate to converge.
		dynUser.On("GetUsers", key).Return(map[string]apikey.UserView{key: {Id: key, InternalIdentifier: "ident", Active: true}}, nil)
		dynUser.On("DeactivateUser", mock.Anything, key, false).Return(nil)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(false)), principal))
		require.Equal(t, models.UserImportResultStatusReconciled, *result.Status)
	})

	t.Run("skips an existing same-identifier user already in the recorded state", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("GetUsers", key).Return(map[string]apikey.UserView{key: {Id: key, InternalIdentifier: "ident", Active: true}}, nil)

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusSkippedExists, *result.Status)
	})

	t.Run("reports partial failure when deactivation fails after create", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("GetUsers", key).Return(map[string]apikey.UserView{}, nil)
		dynUser.On("CheckUserIdentifierExists", "ident").Return(false, nil)
		dynUser.On("CreateUser", mock.Anything, key, strongHash, "ident", "abc", "ns1", mock.Anything).Return(nil)
		dynUser.On("DeactivateUser", mock.Anything, key, false).Return(errors.New("raft down"))

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(false)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "re-run to reconcile")
	})

	t.Run("rejects a record with an empty or malformed secure hash", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		// Neither GetUsers nor CreateUser is mocked: validation must reject the
		// record before any store call.
		dynUser := NewMockDbUserAndRolesGetter(t)

		rec := strongRecord(true)
		rec.SecureHash = "not-an-argon2id-hash"

		h := dynUserHandler{dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(rec), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "argon2id")
	})
}
