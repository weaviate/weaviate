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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/experimental"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/dbuser"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/namespaces"
	wldbusers "github.com/weaviate/weaviate/wl/dbusers"
)

// Import decodes the hash and rejects zero cost parameters, so the valid
// constant must carry real values.
const (
	strongHash        = "$argon2id$v=19$m=65536,t=3,p=2$c29tZXNhbHQ$aGFzaHZhbHVl"
	zeroIterationHash = "$argon2id$v=19$m=65536,t=0,p=2$c29tZXNhbHQ$aGFzaHZhbHVl"
	zeroParallelHash  = "$argon2id$v=19$m=65536,t=3,p=0$c29tZXNhbHQ$aGFzaHZhbHVl"
	zeroMemoryHash    = "$argon2id$v=19$m=0,t=3,p=2$c29tZXNhbHQ$aGFzaHZhbHVl"
)

func strptr(s string) *string { return &s }

var rootOnly = rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}

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
	principal := &models.Principal{Username: "root-user"}

	t.Run("forbidden for a non-root caller even with users read", func(t *testing.T) {
		// Root is checked first, so the authorizer is never consulted.
		h := dynUserHandler{rbacConfig: rootOnly, dbUsers: NewMockDbUserAndRolesGetter(t), authorizer: authorization.NewMockAuthorizer(t), dbUserEnabled: true}
		res := h.exportUsers(experimental.ExportUsersParams{HTTPRequest: req}, &models.Principal{Username: "viewer"})
		_, ok := res.(*experimental.ExportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("forbidden without users read", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(errors.New("denied"))
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(experimental.ExportUsersParams{HTTPRequest: req}, principal)
		_, ok := res.(*experimental.ExportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("emits record and sentinel per user", func(t *testing.T) {
		expiresAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers").Return(map[string]dbuser.ExportRecord{
			"strong":   {Id: "strong", UserIdentifier: "identifier-16-ch", ApiKeyFirstLetters: "abc", Active: true, Status: dbuser.ExportStatusExported, SecureHash: strptr(strongHash), ExpiresAt: expiresAt},
			"imported": {Id: "imported", Status: dbuser.ExportStatusImportedKey},
		}, nil)

		h := dynUserHandler{rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(experimental.ExportUsersParams{HTTPRequest: req}, principal)
		parsed, ok := res.(*experimental.ExportUsersOK)
		require.True(t, ok)
		require.Len(t, parsed.Payload.Users, 2)

		byID := map[string]*models.DBUserCredential{}
		for _, u := range parsed.Payload.Users {
			byID[*u.UserID] = u
		}
		require.Equal(t, models.DBUserCredentialStatusExported, byID["strong"].Status)
		require.Equal(t, strongHash, byID["strong"].SecureHash)
		require.Equal(t, new(strfmt.DateTime(expiresAt)), byID["strong"].ExpiresAt)
		require.Equal(t, models.DBUserCredentialStatusImportedKey, byID["imported"].Status)
		require.Empty(t, byID["imported"].SecureHash)
		require.Nil(t, byID["imported"].ExpiresAt)
	})

	t.Run("response ids are stripped to bare form", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers").Return(map[string]dbuser.ExportRecord{
			"ns1:bob": {Id: "ns1:bob", UserIdentifier: "identifier-16-ch", Namespace: "ns1", Active: true, Status: dbuser.ExportStatusExported, SecureHash: strptr(strongHash)},
		}, nil)

		h := dynUserHandler{rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(experimental.ExportUsersParams{HTTPRequest: req}, principal)
		parsed, ok := res.(*experimental.ExportUsersOK)
		require.True(t, ok)
		require.Len(t, parsed.Payload.Users, 1)
		require.Equal(t, "bob", *parsed.Payload.Users[0].UserID)
		require.Equal(t, "ns1", parsed.Payload.Users[0].Namespace)
	})

	t.Run("refuses a record with an unspecified status", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		// The zero value must never reach a client as an exported record.
		dynUser.On("ExportUsers").Return(map[string]dbuser.ExportRecord{"bob": {Id: "bob"}}, nil)

		h := dynUserHandler{rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(experimental.ExportUsersParams{HTTPRequest: req}, principal)
		_, ok := res.(*experimental.ExportUsersInternalServerError)
		require.True(t, ok)
	})

	t.Run("uses a single export call as the roster", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.READ, authorization.Users("*")[0]).Return(nil)
		// GetUsers is deliberately not mocked: a second roster query would panic
		// as an unexpected call, pinning that export reads only ExportUsers.
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers").Return(map[string]dbuser.ExportRecord{}, nil)

		h := dynUserHandler{rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true}
		res := h.exportUsers(experimental.ExportUsersParams{HTTPRequest: req}, principal)
		_, ok := res.(*experimental.ExportUsersOK)
		require.True(t, ok)
	})
}

func TestImportUsersHandler(t *testing.T) {
	principal := &models.Principal{Username: "root-user", IsGlobalOperator: true}
	const key = "ns1:bob"

	importOne := func(rec *models.DBUserCredential) experimental.ImportUsersParams {
		return experimental.ImportUsersParams{
			HTTPRequest: req,
			Body:        &models.UserImportRequest{Namespace: "ns1", Users: []*models.DBUserCredential{rec}},
		}
	}
	strongRecord := func(active bool) *models.DBUserCredential {
		return &models.DBUserCredential{UserID: strptr("bob"), UserIdentifier: "identifier-16-ch", SecureHash: strongHash, APIKeyFirstLetters: "abc", Active: active}
	}
	firstResult := func(t *testing.T, res interface{}) *models.UserImportResult {
		t.Helper()
		parsed, ok := res.(*experimental.ImportUsersOK)
		require.True(t, ok)
		require.Len(t, parsed.Payload.Results, 1)
		return parsed.Payload.Results[0]
	}

	t.Run("forbidden for a non-root caller before any authorization", func(t *testing.T) {
		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: NewMockDbUserAndRolesGetter(t), authorizer: authorization.NewMockAuthorizer(t), dbUserEnabled: true, namespacesEnabled: true, namespaces: namespaces.NewMockExister(t)}
		res := h.importUsers(importOne(strongRecord(true)), &models.Principal{Username: "admin", IsGlobalOperator: true})
		_, ok := res.(*experimental.ImportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("creates a strong user in the target namespace", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{}, nil)
		dynUser.On("CheckUserIdentifierExists", "identifier-16-ch").Return(false, nil)
		dynUser.On("CreateUser", mock.Anything, key, strongHash, "identifier-16-ch", "abc", "ns1", mock.Anything, mock.Anything).Return(nil)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusCreated, *result.Status)
	})

	t.Run("empty batch authorizes then returns no results", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		wildcardKey := apikey.MakeUserKey("*", "ns1")
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(wildcardKey)[0]).Return(nil)
		// No store or namespace calls are mocked: the handler must not touch them.
		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: NewMockDbUserAndRolesGetter(t), authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: namespaces.NewMockExister(t)}
		res := h.importUsers(experimental.ImportUsersParams{HTTPRequest: req, Body: &models.UserImportRequest{Namespace: "ns1"}}, principal)
		parsed, ok := res.(*experimental.ImportUsersOK)
		require.True(t, ok)
		require.Empty(t, parsed.Payload.Results)
	})

	t.Run("empty batch forbidden for unprivileged caller", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		wildcardKey := apikey.MakeUserKey("*", "ns1")
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(wildcardKey)[0]).Return(errors.New("denied"))
		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: NewMockDbUserAndRolesGetter(t), authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: namespaces.NewMockExister(t)}
		res := h.importUsers(experimental.ImportUsersParams{HTTPRequest: req, Body: &models.UserImportRequest{Namespace: "ns1"}}, principal)
		_, ok := res.(*experimental.ImportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("forbidden when the caller cannot write the target namespace", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(errors.New("denied"))
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: namespaces.NewMockExister(t)}
		res := h.importUsers(importOne(strongRecord(true)), principal)
		_, ok := res.(*experimental.ImportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("rejects import into an inactive namespace", func(t *testing.T) {
		tests := []struct {
			name             string
			namespace        api.Namespace
			exists           bool
			suspendedMessage string
			wantMsg          string
		}{
			{name: "missing", wantMsg: "instance unavailable"},
			{
				name:             "suspended renders NAMESPACE_SUSPENDED_MESSAGE",
				namespace:        api.Namespace{Name: "ns1", State: api.NamespaceStateSuspended},
				exists:           true,
				suspendedMessage: "paused",
				wantMsg:          "paused",
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				authorizer := authorization.NewMockAuthorizer(t)
				authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
				authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
				dynUser := NewMockDbUserAndRolesGetter(t)
				ns := namespaces.NewMockExister(t)
				ns.On("GetNamespace", "ns1").Return(tc.namespace, tc.exists)

				h := dynUserHandler{
					expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true,
					namespacesEnabled: true, namespaceSuspendedMessage: tc.suspendedMessage, namespaces: ns,
				}
				res := h.importUsers(importOne(strongRecord(true)), principal)
				parsed, ok := res.(*experimental.ImportUsersUnprocessableEntity)
				require.True(t, ok, "got %T", res)
				require.Len(t, parsed.Payload.Error, 1)
				assert.Equal(t, tc.wantMsg, parsed.Payload.Error[0].Message)
			})
		}
	})

	t.Run("rejects a supplied namespace on a non-namespaced cluster", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		// Authorization passes so the request reaches the namespace check and its 422.
		nsKey := apikey.MakeUserKey("bob", "ns1")
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(nsKey)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(nsKey)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: false}
		res := h.importUsers(importOne(strongRecord(true)), principal)
		_, ok := res.(*experimental.ImportUsersUnprocessableEntity)
		assert.True(t, ok)
	})

	t.Run("reports clobber when the identifier maps to a different user", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{}, nil)
		dynUser.On("CheckUserIdentifierExists", "identifier-16-ch").Return(true, nil)
		// CreateUser is deliberately not mocked: it must not be called on a clobber.

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "different target user")
	})

	t.Run("errors when an existing id holds a different identifier", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "other-identifier", Active: true, Status: dbuser.ExportStatusExported}}, nil)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
	})

	t.Run("reconciles active state for an existing same-identifier user", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		// The stored user is active but the record says inactive, so import must deactivate it.
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: true, Status: dbuser.ExportStatusExported}}, nil)
		dynUser.On("DeactivateUser", mock.Anything, key, false).Return(nil)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(false)), principal))
		require.Equal(t, models.UserImportResultStatusReconciled, *result.Status)
	})

	t.Run("forbidden when the caller holds create but not update", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(errors.New("forbidden"))
		// No store method is mocked: the whole batch is refused before any read or write.
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: namespaces.NewMockExister(t)}
		res := h.importUsers(importOne(strongRecord(false)), principal)
		_, ok := res.(*experimental.ImportUsersForbidden)
		assert.True(t, ok)
	})

	t.Run("skips an existing same-identifier user already in the recorded state", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: true, Status: dbuser.ExportStatusExported}}, nil)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusSkippedExists, *result.Status)
	})

	t.Run("reports partial failure when deactivation fails after create", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{}, nil)
		dynUser.On("CheckUserIdentifierExists", "identifier-16-ch").Return(false, nil)
		dynUser.On("CreateUser", mock.Anything, key, strongHash, "identifier-16-ch", "abc", "ns1", mock.Anything, mock.Anything).Return(nil)
		dynUser.On("DeactivateUser", mock.Anything, key, false).Return(errors.New("raft down"))

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(false)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "re-run to reconcile")
	})

	t.Run("rejects a record whose secure hash is not a usable argon2id hash", func(t *testing.T) {
		cases := []struct {
			name string
			hash string
		}{
			{name: "empty", hash: ""},
			{name: "malformed", hash: "not-an-argon2id-hash"},
			// These two decode cleanly. Argon2 panics on them at login, so the
			// import is the only place left to refuse them.
			{name: "zero iterations", hash: zeroIterationHash},
			{name: "zero parallelism", hash: zeroParallelHash},
			{name: "zero memory", hash: zeroMemoryHash},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				authorizer := authorization.NewMockAuthorizer(t)
				authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
				authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
				// Neither ExportUsers nor CreateUser is mocked: validation must
				// reject the record before any store call.
				dynUser := NewMockDbUserAndRolesGetter(t)

				rec := strongRecord(true)
				rec.SecureHash = tc.hash

				h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
				result := firstResult(t, h.importUsers(importOne(rec), principal))
				require.Equal(t, models.UserImportResultStatusError, *result.Status)
				require.Contains(t, result.Error, "argon2id")
			})
		}
	})

	t.Run("rejects a record whose identifier is not the key identifier length", func(t *testing.T) {
		cases := []struct {
			name       string
			identifier string
		}{
			{name: "empty", identifier: ""},
			{name: "too short", identifier: "short"},
			{name: "too long", identifier: "identifier-17-chr"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				authorizer := authorization.NewMockAuthorizer(t)
				authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
				authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
				// An identifier of any other length can never resolve a key, so the
				// user would be created unable to log in.
				dynUser := NewMockDbUserAndRolesGetter(t)

				rec := strongRecord(true)
				rec.UserIdentifier = tc.identifier

				h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
				result := firstResult(t, h.importUsers(importOne(rec), principal))
				require.Equal(t, models.UserImportResultStatusError, *result.Status)
				require.Contains(t, result.Error, "userIdentifier must be exactly 16 characters")
			})
		}
	})

	t.Run("refuses to shadow a static api key user", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		// No store method is mocked: the record is refused before any store call.
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{
			expiry:               unlicensedExpiry,
			rbacConfig:           rootOnly,
			dbUsers:              dynUser,
			authorizer:           authorizer,
			dbUserEnabled:        true,
			namespacesEnabled:    true,
			namespaces:           activeNsExister(t),
			staticApiKeysConfigs: config.StaticAPIKey{Enabled: true, Users: []string{key}},
		}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "static api key name")
	})

	t.Run("refuses an existing user whose key is revoked", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		// The identifier still matches, so without the status check this record
		// would be reported as reconciled while the key stays unusable.
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: false, Status: dbuser.ExportStatusRevoked}}, nil)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "revoked")
	})

	t.Run("refuses an existing user holding an imported static key", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		// An imported user authenticates on a weak hash that import cannot replace.
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: true, Status: dbuser.ExportStatusImportedKey}}, nil)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(false)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "imported static key")
	})

	t.Run("maps an apply-time user conflict to a per-record error", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(key)[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(key)[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)
		// The pre-checks pass, then the leader refuses: a concurrent create took
		// the id between the read and the apply.
		dynUser.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{}, nil)
		dynUser.On("CheckUserIdentifierExists", "identifier-16-ch").Return(false, nil)
		dynUser.On("CreateUser", mock.Anything, key, strongHash, "identifier-16-ch", "abc", "ns1", mock.Anything, mock.Anything).
			Return(fmt.Errorf("creating user: %w", apikey.ErrUserExists))

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(strongRecord(true)), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Equal(t, "a different credential already exists for this user id", result.Error)
	})

	t.Run("reports a null record as an error instead of panicking", func(t *testing.T) {
		authorizer := authorization.NewMockAuthorizer(t)
		// Swagger validation lets a null element through, so the key is built from an empty id.
		authorizer.On("Authorize", mock.Anything, principal, authorization.CREATE, authorization.Users(apikey.MakeUserKey("", "ns1"))[0]).Return(nil)
		authorizer.On("Authorize", mock.Anything, principal, authorization.UPDATE, authorization.Users(apikey.MakeUserKey("", "ns1"))[0]).Return(nil)
		dynUser := NewMockDbUserAndRolesGetter(t)

		h := dynUserHandler{expiry: unlicensedExpiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
		result := firstResult(t, h.importUsers(importOne(nil), principal))
		require.Equal(t, models.UserImportResultStatusError, *result.Status)
		require.Contains(t, result.Error, "null")
	})

	t.Run("expiresAt", func(t *testing.T) {
		licensed := wldbusers.NewValidatingExpiry()
		past := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
		future := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
		year10000InUTC := time.Date(9999, 12, 31, 23, 59, 59, 0, time.FixedZone("-01:00", -60*60))
		record := func(id string, active bool, expiresAt *time.Time) *models.DBUserCredential {
			rec := strongRecord(active)
			rec.UserID = strptr(id)
			rec.ExpiresAt = (*strfmt.DateTime)(expiresAt)
			return rec
		}
		expectCreate := func(expiresAt time.Time) func(*MockDbUserAndRolesGetter) {
			return func(m *MockDbUserAndRolesGetter) {
				m.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{}, nil)
				m.On("CheckUserIdentifierExists", "identifier-16-ch").Return(false, nil)
				m.On("CreateUser", mock.Anything, key, strongHash, "identifier-16-ch", "abc", "ns1", mock.Anything, expiresAt).Return(nil).Once()
			}
		}
		expectReconcile := func(m *MockDbUserAndRolesGetter) {
			m.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: true, Status: dbuser.ExportStatusExported, ExpiresAt: past}}, nil)
		}

		cases := []struct {
			name            string
			expiry          apikey.ExpiryResolver
			records         []*models.DBUserCredential
			store           func(*MockDbUserAndRolesGetter)
			wantResponse    middleware.Responder
			wantStatuses    []string
			wantErrContains string
		}{
			{
				name:         "unlicensed refuses the whole batch when a later record carries one",
				expiry:       unlicensedExpiry,
				records:      []*models.DBUserCredential{record("bob", true, nil), record("alice", true, &future)},
				wantResponse: &experimental.ImportUsersForbidden{},
			},
			{
				name:         "licensed refuses the whole batch with 422 when a later record's expiry is invalid",
				expiry:       licensed,
				records:      []*models.DBUserCredential{record("bob", true, &past), record("alice", true, &year10000InUTC)},
				wantResponse: &experimental.ImportUsersUnprocessableEntity{},
			},
			{
				name:         "licensed reports a null record and imports the next",
				expiry:       licensed,
				records:      []*models.DBUserCredential{nil, record("bob", true, &past)},
				store:        expectCreate(past),
				wantStatuses: []string{models.UserImportResultStatusError, models.UserImportResultStatusCreated},
			},
			{
				name:            "reconcile refuses a different future expiry",
				expiry:          licensed,
				records:         []*models.DBUserCredential{record("bob", true, &future)},
				store:           expectReconcile,
				wantStatuses:    []string{models.UserImportResultStatusError},
				wantErrContains: "change it with " + setExpirationRoute,
			},
			{
				name:    "reconcile refuses a different future expiry onto a deactivated user before activating it",
				expiry:  licensed,
				records: []*models.DBUserCredential{record("bob", true, &future)},
				store: func(m *MockDbUserAndRolesGetter) {
					m.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: false, Status: dbuser.ExportStatusExported, ExpiresAt: past}}, nil)
				},
				wantStatuses: []string{models.UserImportResultStatusError},
			},
			{
				name:    "reconcile refuses a passed expiry onto an active user without one, advising deactivation",
				expiry:  licensed,
				records: []*models.DBUserCredential{record("bob", true, &past)},
				store: func(m *MockDbUserAndRolesGetter) {
					m.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: true, Status: dbuser.ExportStatusExported}}, nil)
				},
				wantStatuses:    []string{models.UserImportResultStatusError},
				wantErrContains: "deactivate the user, or delete it and re-import",
			},
			{
				name:    "reconcile skips a passed expiry onto a deactivated user without re-activating it",
				expiry:  licensed,
				records: []*models.DBUserCredential{record("bob", true, &past)},
				store: func(m *MockDbUserAndRolesGetter) {
					m.On("ExportUsers", key).Return(map[string]dbuser.ExportRecord{key: {Id: key, UserIdentifier: "identifier-16-ch", Active: false, Status: dbuser.ExportStatusExported}}, nil)
				},
				wantStatuses: []string{models.UserImportResultStatusSkippedExists},
			},
			{
				name:         "reconcile without expiry keeps the existing one",
				expiry:       licensed,
				records:      []*models.DBUserCredential{record("bob", true, nil)},
				store:        expectReconcile,
				wantStatuses: []string{models.UserImportResultStatusSkippedExists},
			},
			{
				name:         "reconcile with an equal expiry skips",
				expiry:       licensed,
				records:      []*models.DBUserCredential{record("bob", true, new(past.In(time.FixedZone("+02:00", 2*60*60))))},
				store:        expectReconcile,
				wantStatuses: []string{models.UserImportResultStatusSkippedExists},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				keys := make([]string, 0, len(tc.records))
				for _, rec := range tc.records {
					keys = append(keys, apikey.MakeUserKey(bareUserID(rec), "ns1"))
				}
				authorizer := authorization.NewMockAuthorizer(t)
				for _, verb := range []string{authorization.CREATE, authorization.UPDATE} {
					args := []any{mock.Anything, principal, verb}
					for _, resource := range authorization.Users(keys...) {
						args = append(args, resource)
					}
					authorizer.On("Authorize", args...).Return(nil)
				}
				// A store call the row does not expect fails the test, so a refused
				// batch writes nothing.
				dynUser := NewMockDbUserAndRolesGetter(t)
				if tc.store != nil {
					tc.store(dynUser)
				}

				h := dynUserHandler{expiry: tc.expiry, rbacConfig: rootOnly, dbUsers: dynUser, authorizer: authorizer, dbUserEnabled: true, namespacesEnabled: true, namespaces: activeNsExister(t)}
				res := h.importUsers(experimental.ImportUsersParams{HTTPRequest: req, Body: &models.UserImportRequest{Namespace: "ns1", Users: tc.records}}, principal)

				if tc.wantResponse != nil {
					require.IsType(t, tc.wantResponse, res)
					return
				}
				parsed, ok := res.(*experimental.ImportUsersOK)
				require.True(t, ok)
				statuses := make([]string, 0, len(parsed.Payload.Results))
				for _, result := range parsed.Payload.Results {
					statuses = append(statuses, *result.Status)
				}
				require.Equal(t, tc.wantStatuses, statuses)
				if tc.wantErrContains != "" {
					require.Contains(t, parsed.Payload.Results[0].Error, tc.wantErrContains)
				}
			})
		}
	})
}
