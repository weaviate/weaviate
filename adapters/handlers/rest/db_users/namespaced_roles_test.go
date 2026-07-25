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
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

func collPolicy(verb, collection string) authorization.Policy {
	return authorization.Policy{Resource: authorization.Collections(collection)[0], Verb: verb, Domain: authorization.SchemaDomain}
}

// nsRolesAuthorizer models a confined namespaced caller reading a user in its own
// namespace: it may read that user, holds no ALL-scope on roles, and holds no
// role permission (so any role carrying a permission fails the content gate).
func nsRolesAuthorizer(t *testing.T, userResource string) *authorization.MockAuthorizer {
	t.Helper()
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, authorization.READ, userResource).Return(nil).Maybe()
	authorizer.On("Authorize", mock.Anything, mock.Anything,
		authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
		authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("forbidden")).Maybe()
	return authorizer
}

// TestGetUserNamespacedRolesStrippedAndFiltered pins that a confined caller
// reading a user in its own namespace sees own-namespace role names with the
// prefix stripped, and does not see foreign-namespace roles or global roles whose
// permissions it does not hold.
func TestGetUserNamespacedRolesStrippedAndFiltered(t *testing.T) {
	principal := &models.Principal{Username: "customer1:admin", UserType: models.UserTypeInputDb, Namespace: "customer1"}
	authorizer := nsRolesAuthorizer(t, authorization.Users("customer1:bob")[0])

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "customer1:bob").Return(map[string]apikey.UserView{"customer1:bob": {Id: "customer1:bob"}}, nil)
	dynUser.On("GetRolesForUserOrGroup", "customer1:bob", authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{
		"customer1:editor": {},                                           // own-namespace local role, no permissions to gate
		"customer2:secret": {},                                           // foreign namespace, hidden by name
		"restricted":       {collPolicy(authorization.CREATE, "Secret")}, // global role the caller does not hold
	}, nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		rbacConfig:        rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "bob", HTTPRequest: req}, principal)
	parsed, ok := res.(*users.GetUserInfoOK)
	require.True(t, ok, "got %T", res)
	require.Equal(t, []string{"editor"}, parsed.Payload.Roles)
}

// TestGetUserNamespacedOwnRolesKept pins that a subject reading its own user keeps
// every assigned role (the content gate is skipped for a self-read), with the own
// namespace prefix stripped.
func TestGetUserNamespacedOwnRolesKept(t *testing.T) {
	principal := &models.Principal{Username: "customer1:bob", UserType: models.UserTypeInputDb, Namespace: "customer1"}
	authorizer := nsRolesAuthorizer(t, authorization.Users("customer1:bob")[0])

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers", "customer1:bob").Return(map[string]apikey.UserView{"customer1:bob": {Id: "customer1:bob"}}, nil)
	dynUser.On("GetRolesForUserOrGroup", "customer1:bob", authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{
		"customer1:editor": {},
		"restricted":       {collPolicy(authorization.CREATE, "Secret")},
	}, nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		rbacConfig:        rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.getUser(users.GetUserInfoParams{UserID: "bob", HTTPRequest: req}, principal)
	parsed, ok := res.(*users.GetUserInfoOK)
	require.True(t, ok, "got %T", res)
	require.ElementsMatch(t, []string{"editor", "restricted"}, parsed.Payload.Roles)
}

// TestListUsersNamespacedRolesStrippedAndFiltered pins the same strip-and-filter
// behavior on the list endpoint.
func TestListUsersNamespacedRolesStrippedAndFiltered(t *testing.T) {
	principal := &models.Principal{Username: "customer1:admin", UserType: models.UserTypeInputDb, Namespace: "customer1"}
	authorizer := nsRolesAuthorizer(t, authorization.Users("customer1:bob")[0])
	// Resource filter authorizes the shared parent of the returned users.
	authorizer.On("Authorize", mock.Anything, principal, authorization.READ, mock.Anything).Return(nil).Maybe()

	dynUser := NewMockDbUserAndRolesGetter(t)
	dynUser.On("GetUsers").Return(map[string]apikey.UserView{"customer1:bob": {Id: "customer1:bob"}}, nil)
	dynUser.On("GetRolesForUserOrGroup", "customer1:bob", authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{
		"customer1:editor": {},
		"customer2:secret": {},
		"restricted":       {collPolicy(authorization.CREATE, "Secret")},
	}, nil)

	h := dynUserHandler{
		dbUsers:           dynUser,
		authorizer:        authorizer,
		rbacConfig:        rbacconf.Config{Enabled: true, RootUsers: []string{"root"}},
		dbUserEnabled:     true,
		namespacesEnabled: true,
	}

	res := h.listUsers(users.ListAllUsersParams{HTTPRequest: req}, principal)
	parsed, ok := res.(*users.ListAllUsersOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	require.Equal(t, []string{"editor"}, parsed.Payload[0].Roles)
}
