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

package authz

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// nsCrossAuthorizer models a non-operator namespaced caller: it holds no
// ALL-scope on roles, and AuthorizeSilent grants any resource carrying the
// caller's own namespace (role policies and own-namespace user reads).
func nsCrossAuthorizer(t *testing.T, ns string) *authorization.MockAuthorizer {
	t.Helper()
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything,
		authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
		authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
			if len(resources) == 1 && strings.Contains(resources[0], ns+":") {
				return nil
			}
			return fmt.Errorf("forbidden")
		}).Maybe()
	return authorizer
}

func TestGetUsersForRoleNamespaced(t *testing.T) {
	authorizer := nsCrossAuthorizer(t, "customer1")
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", "customer1:editor").Return(map[string][]authorization.Policy{
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
	}, nil).Maybe()
	controller.On("GetUsersOrGroupForRole", "customer1:editor", authentication.AuthTypeOIDC, false).Return([]string{}, nil)
	controller.On("GetUsersOrGroupForRole", "customer1:editor", authentication.AuthTypeDb, false).Return([]string{"customer1:bob"}, nil)
	controller.On("GetUsers", "customer1:bob").Return(map[string]apikey.UserView{"customer1:bob": {}}, nil)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}

	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}
	res := h.getUsersForRole(authz.GetUsersForRoleParams{HTTPRequest: req, ID: "editor"}, principal)
	parsed, ok := res.(*authz.GetUsersForRoleOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	// Short role name resolved to the local role, and the DB-user id stripped.
	require.Equal(t, "bob", parsed.Payload[0].UserID)
}

func TestGetUsersForRoleNamespacedOutOfEnvelope(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything,
		authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
		authorization.Roles()[0]).Return(fmt.Errorf("forbidden")).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("forbidden")).Maybe()
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", "admin").Return(map[string][]authorization.Policy{
		"admin": {collPolicy(authorization.CREATE, "Movies")},
	}, nil).Maybe()
	controller.On("GetRoles", "customer1:admin").Return(map[string][]authorization.Policy{}, nil).Maybe()

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}

	principal := &models.Principal{Username: "customer1:viewer", UserType: "db", Namespace: "customer1"}
	res := h.getUsersForRole(authz.GetUsersForRoleParams{HTTPRequest: req, ID: "admin"}, principal)
	_, ok := res.(*authz.GetUsersForRoleForbidden)
	require.True(t, ok, "got %T", res)
}

func TestGetGroupsForRoleNamespaced(t *testing.T) {
	authorizer := nsCrossAuthorizer(t, "customer1")
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", "customer1:editor").Return(map[string][]authorization.Policy{
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
	}, nil).Maybe()
	controller.On("GetUsersOrGroupForRole", "customer1:editor", authentication.AuthTypeOIDC, true).Return([]string{"engineers"}, nil)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}

	// The caller's own group is returned unstripped (groups stay globally named).
	principal := &models.Principal{Username: "customer1:admin", UserType: "oidc", Namespace: "customer1", Groups: []string{"engineers"}}
	res := h.getGroupsForRole(authz.GetGroupsForRoleParams{HTTPRequest: req, ID: "editor"}, principal)
	parsed, ok := res.(*authz.GetGroupsForRoleOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	require.Equal(t, "engineers", parsed.Payload[0].GroupID)
}

func TestGetRolesForUserNamespacedStripsOwn(t *testing.T) {
	authorizer := nsCrossAuthorizer(t, "customer1")
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRolesForUserOrGroup", "customer1:u", authentication.AuthTypeDb, false).Return(map[string][]authorization.Policy{
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
	}, nil)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		apiKeysConfigs:    config.StaticAPIKey{Enabled: true, Users: []string{"customer1:u"}},
		namespacesEnabled: true,
	}

	includeFull := true
	// Reading one's own roles: names and permission bodies are stripped.
	principal := &models.Principal{Username: "customer1:u", UserType: "db", Namespace: "customer1"}
	res := h.getRolesForUser(authz.GetRolesForUserParams{HTTPRequest: req, ID: "u", UserType: "db", IncludeFullRoles: &includeFull}, principal)
	parsed, ok := res.(*authz.GetRolesForUserOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	require.Equal(t, "editor", *parsed.Payload[0].Name)
	require.Equal(t, "Films", *parsed.Payload[0].Permissions[0].Tenants.Collection)
}

func TestGetRolesForGroupNamespacedStripsOwn(t *testing.T) {
	authorizer := nsCrossAuthorizer(t, "customer1")
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRolesForUserOrGroup", "engineers", authentication.AuthTypeOIDC, true).Return(map[string][]authorization.Policy{
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
	}, nil)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}

	includeFull := true
	// Reading one's own group's roles: names and permission bodies are stripped.
	principal := &models.Principal{Username: "customer1:u", UserType: "oidc", Namespace: "customer1", Groups: []string{"engineers"}}
	res := h.getRolesForGroup(authz.GetRolesForGroupParams{HTTPRequest: req, ID: "engineers", GroupType: string(authentication.AuthTypeOIDC), IncludeFullRoles: &includeFull}, principal)
	parsed, ok := res.(*authz.GetRolesForGroupOK)
	require.True(t, ok, "got %T", res)
	require.Len(t, parsed.Payload, 1)
	require.Equal(t, "editor", *parsed.Payload[0].Name)
	require.Equal(t, "Films", *parsed.Payload[0].Permissions[0].Tenants.Collection)
}
