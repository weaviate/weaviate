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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/schema"
)

// allScopedRolesPermission is the escalation-grade permission that may never be
// stored in a role on an NS cluster: role management at ALL scope.
func allScopedRolesPermission() *models.Permission {
	scope := models.PermissionRolesScopeAll
	return &models.Permission{
		Action: String(authorization.UpdateRoles),
		Roles:  &models.PermissionRoles{Role: authorization.All, Scope: &scope},
	}
}

// permissiveAuthorizer grants everything — so a denial proves the content guard
// fired, not a lack of privilege.
func permissiveAuthorizer(t *testing.T) *authorization.MockAuthorizer {
	t.Helper()
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	return authorizer
}

// On NS clusters no role may carry ALL-scoped role management, regardless of who
// creates it (operator included) — only root's wildcard grants it.
func TestCreateRoleAllScopedRolesDeniedOnNamespaceCluster(t *testing.T) {
	controller := NewMockControllerAndGetUsers(t)
	// Reachable only if the guard fails to fire (a regression returns 201).
	controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil).Maybe()
	controller.On("CreateRolesPermissions", mock.Anything).Return(nil).Maybe()
	logger, _ := test.NewNullLogger()

	h := &authZHandlers{
		authorizer:        permissiveAuthorizer(t),
		controller:        controller,
		schemaReader:      schema.NewMockSchemaGetter(t),
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}
	// The guard is not caller-conditional, so a global operator — the most
	// privileged caller — is the strongest "no caller can bypass" assertion.
	res := h.createRole(authz.CreateRoleParams{
		HTTPRequest: req,
		Body:        &models.Role{Name: String("superrole"), Permissions: []*models.Permission{allScopedRolesPermission()}},
	}, &models.Principal{Username: "root"})
	_, ok := res.(*authz.CreateRoleForbidden)
	require.True(t, ok, "got %T", res)
}

// The same rule applies to addPermissions: an ALL-scoped roles permission can't
// be added to any role on an NS cluster.
func TestAddPermissionsAllScopedRolesDeniedOnNamespaceCluster(t *testing.T) {
	controller := NewMockControllerAndGetUsers(t)
	// Reachable only if the guard fails to fire (a regression returns 200).
	controller.On("GetRoles", mock.Anything).Return(map[string][]authorization.Policy{
		"editor": {collPolicy(authorization.READ, "Movies")},
	}, nil).Maybe()
	controller.On("UpdateRolesPermissions", mock.Anything).Return(nil).Maybe()
	logger, _ := test.NewNullLogger()

	h := &authZHandlers{
		authorizer:        permissiveAuthorizer(t),
		controller:        controller,
		schemaReader:      schema.NewMockSchemaGetter(t),
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}
	res := h.addPermissions(authz.AddPermissionsParams{
		HTTPRequest: req,
		ID:          "editor",
		Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{allScopedRolesPermission()}},
	}, &models.Principal{Username: "root"})
	_, ok := res.(*authz.AddPermissionsForbidden)
	require.True(t, ok, "got %T", res)
}

// Backstop: even if a namespaced principal somehow holds an ALL-scoped roles
// grant (stale data, or an operator mutating an assigned role),
// authorizeRoleScopes must not honor the ALL short-circuit — a write still
// falls through to the MATCH must-already-hold check and is denied.
func TestAuthorizeRoleScopesIgnoresAllGrantForNamespaced(t *testing.T) {
	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}
	authorizer := authorization.NewMockAuthorizer(t)
	// ALL and MATCH role authorization both granted (models a pre-existing ALL
	// grant), but the caller holds none of the underlying permissions.
	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(fmt.Errorf("forbidden")).Maybe()
	controller := NewMockControllerAndGetUsers(t)
	// Reachable only if the ALL short-circuit wrongly fires (a regression returns 201).
	controller.On("GetRoles").Return(map[string][]authorization.Policy{}, nil).Maybe()
	controller.On("CreateRolesPermissions", mock.Anything).Return(nil).Maybe()
	logger, _ := test.NewNullLogger()

	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		schemaReader:      schema.NewMockSchemaGetter(t),
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}
	res := h.createRole(authz.CreateRoleParams{
		HTTPRequest: req,
		Body: &models.Role{
			Name:        String("editor"),
			Permissions: []*models.Permission{{Action: String(authorization.CreateCollections)}},
		},
	}, principal)
	_, ok := res.(*authz.CreateRoleForbidden)
	require.True(t, ok, "got %T", res)
}
