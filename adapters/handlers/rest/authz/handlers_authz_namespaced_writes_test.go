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
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

// nsWriteRoles holds a namespaced caller's own local role and a global role it
// must not be able to modify.
func nsWriteRoles() map[string][]authorization.Policy {
	return map[string][]authorization.Policy{
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
		"globalauditor":    {collPolicy(authorization.READ, "Movies")},
	}
}

// nsWriteHandler wires an authZHandlers whose authorizer models the role-name
// matcher: a namespaced caller has no ALL-scope on roles and holds MATCH-scope
// only on roles inside its own namespace; AuthorizeSilent grants any resource in
// that namespace, standing in for the caller's effective permissions.
func nsWriteHandler(t *testing.T, all map[string][]authorization.Policy, ownNamespace string) (*authZHandlers, *MockControllerAndGetUsers) {
	t.Helper()
	prefix := ownNamespace + ":"
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
			if strings.HasSuffix(verb, "_"+authorization.ROLE_SCOPE_MATCH) &&
				len(resources) == 1 && strings.Contains(resources[0], prefix) {
				return nil
			}
			return fmt.Errorf("forbidden")
		}).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
			if len(resources) == 1 && strings.Contains(resources[0], prefix) {
				return nil
			}
			return fmt.Errorf("forbidden")
		}).Maybe()

	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", mock.Anything).Return(func(names ...string) map[string][]authorization.Policy {
		out := map[string][]authorization.Policy{}
		if p, ok := all[names[0]]; ok {
			out[names[0]] = p
		}
		return out
	}, nil).Maybe()
	controller.On("UpdateRolesPermissions", mock.Anything).Return(nil).Maybe()
	controller.On("RemovePermissions", mock.Anything, mock.Anything).Return(nil).Maybe()
	controller.On("DeleteRoles", mock.Anything).Return(nil).Maybe()

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}
	return h, controller
}

// nsWritePerm is a bare permission a namespaced caller submits; it is qualified
// into the caller's namespace before the matcher and effective-permission checks.
func nsWritePerm() *models.Permission {
	return &models.Permission{
		Action:      String(authorization.CreateCollections),
		Collections: &models.PermissionCollections{Collection: String("Films")},
	}
}

// permRespKind maps the add and remove permission responder types to a common
// status label so the two write paths can share table-driven cases.
func permRespKind(res any) string {
	switch res.(type) {
	case *authz.AddPermissionsOK, *authz.RemovePermissionsOK:
		return "ok"
	case *authz.AddPermissionsForbidden, *authz.RemovePermissionsForbidden:
		return "403"
	case *authz.AddPermissionsBadRequest, *authz.RemovePermissionsBadRequest:
		return "400"
	case *authz.AddPermissionsNotFound, *authz.RemovePermissionsNotFound:
		return "404"
	default:
		return fmt.Sprintf("%T", res)
	}
}

func TestModifyPermissionsNamespaced(t *testing.T) {
	tests := []struct {
		name     string
		roleID   string
		wantType string
	}{
		{name: "own local role updated", roleID: "editor", wantType: "ok"},
		{name: "global role forbidden", roleID: "globalauditor", wantType: "403"},
		{name: "qualified input rejected", roleID: "customer2:editor", wantType: "400"},
		{name: "built-in role rejected", roleID: authorization.BuiltInRoles[0], wantType: "400"},
		{name: "missing role not found", roleID: "ghost", wantType: "404"},
	}

	for _, op := range []string{"add", "remove"} {
		for _, tt := range tests {
			t.Run(op+"/"+tt.name, func(t *testing.T) {
				h, _ := nsWriteHandler(t, nsWriteRoles(), "customer1")
				principal := &models.Principal{Username: "u", Namespace: "customer1"}
				perms := []*models.Permission{nsWritePerm()}

				var res any
				if op == "add" {
					res = h.addPermissions(authz.AddPermissionsParams{
						HTTPRequest: req,
						ID:          tt.roleID,
						Body:        authz.AddPermissionsBody{Permissions: perms},
					}, principal)
				} else {
					res = h.removePermissions(authz.RemovePermissionsParams{
						HTTPRequest: req,
						ID:          tt.roleID,
						Body:        authz.RemovePermissionsBody{Permissions: perms},
					}, principal)
				}
				require.Equal(t, tt.wantType, permRespKind(res), "got %T", res)
			})
		}
	}
}

// TestAddPermissionsNamespacedEffectiveDeny pins the per-permission must-already-hold
// MATCH check on the add path: a namespaced caller that may manage its own local
// role at MATCH scope but does not hold the submitted permission gets 403. The
// shared nsWriteHandler grants every own-namespace resource, so the deny arm
// needs its own authorizer.
func TestAddPermissionsNamespacedEffectiveDeny(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything,
		authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_MATCH),
		mock.Anything).Return(nil)
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("forbidden"))
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", "customer1:editor").Return(map[string][]authorization.Policy{
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
	}, nil).Maybe()

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true},
		namespacesEnabled: true,
	}
	principal := &models.Principal{Username: "u", Namespace: "customer1"}
	res := h.addPermissions(authz.AddPermissionsParams{
		HTTPRequest: req,
		ID:          "editor",
		Body:        authz.AddPermissionsBody{Permissions: []*models.Permission{nsWritePerm()}},
	}, principal)
	_, ok := res.(*authz.AddPermissionsForbidden)
	require.True(t, ok, "got %T", res)
}

func TestDeleteRoleNamespaced(t *testing.T) {
	tests := []struct {
		name     string
		roleID   string
		wantType string
	}{
		{name: "own local role deleted", roleID: "editor", wantType: "204"},
		{name: "global role forbidden", roleID: "globalauditor", wantType: "403"},
		{name: "qualified input rejected", roleID: "customer2:editor", wantType: "400"},
		{name: "built-in role rejected", roleID: authorization.BuiltInRoles[0], wantType: "400"},
		{name: "missing role idempotent no content", roleID: "ghost", wantType: "204"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := nsWriteHandler(t, nsWriteRoles(), "customer1")
			principal := &models.Principal{Username: "u", Namespace: "customer1"}
			res := h.deleteRole(authz.DeleteRoleParams{HTTPRequest: req, ID: tt.roleID}, principal)
			switch tt.wantType {
			case "204":
				_, ok := res.(*authz.DeleteRoleNoContent)
				require.True(t, ok, "got %T", res)
			case "403":
				_, ok := res.(*authz.DeleteRoleForbidden)
				require.True(t, ok, "got %T", res)
			case "400":
				_, ok := res.(*authz.DeleteRoleBadRequest)
				require.True(t, ok, "got %T", res)
			}
		})
	}
}
