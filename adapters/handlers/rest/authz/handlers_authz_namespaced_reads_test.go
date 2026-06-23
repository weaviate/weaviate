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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

// collPolicy builds a schema-domain policy for a (possibly namespace-qualified)
// collection so PoliciesToPermission round-trips it in the read handlers.
func collPolicy(verb, collection string) authorization.Policy {
	return authorization.Policy{
		Resource: authorization.Collections(collection)[0],
		Verb:     verb,
		Domain:   authorization.SchemaDomain,
	}
}

func policyKey(p authorization.Policy) string { return p.Verb + "\x00" + p.Resource }

// nsReadHandler wires an authZHandlers whose authorizer models a caller's
// effective permissions: an operator short-circuits read-all on roles; everyone
// else "holds" exactly the policies in held. The controller serves the role map
// for both the no-arg list and per-name lookups.
func nsReadHandler(t *testing.T, isOperator bool, all map[string][]authorization.Policy, held map[string]bool, rootUsers []string) (*authZHandlers, *MockControllerAndGetUsers) {
	t.Helper()
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything,
		authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
		authorization.Roles()[0]).
		Return(func(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
			if isOperator {
				return nil
			}
			return fmt.Errorf("forbidden")
		}).Maybe()
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
			if len(resources) == 1 && held[verb+"\x00"+resources[0]] {
				return nil
			}
			return fmt.Errorf("forbidden")
		}).Maybe()

	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles").Return(all, nil).Maybe()
	controller.On("GetRoles", mock.Anything).Return(func(names ...string) map[string][]authorization.Policy {
		out := map[string][]authorization.Policy{}
		if p, ok := all[names[0]]; ok {
			out[names[0]] = p
		}
		return out
	}, nil).Maybe()

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{
		authorizer:        authorizer,
		controller:        controller,
		logger:            logger,
		rbacconfig:        rbacconf.Config{Enabled: true, RootUsers: rootUsers},
		namespacesEnabled: true,
	}
	return h, controller
}

// nsRoles is the shared role layout: two built-ins, two local roles in distinct
// namespaces, plus root/read-only that a namespaced caller must never see.
func nsRoles() map[string][]authorization.Policy {
	return map[string][]authorization.Policy{
		"admin":            {collPolicy(authorization.CREATE, "Movies")},
		"viewer":           {collPolicy(authorization.READ, "Movies")},
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
		"customer2:editor": {collPolicy(authorization.CREATE, "customer2:Films")},
		// A foreign-namespace role whose only policy is in a customer1 admin's
		// envelope: only the name pre-filter (not content-match) hides it.
		"customer2:probe":  {collPolicy(authorization.CREATE, "Movies")},
		authorization.Root: {collPolicy(authorization.CREATE, "Secret")},
		"read-only":        {collPolicy(authorization.READ, "Everything")},
	}
}

// adminHeld is a customer1 admin's effective permissions: namespace-scoped, so
// a global template role's policies match only after projection into customer1.
func adminHeld() map[string]bool {
	return map[string]bool{
		policyKey(collPolicy(authorization.CREATE, "customer1:Movies")): true,
		policyKey(collPolicy(authorization.READ, "customer1:Movies")):   true,
		policyKey(collPolicy(authorization.CREATE, "customer1:Films")):  true,
	}
}

func roleNames(roles []*models.Role) []string {
	out := make([]string, 0, len(roles))
	for _, r := range roles {
		out = append(out, *r.Name)
	}
	return out
}

func TestGetRolesNamespacedVisibility(t *testing.T) {
	tests := []struct {
		name       string
		principal  *models.Principal
		isOperator bool
		held       map[string]bool
		rootUsers  []string
		wantNames  []string
	}{
		{
			name:      "namespaced admin sees built-ins and own local role, stripped",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			held:      adminHeld(),
			wantNames: []string{"admin", "editor", "viewer"},
		},
		{
			name:      "namespaced viewer sees only viewer, not admin",
			principal: &models.Principal{Username: "v", Namespace: "customer1"},
			held: map[string]bool{
				policyKey(collPolicy(authorization.READ, "customer1:Movies")): true,
			},
			wantNames: []string{"viewer"},
		},
		{
			name:       "operator root sees every role unstripped",
			principal:  &models.Principal{Username: "op"},
			isOperator: true,
			rootUsers:  []string{"op"},
			wantNames:  []string{"admin", "customer1:editor", "customer2:editor", "customer2:probe", "read-only", "root", "viewer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := nsReadHandler(t, tt.isOperator, nsRoles(), tt.held, tt.rootUsers)
			res := h.getRoles(authz.GetRolesParams{HTTPRequest: req}, tt.principal)
			parsed, ok := res.(*authz.GetRolesOK)
			require.True(t, ok)
			require.ElementsMatch(t, tt.wantNames, roleNames(parsed.Payload))
		})
	}
}

func TestGetRoleNamespacedVisibility(t *testing.T) {
	tests := []struct {
		name       string
		principal  *models.Principal
		isOperator bool
		held       map[string]bool
		rootUsers  []string
		roleID     string
		wantType   string
		wantName   string
	}{
		{
			name:      "own local role visible and stripped",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			held:      adminHeld(),
			roleID:    "editor",
			wantType:  "ok",
			wantName:  "editor",
		},
		{
			name:      "in-envelope global role visible",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			held:      adminHeld(),
			roleID:    "viewer",
			wantType:  "ok",
			wantName:  "viewer",
		},
		{
			name:      "out-of-envelope role forbidden not found",
			principal: &models.Principal{Username: "v", Namespace: "customer1"},
			held: map[string]bool{
				policyKey(collPolicy(authorization.READ, "customer1:Movies")): true,
			},
			roleID:   "admin",
			wantType: "403",
		},
		{
			name:      "missing role not found",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			held:      adminHeld(),
			roleID:    "ghost",
			wantType:  "404",
		},
		{
			name:       "operator reads foreign local role unstripped",
			principal:  &models.Principal{Username: "op"},
			isOperator: true,
			roleID:     "customer2:editor",
			wantType:   "ok",
			wantName:   "customer2:editor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := nsReadHandler(t, tt.isOperator, nsRoles(), tt.held, tt.rootUsers)
			res := h.getRole(authz.GetRoleParams{HTTPRequest: req, ID: tt.roleID}, tt.principal)
			switch tt.wantType {
			case "ok":
				parsed, ok := res.(*authz.GetRoleOK)
				require.True(t, ok, "got %T", res)
				require.Equal(t, tt.wantName, *parsed.Payload.Name)
			case "403":
				_, ok := res.(*authz.GetRoleForbidden)
				require.True(t, ok, "got %T", res)
			case "404":
				_, ok := res.(*authz.GetRoleNotFound)
				require.True(t, ok, "got %T", res)
			}
		})
	}
}

func TestHasPermissionNamespacedVisibility(t *testing.T) {
	// A permission the customer1 admin holds, expressed in its own (bare) terms.
	heldPerm := &models.Permission{
		Action:      String(authorization.CreateCollections),
		Collections: &models.PermissionCollections{Collection: String("Films")},
	}

	t.Run("in-envelope role returns controller verdict", func(t *testing.T) {
		h, controller := nsReadHandler(t, false, nsRoles(), adminHeld(), nil)
		controller.On("HasPermission", "customer1:editor", mock.Anything).Return(true, nil)

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		res := h.hasPermission(authz.HasPermissionParams{HTTPRequest: req, ID: "editor", Body: heldPerm}, principal)
		parsed, ok := res.(*authz.HasPermissionOK)
		require.True(t, ok, "got %T", res)
		require.True(t, parsed.Payload)
	})

	t.Run("out-of-envelope role forbidden", func(t *testing.T) {
		held := map[string]bool{
			policyKey(collPolicy(authorization.READ, "customer1:Movies")): true,
		}
		h, _ := nsReadHandler(t, false, nsRoles(), held, nil)

		principal := &models.Principal{Username: "v", Namespace: "customer1"}
		res := h.hasPermission(authz.HasPermissionParams{HTTPRequest: req, ID: "admin", Body: heldPerm}, principal)
		_, ok := res.(*authz.HasPermissionForbidden)
		require.True(t, ok, "got %T", res)
	})
}

func TestGetRolePermissionStripping(t *testing.T) {
	principal := &models.Principal{Username: "u", Namespace: "customer1"}

	t.Run("getRole strips own-namespace permission resources", func(t *testing.T) {
		h, _ := nsReadHandler(t, false, nsRoles(), adminHeld(), nil)
		res := h.getRole(authz.GetRoleParams{HTTPRequest: req, ID: "editor"}, principal)
		parsed, ok := res.(*authz.GetRoleOK)
		require.True(t, ok, "got %T", res)
		require.Equal(t, "editor", *parsed.Payload.Name)
		require.Equal(t, "Films", *parsed.Payload.Permissions[0].Tenants.Collection)
	})

	t.Run("operator getRole keeps foreign-namespace resources raw", func(t *testing.T) {
		h, _ := nsReadHandler(t, true, nsRoles(), nil, nil)
		res := h.getRole(authz.GetRoleParams{HTTPRequest: req, ID: "customer2:editor"}, &models.Principal{Username: "op"})
		parsed, ok := res.(*authz.GetRoleOK)
		require.True(t, ok, "got %T", res)
		require.Equal(t, "customer2:Films", *parsed.Payload.Permissions[0].Tenants.Collection)
	})

	t.Run("getRoles strips own-namespace permission resources", func(t *testing.T) {
		h, _ := nsReadHandler(t, false, nsRoles(), adminHeld(), nil)
		res := h.getRoles(authz.GetRolesParams{HTTPRequest: req}, principal)
		parsed, ok := res.(*authz.GetRolesOK)
		require.True(t, ok, "got %T", res)
		var editor *models.Role
		for _, r := range parsed.Payload {
			if *r.Name == "editor" {
				editor = r
			}
		}
		require.NotNil(t, editor)
		require.Equal(t, "Films", *editor.Permissions[0].Tenants.Collection)
	})
}
