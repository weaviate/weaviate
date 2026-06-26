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
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

// nsAssignRoles is the role fixture for assignment tests: a namespaced caller's
// own local role, a global template role within the caller's permissions, and a
// global role carrying a cluster permission it does not hold.
func nsAssignRoles() map[string][]authorization.Policy {
	return map[string][]authorization.Policy{
		"customer1:editor": {collPolicy(authorization.CREATE, "customer1:Films")},
		"viewer":           {collPolicy(authorization.READ, "Movies")},
		"admin":            {{Resource: authorization.Cluster(), Verb: authorization.READ, Domain: authorization.ClusterDomain}},
	}
}

// nsAssignHandler models a namespaced caller whose effective permissions cover
// only its own namespace: USER_AND_GROUP_ASSIGN_AND_REVOKE is granted on its own
// users, and AuthorizeSilent grants any resource carrying the caller's prefix
// (so a role within the caller's permissions passes the must-already-hold check
// and one beyond them does not).
func nsAssignHandler(t *testing.T, ns string) (*authZHandlers, *MockControllerAndGetUsers) {
	t.Helper()
	prefix := ns + ":"
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
			if len(resources) == 1 && strings.Contains(resources[0], prefix) {
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

	all := nsAssignRoles()
	getRoles := func(names ...string) map[string][]authorization.Policy {
		out := map[string][]authorization.Policy{}
		for _, n := range names {
			if p, ok := all[n]; ok {
				out[n] = p
			}
		}
		return out
	}
	controller := NewMockControllerAndGetUsers(t)
	// One expectation per call arity: resolution looks roles up one at a time,
	// the must-already-hold fetch passes the whole resolved slice.
	controller.On("GetRoles", mock.Anything).Return(getRoles, nil).Maybe()
	controller.On("GetRoles", mock.Anything, mock.Anything).Return(getRoles, nil).Maybe()
	controller.On("GetUsers", prefix+"bob").Return(map[string]apikey.UserView{prefix + "bob": {}}, nil).Maybe()
	controller.On("AddRolesForUser", mock.Anything, mock.Anything).Return(nil).Maybe()
	controller.On("RevokeRolesForUser", mock.Anything, mock.Anything).Return(nil).Maybe()

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

func TestAssignRoleToUserNamespaced(t *testing.T) {
	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}

	tests := []struct {
		name   string
		role   string
		assert func(t *testing.T, res any)
	}{
		{
			name: "own local role",
			role: "editor",
			assert: func(t *testing.T, res any) {
				_, ok := res.(*authz.AssignRoleToUserOK)
				require.True(t, ok, "got %T", res)
			},
		},
		{
			name: "global template role within caller permissions",
			role: "viewer",
			assert: func(t *testing.T, res any) {
				_, ok := res.(*authz.AssignRoleToUserOK)
				require.True(t, ok, "got %T", res)
			},
		},
		{
			name: "global role beyond caller permissions",
			role: "admin",
			assert: func(t *testing.T, res any) {
				_, ok := res.(*authz.AssignRoleToUserForbidden)
				require.True(t, ok, "got %T", res)
			},
		},
		{
			name: "unknown role",
			role: "ghost",
			assert: func(t *testing.T, res any) {
				_, ok := res.(*authz.AssignRoleToUserNotFound)
				require.True(t, ok, "got %T", res)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := nsAssignHandler(t, "customer1")
			res := h.assignRoleToUser(authz.AssignRoleToUserParams{
				HTTPRequest: req,
				ID:          "bob",
				Body:        authz.AssignRoleToUserBody{Roles: []string{tt.role}, UserType: models.UserTypeInputDb},
			}, principal)
			tt.assert(t, res)
		})
	}
}

// A single request mixing a role within the caller's permissions with one
// beyond them is rejected wholesale: the must-already-hold check sees every
// assigned role's permissions.
func TestAssignRoleToUserNamespacedMixedEnvelope(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}
	res := h.assignRoleToUser(authz.AssignRoleToUserParams{
		HTTPRequest: req,
		ID:          "bob",
		Body:        authz.AssignRoleToUserBody{Roles: []string{"editor", "admin"}, UserType: models.UserTypeInputDb},
	}, principal)
	_, ok := res.(*authz.AssignRoleToUserForbidden)
	require.True(t, ok, "got %T", res)
}

func TestAssignRoleToUserNamespacedUserTypeRequired(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}
	res := h.assignRoleToUser(authz.AssignRoleToUserParams{
		HTTPRequest: req,
		ID:          "bob",
		Body:        authz.AssignRoleToUserBody{Roles: []string{"editor"}},
	}, principal)
	_, ok := res.(*authz.AssignRoleToUserBadRequest)
	require.True(t, ok, "got %T", res)
}

func TestRevokeRoleFromUserNamespacedUserTypeRequired(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}
	res := h.revokeRoleFromUser(authz.RevokeRoleFromUserParams{
		HTTPRequest: req,
		ID:          "bob",
		Body:        authz.RevokeRoleFromUserBody{Roles: []string{"editor"}},
	}, principal)
	_, ok := res.(*authz.RevokeRoleFromUserBadRequest)
	require.True(t, ok, "got %T", res)
}

func TestRevokeRoleFromUserNamespaced(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}
	res := h.revokeRoleFromUser(authz.RevokeRoleFromUserParams{
		HTTPRequest: req,
		ID:          "bob",
		Body:        authz.RevokeRoleFromUserBody{Roles: []string{"editor"}, UserType: models.UserTypeInputDb},
	}, principal)
	_, ok := res.(*authz.RevokeRoleFromUserOK)
	require.True(t, ok, "got %T", res)
}

func TestAssignRoleToGroupNamespacedDenied(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "customer1:admin", UserType: "oidc", Namespace: "customer1"}
	res := h.assignRoleToGroup(authz.AssignRoleToGroupParams{
		HTTPRequest: req,
		ID:          "engineers",
		Body:        authz.AssignRoleToGroupBody{Roles: []string{"editor"}, GroupType: models.GroupTypeOidc},
	}, principal)
	_, ok := res.(*authz.AssignRoleToGroupForbidden)
	require.True(t, ok, "got %T", res)
}

func TestRevokeRoleFromGroupNamespacedDenied(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "customer1:admin", UserType: "oidc", Namespace: "customer1"}
	res := h.revokeRoleFromGroup(authz.RevokeRoleFromGroupParams{
		HTTPRequest: req,
		ID:          "engineers",
		Body:        authz.RevokeRoleFromGroupBody{Roles: []string{"editor"}, GroupType: models.GroupTypeOidc},
	}, principal)
	_, ok := res.(*authz.RevokeRoleFromGroupForbidden)
	require.True(t, ok, "got %T", res)
}

// TestAssignRoleToGroupGlobalCallerAllowed pins the global-caller ALLOW path:
// the namespaced-deny gate does not apply, and a role within the caller's
// effective permissions is assigned to the group.
func TestAssignRoleToGroupGlobalCallerAllowed(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, mock.Anything).Return(nil)
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", "viewer").Return(map[string][]authorization.Policy{
		"viewer": {collPolicy(authorization.READ, "Movies")},
	}, nil)
	controller.On("AddRolesForUser", mock.Anything, mock.Anything).Return(nil)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
	principal := &models.Principal{Username: "op", UserType: "oidc"}
	res := h.assignRoleToGroup(authz.AssignRoleToGroupParams{
		HTTPRequest: req,
		ID:          "engineers",
		Body:        authz.AssignRoleToGroupBody{Roles: []string{"viewer"}, GroupType: models.GroupTypeOidc},
	}, principal)
	_, ok := res.(*authz.AssignRoleToGroupOK)
	require.True(t, ok, "got %T", res)
}

// TestAssignRoleToGroupGlobalCallerEffectiveDeny pins the must-already-hold
// guard for a non-root global caller: a role whose permissions it does not
// itself hold cannot be assigned to a group.
func TestAssignRoleToGroupGlobalCallerEffectiveDeny(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, mock.Anything).Return(nil)
	authorizer.On("AuthorizeSilent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("forbidden"))
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", "admin").Return(map[string][]authorization.Policy{
		"admin": {{Resource: authorization.Cluster(), Verb: authorization.READ, Domain: authorization.ClusterDomain}},
	}, nil)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
	principal := &models.Principal{Username: "op", UserType: "oidc"}
	res := h.assignRoleToGroup(authz.AssignRoleToGroupParams{
		HTTPRequest: req,
		ID:          "engineers",
		Body:        authz.AssignRoleToGroupBody{Roles: []string{"admin"}, GroupType: models.GroupTypeOidc},
	}, principal)
	_, ok := res.(*authz.AssignRoleToGroupForbidden)
	require.True(t, ok, "got %T", res)
}

// TestRevokeRoleFromGroupGlobalCallerAllowed pins the global-caller revoke path;
// revoke carries no must-already-hold guard because revoking cannot escalate.
func TestRevokeRoleFromGroupGlobalCallerAllowed(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, mock.Anything).Return(nil)
	controller := NewMockControllerAndGetUsers(t)
	controller.On("GetRoles", "viewer").Return(map[string][]authorization.Policy{
		"viewer": {collPolicy(authorization.READ, "Movies")},
	}, nil)
	controller.On("RevokeRolesForUser", mock.Anything, mock.Anything).Return(nil)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
	principal := &models.Principal{Username: "op", UserType: "oidc"}
	res := h.revokeRoleFromGroup(authz.RevokeRoleFromGroupParams{
		HTTPRequest: req,
		ID:          "engineers",
		Body:        authz.RevokeRoleFromGroupBody{Roles: []string{"viewer"}, GroupType: models.GroupTypeOidc},
	}, principal)
	_, ok := res.(*authz.RevokeRoleFromGroupOK)
	require.True(t, ok, "got %T", res)
}

// TestAssignRoleToUserGlobalOperatorLocalRoleDenied pins the cross-namespace
// guard: a global operator cannot assign a namespace-local role to any user,
// not even a user in that role's own namespace — local roles are managed only
// by a caller confined to the namespace.
func TestAssignRoleToUserGlobalOperatorLocalRoleDenied(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	authorizer.On("Authorize", mock.Anything, mock.Anything, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, mock.Anything).Return(nil)
	controller := NewMockControllerAndGetUsers(t)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
	principal := &models.Principal{Username: "op", UserType: "db"}
	res := h.assignRoleToUser(authz.AssignRoleToUserParams{
		HTTPRequest: req,
		ID:          "customer1:bob",
		Body:        authz.AssignRoleToUserBody{Roles: []string{"customer1:editor"}, UserType: models.UserTypeInputDb},
	}, principal)
	_, ok := res.(*authz.AssignRoleToUserForbidden)
	require.True(t, ok, "got %T", res)
}

// TestAssignRoleToGroupGlobalOperatorLocalRoleDenied pins that a namespace-local
// role can never be assigned to a (global) group.
func TestAssignRoleToGroupGlobalOperatorLocalRoleDenied(t *testing.T) {
	authorizer := authorization.NewMockAuthorizer(t)
	controller := NewMockControllerAndGetUsers(t)

	logger, _ := test.NewNullLogger()
	h := &authZHandlers{authorizer: authorizer, controller: controller, logger: logger, rbacconfig: rbacconf.Config{Enabled: true}, namespacesEnabled: true}
	principal := &models.Principal{Username: "op", UserType: "oidc"}
	res := h.assignRoleToGroup(authz.AssignRoleToGroupParams{
		HTTPRequest: req,
		ID:          "engineers",
		Body:        authz.AssignRoleToGroupBody{Roles: []string{"customer1:editor"}, GroupType: models.GroupTypeOidc},
	}, principal)
	_, ok := res.(*authz.AssignRoleToGroupForbidden)
	require.True(t, ok, "got %T", res)
}

// TestAssignRoleToUserNamespacedForeignRoleRejected pins that a namespaced
// caller cannot reach another namespace's role by addressing it with a
// qualified name: the qualified input is rejected before any assignment.
func TestAssignRoleToUserNamespacedForeignRoleRejected(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "customer1:admin", UserType: "db", Namespace: "customer1"}
	res := h.assignRoleToUser(authz.AssignRoleToUserParams{
		HTTPRequest: req,
		ID:          "bob",
		Body:        authz.AssignRoleToUserBody{Roles: []string{"customer2:editor"}, UserType: models.UserTypeInputDb},
	}, principal)
	_, ok := res.(*authz.AssignRoleToUserBadRequest)
	require.True(t, ok, "got %T", res)
}

// TestRevokeRoleFromUserGlobalOperatorLocalRoleAllowed pins the deliberate
// asymmetry with assign: a global operator MAY revoke a namespace-local role.
// Revoke only removes a grant, so it cannot escalate privilege or make a local
// role reach a foreign subject — the concerns the assign-side guard exists for —
// so the revoke path carries no validateLocalRoleAssignment gate.
func TestRevokeRoleFromUserGlobalOperatorLocalRoleAllowed(t *testing.T) {
	h, _ := nsAssignHandler(t, "customer1")
	principal := &models.Principal{Username: "op", UserType: "db", IsGlobalOperator: true}
	res := h.revokeRoleFromUser(authz.RevokeRoleFromUserParams{
		HTTPRequest: req,
		ID:          "customer1:bob",
		Body:        authz.RevokeRoleFromUserBody{Roles: []string{"customer1:editor"}, UserType: models.UserTypeInputDb},
	}, principal)
	_, ok := res.(*authz.RevokeRoleFromUserOK)
	require.True(t, ok, "got %T", res)
}
