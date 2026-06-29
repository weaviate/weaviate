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
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

const (
	roleNameMaxLength = 64
	roleNameRegexCore = `[A-Za-z][-_0-9A-Za-z]{0,254}`
)

var validateRoleNameRegex = regexp.MustCompile(`^` + roleNameRegexCore + `$`)

type authZHandlers struct {
	authorizer        authorization.Authorizer
	controller        ControllerAndGetUsers
	schemaReader      schemaUC.SchemaGetter
	logger            logrus.FieldLogger
	metrics           *monitoring.PrometheusMetrics
	apiKeysConfigs    config.StaticAPIKey
	oidcConfigs       config.OIDC
	rbacconfig        rbacconf.Config
	namespacesEnabled bool
}

type ControllerAndGetUsers interface {
	authorization.Controller
	GetUsers(userIds ...string) (map[string]apikey.UserView, error)
}

func SetupHandlers(api *operations.WeaviateAPI, controller ControllerAndGetUsers, schemaReader schemaUC.SchemaGetter,
	apiKeysConfigs config.StaticAPIKey, oidcConfigs config.OIDC, rconfig rbacconf.Config, namespacesEnabled bool, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	h := &authZHandlers{
		controller:        controller,
		authorizer:        authorizer,
		schemaReader:      schemaReader,
		rbacconfig:        rconfig,
		oidcConfigs:       oidcConfigs,
		apiKeysConfigs:    apiKeysConfigs,
		namespacesEnabled: namespacesEnabled,
		logger:            logger,
		metrics:           metrics,
	}

	// rbac role handlers
	api.AuthzCreateRoleHandler = authz.CreateRoleHandlerFunc(h.createRole)
	api.AuthzGetRolesHandler = authz.GetRolesHandlerFunc(h.getRoles)
	api.AuthzGetRoleHandler = authz.GetRoleHandlerFunc(h.getRole)
	api.AuthzDeleteRoleHandler = authz.DeleteRoleHandlerFunc(h.deleteRole)
	api.AuthzAddPermissionsHandler = authz.AddPermissionsHandlerFunc(h.addPermissions)
	api.AuthzRemovePermissionsHandler = authz.RemovePermissionsHandlerFunc(h.removePermissions)
	api.AuthzHasPermissionHandler = authz.HasPermissionHandlerFunc(h.hasPermission)

	// rbac users handlers
	api.AuthzGetRolesForUserHandler = authz.GetRolesForUserHandlerFunc(h.getRolesForUser)
	api.AuthzGetUsersForRoleHandler = authz.GetUsersForRoleHandlerFunc(h.getUsersForRole)
	api.AuthzGetUsersForRoleDeprecatedHandler = authz.GetUsersForRoleDeprecatedHandlerFunc(h.getUsersForRoleDeprecated)
	api.AuthzAssignRoleToUserHandler = authz.AssignRoleToUserHandlerFunc(h.assignRoleToUser)
	api.AuthzRevokeRoleFromUserHandler = authz.RevokeRoleFromUserHandlerFunc(h.revokeRoleFromUser)
	api.AuthzGetRolesForUserDeprecatedHandler = authz.GetRolesForUserDeprecatedHandlerFunc(h.getRolesForUserDeprecated)

	// rbac group handlers
	api.AuthzAssignRoleToGroupHandler = authz.AssignRoleToGroupHandlerFunc(h.assignRoleToGroup)
	api.AuthzRevokeRoleFromGroupHandler = authz.RevokeRoleFromGroupHandlerFunc(h.revokeRoleFromGroup)
	api.AuthzGetRolesForGroupHandler = authz.GetRolesForGroupHandlerFunc(h.getRolesForGroup)
	api.AuthzGetGroupsHandler = authz.GetGroupsHandlerFunc(h.getGroups)
	api.AuthzGetGroupsForRoleHandler = authz.GetGroupsForRoleHandlerFunc(h.getGroupsForRole)
}

func (h *authZHandlers) authorizeRoleScopes(ctx context.Context, principal *models.Principal, originalVerb string, policies []authorization.Policy, roleName string) error {
	// The error will be accumulated with each check. We first verify if the user has the necessary permissions.
	// If not, we check for matching permissions and authorize each permission being added or removed from the role.
	// NOTE: logic is inverted for error checks if err == nil
	var err error

	// ALL scope is for unconfined (global) principals only; a principal confined
	// to a namespace is always forced through the MATCH path, where it must
	// already hold each permission, so an ALL-scoped grant it somehow holds can't
	// escalate to cluster-wide role management.
	confinedToNamespace := h.callerConfined(principal)
	if !confinedToNamespace {
		if err = h.authorizer.Authorize(ctx, principal, authorization.VerbWithScope(originalVerb, authorization.ROLE_SCOPE_ALL), authorization.Roles(roleName)...); err == nil {
			return nil
		}
	}

	// Check if user can manage roles with matching permissions
	if err = h.authorizer.Authorize(ctx, principal, authorization.VerbWithScope(originalVerb, authorization.ROLE_SCOPE_MATCH), authorization.Roles(roleName)...); err == nil {
		// Verify user has all permissions they're trying to grant
		var errs error
		for _, policy := range policies {
			if err := h.authorizer.AuthorizeSilent(ctx, principal, policy.Verb, policy.Resource); err != nil {
				errs = errors.Join(errs, err)
			}
		}
		return errs
	}

	return fmt.Errorf("can only create roles with less or equal permissions as the current user: %w", err)
}

// rolePoliciesVisibleToPrincipal reports whether the caller may see a role with
// the given policies: an operator with read-all on roles sees every role; any
// other caller sees a role only when it already holds every permission the role
// grants. For a namespaced caller each permission is projected into its
// namespace first, mirroring assignment, so a global template role is evaluated
// as it applies to the caller (and a role bound to another namespace is hidden).
func (h *authZHandlers) rolePoliciesVisibleToPrincipal(ctx context.Context, principal *models.Principal, policies []authorization.Policy) bool {
	if err := h.authorizer.Authorize(ctx, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()...); err == nil {
		return true
	}
	namespaced := h.callerConfined(principal)
	for _, policy := range policies {
		// Permission-less roles carry this placeholder; it grants nothing.
		if policy.Resource == conv.InternalPlaceHolder {
			continue
		}
		resource := policy.Resource
		if namespaced {
			projected, err := namespacing.ProjectResourceForNamespace(policy.Resource, principal.Namespace)
			if err != nil {
				return false
			}
			resource = projected
		}
		if err := h.authorizer.AuthorizeSilent(ctx, principal, policy.Verb, resource); err != nil {
			return false
		}
	}
	return true
}

// callerConfined reports whether the principal is a namespace-confined caller —
// its RBAC reads and writes scope to its own namespace. False for global callers
// and on NS-disabled clusters.
func (h *authZHandlers) callerConfined(principal *models.Principal) bool {
	return h.namespacesEnabled && namespacing.ConfinedNamespace(principal) != ""
}

// validateLocalRoleAssignment blocks assigning a namespace-local role unless the
// caller is confined to that role's namespace. A local role is managed entirely
// within its namespace, so a global operator (and any cross-namespace caller)
// cannot assign it; global roles carry no namespace and assign anywhere. This is
// what keeps a namespace1 role from ever reaching a namespace2 (or global)
// subject. No-op on NS-disabled clusters, where ':' is a valid name character.
func (h *authZHandlers) validateLocalRoleAssignment(principal *models.Principal, roleNames []string) error {
	if !h.namespacesEnabled {
		return nil
	}
	callerNS := namespacing.ConfinedNamespace(principal)
	for _, roleName := range roleNames {
		if ns := namespacing.NamespaceFromQualified(roleName); ns != "" && ns != callerNS {
			return fmt.Errorf("a namespace-local role can only be assigned by an administrator of its own namespace")
		}
	}
	return nil
}

// validateLocalRoleModification blocks editing a namespace-local role's
// permissions unless the caller is confined to that role's namespace. A global
// operator's bare resources would be stored unqualified inside the role, and it
// cannot express the qualified form; global roles carry no namespace and stay
// editable. No-op on NS-disabled clusters.
func (h *authZHandlers) validateLocalRoleModification(principal *models.Principal, roleName string) error {
	if !h.namespacesEnabled {
		return nil
	}
	if ns := namespacing.NamespaceFromQualified(roleName); ns != "" && ns != namespacing.ConfinedNamespace(principal) {
		return fmt.Errorf("a namespace-local role can only be modified by an administrator of its own namespace")
	}
	return nil
}

// validateOperatorRoleAssignmentToUser blocks assigning an operator-reserved
// global role to a namespaced user. Keyed on the target's namespace, not the
// caller's, so even an operator cannot pull a reserved global role into a
// namespace. No-op on NS-disabled clusters and for global targets.
func (h *authZHandlers) validateOperatorRoleAssignmentToUser(targetNamespace string, roleNames []string) error {
	if !h.namespacesEnabled || targetNamespace == "" {
		return nil
	}
	for _, roleName := range roleNames {
		if namespacing.NamespaceFromQualified(roleName) == "" && authorization.IsOperatorReservedRoleName(namespacing.StripQualification(roleName)) {
			return fmt.Errorf("this role cannot be assigned to a namespaced user")
		}
	}
	return nil
}

// roleHiddenFromCaller reports whether storedRoleName belongs to a namespace
// other than the caller's, so its very existence must not be revealed.
// Own-namespace and global roles are visible; only foreign-namespace roles are
// hidden. Global operators (and NS-disabled clusters) hide nothing.
func (h *authZHandlers) roleHiddenFromCaller(principal *models.Principal, storedRoleName string) bool {
	if !h.callerConfined(principal) {
		return false
	}
	ns := namespacing.NamespaceFromQualified(storedRoleName)
	return ns != "" && ns != principal.Namespace
}

// roleOperatorReservedFromCaller hides an operator-reserved global role from any
// caller that is not a global operator. Additive to the permission-content gate:
// it can only hide more, never expose a role that gate would block.
func (h *authZHandlers) roleOperatorReservedFromCaller(principal *models.Principal, storedRoleName string) bool {
	if principal == nil {
		return false
	}
	// Operator status is IsGlobalOperator, not an empty namespace: a namespace-less
	// non-operator must still have reserved roles hidden.
	if !h.namespacesEnabled || principal.IsGlobalOperator {
		return false
	}
	return authorization.IsOperatorReservedRoleName(namespacing.StripQualification(storedRoleName))
}

// roleVisibleToCaller reports whether the caller may see another subject's role
// given its stored name and policies. A role is hidden when it belongs to another
// namespace, when it is an operator-reserved global role and the caller is not a
// global operator, or when the caller does not already hold every permission it
// grants. A caller's own roles are always visible, so callers skip this check for
// a self-read.
func (h *authZHandlers) roleVisibleToCaller(ctx context.Context, principal *models.Principal, storedRoleName string, policies []authorization.Policy) bool {
	if h.roleHiddenFromCaller(principal, storedRoleName) {
		return false
	}
	if h.roleOperatorReservedFromCaller(principal, storedRoleName) {
		return false
	}
	if h.namespacesEnabled && !h.rolePoliciesVisibleToPrincipal(ctx, principal, policies) {
		return false
	}
	return true
}

// roleExists backs ResolveRoleName's local-then-global lookup.
func (h *authZHandlers) roleExists(name string) (bool, error) {
	roles, err := h.controller.GetRoles(name)
	if err != nil {
		return false, err
	}
	return len(roles) > 0, nil
}

// authorizeRoleRead reports whether the caller may read a role's full body.
// NS-disabled gates on the role-name matcher; NS-enabled gates on content scope.
func (h *authZHandlers) authorizeRoleRead(ctx context.Context, principal *models.Principal, roleName string, policies []authorization.Policy) error {
	if !h.namespacesEnabled {
		return h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, roleName)
	}
	if !h.rolePoliciesVisibleToPrincipal(ctx, principal, policies) {
		return fmt.Errorf("insufficient permissions to view role %q", namespacing.StripOwnNamespace(principal, roleName))
	}
	return nil
}

type roleReadOutcome int

const (
	roleReadOK roleReadOutcome = iota
	roleReadNotFound
	roleReadBadRequest
	roleReadForbidden
	roleReadInternalErr
)

// resolveRoleNameForCaller resolves a caller-supplied role name to its stored
// form, reporting an operator-reserved global role as not found for any caller
// that is not a global operator. Keeps a reserved role's existence from leaking
// through the assign/revoke/update/delete responses, mirroring the read paths.
func (h *authZHandlers) resolveRoleNameForCaller(principal *models.Principal, rawID string) (string, error) {
	stored, err := namespacing.ResolveRoleName(principal, h.namespacesEnabled, rawID, h.roleExists)
	if err != nil {
		return "", err
	}
	if h.roleOperatorReservedFromCaller(principal, stored) {
		return "", namespacing.ErrRoleNotFound
	}
	return stored, nil
}

// resolveRoleForRead resolves a caller-supplied role name to its stored form
// and gates a role-target read: NS-disabled on the role-name matcher,
// NS-enabled on content scope (resolve local-then-global, then verify the
// caller holds every permission the role grants). The outcome maps to the
// caller's own responder; err carries the message for the non-OK outcomes.
// policies is the resolved role's stored policies, populated only on the
// NS-enabled OK path (where they are fetched for the content-scope gate) so
// callers needing them avoid a second GetRoles; it is nil otherwise.
func (h *authZHandlers) resolveRoleForRead(ctx context.Context, principal *models.Principal, rawID string) (roleID string, policies []authorization.Policy, outcome roleReadOutcome, err error) {
	if !h.namespacesEnabled {
		if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, rawID); err != nil {
			return "", nil, roleReadForbidden, err
		}
		return rawID, nil, roleReadOK, nil
	}
	stored, err := h.resolveRoleNameForCaller(principal, rawID)
	if errors.Is(err, namespacing.ErrRoleNotFound) {
		return "", nil, roleReadNotFound, nil
	}
	if err != nil {
		return "", nil, roleReadBadRequest, err
	}
	roles, err := h.controller.GetRoles(stored)
	if err != nil {
		return "", nil, roleReadInternalErr, fmt.Errorf("GetRoles: %w", err)
	}
	storedPolicies, ok := roles[stored]
	if !ok {
		// Role removed between the resolve and the fetch.
		return "", nil, roleReadNotFound, nil
	}
	if !h.roleVisibleToCaller(ctx, principal, stored, storedPolicies) {
		return "", nil, roleReadForbidden, errors.New("insufficient permissions to view role")
	}
	return stored, storedPolicies, roleReadOK, nil
}

// authorizeAssignRolePermissions blocks privilege escalation via role
// assignment: the caller must already hold every permission the assigned roles
// grant, else assign_and_revoke_users alone could be used to grant admin. For a
// namespaced caller each permission is projected into the assignee's namespace
// before the check, so a global template role is evaluated as it applies to the
// assignee rather than against its raw unqualified resource strings.
func (h *authZHandlers) authorizeAssignRolePermissions(ctx context.Context, principal *models.Principal, targetNamespace string, roles map[string][]authorization.Policy) error {
	namespaced := h.callerConfined(principal)
	var errs error
	for _, policies := range roles {
		for _, policy := range policies {
			// GetRoles returns this placeholder for permission-less roles; it grants nothing.
			if policy.Resource == conv.InternalPlaceHolder {
				continue
			}
			resource := policy.Resource
			if namespaced {
				projected, err := namespacing.ProjectResourceForNamespace(policy.Resource, targetNamespace)
				if err != nil {
					// A role bound elsewhere can't be granted here; deny without
					// echoing the foreign resource back to the caller.
					errs = errors.Join(errs, errors.New("role grants a permission you do not hold"))
					continue
				}
				resource = projected
			}
			if err := h.authorizer.AuthorizeSilent(ctx, principal, policy.Verb, resource); err != nil {
				errs = errors.Join(errs, err)
			}
		}
	}
	if errs != nil {
		return fmt.Errorf("can only assign roles with less or equal permissions as the current user: %w", errs)
	}
	return nil
}

// hasAllScopedRolePermission reports whether any policy grants role management
// at ALL scope (verb-suffix _ALL on a roles/ resource). On NS clusters such a
// permission cannot be stored in any role — cluster-wide role management exists
// only as root's wildcard, never as an assignable permission.
func hasAllScopedRolePermission(policies []authorization.Policy) bool {
	for _, p := range policies {
		if strings.HasPrefix(p.Resource, authorization.RolesDomain+"/") &&
			strings.HasSuffix(p.Verb, "_"+authorization.ROLE_SCOPE_ALL) {
			return true
		}
	}
	return false
}

func (h *authZHandlers) createRole(params authz.CreateRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if *params.Body.Name == "" {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("role name is required")))
	}

	if err := validateRoleName(*params.Body.Name); err != nil {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("role name is invalid")))
	}

	if err := validatePermissions(h.namespacesEnabled, true, params.Body.Permissions...); err != nil {
		return authz.NewCreateRoleUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("role permissions are invalid: %w", err)))
	}

	policies, err := conv.RolesToPolicies(params.Body)
	if err != nil {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("invalid role: %w", err)))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("you cannot create role with the same name as built-in role %s", *params.Body.Name)))
	}

	// The operator-reserved prefix is for operator-only global roles; a confined
	// caller submits a bare short name, so check it before it is namespace-qualified.
	if h.callerConfined(principal) && authorization.IsOperatorReservedRoleName(*params.Body.Name) {
		return authz.NewCreateRoleUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("role name uses a reserved operator prefix")))
	}

	// A namespaced caller's role name is auto-prefixed with its namespace.
	roleName, err := namespacing.QualifyRoleNameForCreate(principal, h.namespacesEnabled, *params.Body.Name)
	if err != nil {
		return authz.NewCreateRoleUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// Reject namespace-qualified resource paths up front (callers submit bare
	// names; a namespaced caller's are auto-prefixed below).
	rolePolicies := policies[*params.Body.Name]
	if err := h.validateNoQualifiedNamespaceInPolicies(principal, rolePolicies, false); err != nil {
		return authz.NewCreateRoleUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// A namespaced caller's permission segments are auto-prefixed in place.
	if err := namespacing.QualifyRolePoliciesForCreate(principal, h.namespacesEnabled, rolePolicies); err != nil {
		return authz.NewCreateRoleUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// On NS clusters no role may carry ALL-scoped role management; cluster-wide
	// role management exists only as root's wildcard.
	if h.namespacesEnabled && hasAllScopedRolePermission(rolePolicies) {
		return authz.NewCreateRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("cannot create a role granting cluster-wide role management")))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.CREATE, rolePolicies, roleName); err != nil {
		return authz.NewCreateRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// A global name reserves its short name across every namespace, so the
	// conflict scan spans all roles, not just the candidate name.
	allRoles, err := h.controller.GetRoles()
	if err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}
	if namespacing.FindShortNameConflict(maps.Keys(allRoles), roleName) != namespacing.NoRoleConflict {
		short := namespacing.StripOwnNamespace(principal, roleName)
		return authz.NewCreateRoleConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("role with name %s already exists", short)))
	}

	if err = h.controller.CreateRolesPermissions(map[string][]authorization.Policy{roleName: rolePolicies}); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "create_role",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    roleName,
		"permissions": params.Body.Permissions,
	}).Info("role created")

	return authz.NewCreateRoleCreated()
}

func (h *authZHandlers) addPermissions(params authz.AddPermissionsParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("you can not update built-in role %s", params.ID)))
	}

	if err := validatePermissions(h.namespacesEnabled, false, params.Body.Permissions...); err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("invalid permissions %w", err)))
	}

	policies, err := conv.RolesToPolicies(&models.Role{
		Name:        &params.ID,
		Permissions: params.Body.Permissions,
	})
	if err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("invalid permissions %w", err)))
	}

	rolePolicies := policies[params.ID]
	if err := h.validateNoQualifiedNamespaceInPolicies(principal, rolePolicies, false); err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// A namespaced caller's permission segments are auto-prefixed in place.
	if err := namespacing.QualifyRolePoliciesForCreate(principal, h.namespacesEnabled, rolePolicies); err != nil {
		return authz.NewAddPermissionsUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// On NS clusters no role may carry ALL-scoped role management; cluster-wide
	// role management exists only as root's wildcard.
	if h.namespacesEnabled && hasAllScopedRolePermission(rolePolicies) {
		return authz.NewAddPermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("cannot add a permission granting cluster-wide role management")))
	}

	roleName, err := h.resolveRoleNameForCaller(principal, params.ID)
	if errors.Is(err, namespacing.ErrRoleNotFound) {
		return authz.NewAddPermissionsNotFound()
	}
	if err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateLocalRoleModification(principal, roleName); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.UPDATE, rolePolicies, roleName); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	roles, err := h.controller.GetRoles(roleName)
	if err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}

	if len(roles) == 0 { // i.e. new role
		return authz.NewAddPermissionsNotFound()
	}

	if err := h.controller.UpdateRolesPermissions(map[string][]authorization.Policy{roleName: rolePolicies}); err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "add_permissions",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    roleName,
		"permissions": params.Body.Permissions,
	}).Info("permissions added")

	return authz.NewAddPermissionsOK()
}

func (h *authZHandlers) removePermissions(params authz.RemovePermissionsParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	// we don't validate permissions entity existence
	// in case of the permissions gets removed after the entity got removed
	// delete class ABC, then remove permissions on class ABC
	if err := validatePermissions(h.namespacesEnabled, false, params.Body.Permissions...); err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("invalid permissions %w", err)))
	}

	permissions, err := conv.PermissionToPolicies(params.Body.Permissions...)
	if err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("invalid permissions %w", err)))
	}
	// TODO-RBAC PermissionToPolicies has to be []Policy{} not slice of pointers
	policies := make([]authorization.Policy, len(permissions))
	for i, p := range permissions {
		policies[i] = *p
	}

	if err := h.validateNoQualifiedNamespaceInPolicies(principal, policies, false); err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// A namespaced caller's permission segments are auto-prefixed in place so the
	// stored qualified rows are the ones removed.
	if err := namespacing.QualifyRolePoliciesForCreate(principal, h.namespacesEnabled, policies); err != nil {
		return authz.NewRemovePermissionsUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	for i := range permissions {
		permissions[i] = &policies[i]
	}

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("you cannot update built-in role %s", params.ID)))
	}

	roleName, err := h.resolveRoleNameForCaller(principal, params.ID)
	if errors.Is(err, namespacing.ErrRoleNotFound) {
		return authz.NewRemovePermissionsNotFound()
	}
	if err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateLocalRoleModification(principal, roleName); err != nil {
		return authz.NewRemovePermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.UPDATE, policies, roleName); err != nil {
		return authz.NewRemovePermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	role, err := h.controller.GetRoles(roleName)
	if err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}

	if len(role) == 0 {
		return authz.NewRemovePermissionsNotFound()
	}

	if err := h.controller.RemovePermissions(roleName, permissions); err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("RemovePermissions: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "remove_permissions",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    roleName,
		"permissions": params.Body.Permissions,
	}).Info("permissions removed")

	return authz.NewRemovePermissionsOK()
}

func (h *authZHandlers) hasPermission(params authz.HasPermissionParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if params.Body == nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("permission is required")))
	}

	if err := validatePermissions(h.namespacesEnabled, false, params.Body); err != nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("invalid permissions %w", err)))
	}

	policy, err := conv.PermissionToPolicies(params.Body)
	if err != nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("invalid permissions %w", err)))
	}
	if len(policy) == 0 {
		return authz.NewHasPermissionInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.New("unknown error occurred passing permission to policy")))
	}

	// A confined caller submits a bare resource, qualified into its namespace
	// below; a global operator must name the role's namespace explicitly to check
	// a namespace-local role's qualified rows.
	if err := h.validateNoQualifiedNamespaceInPolicies(principal, []authorization.Policy{*policy[0]}, true); err != nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	roleID, _, outcome, err := h.resolveRoleForRead(ctx, principal, params.ID)
	switch outcome {
	case roleReadOK:
		// proceed
	case roleReadNotFound:
		return authz.NewHasPermissionNotFound()
	case roleReadBadRequest:
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadForbidden:
		return authz.NewHasPermissionForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadInternalErr:
		return authz.NewHasPermissionInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// For a namespace-local role, a confined caller's bare resource is qualified
	// into its namespace; a global caller's resource is checked as submitted (it
	// already names the namespace). A global role stores unqualified rows, so its
	// check is raw.
	policyToCheck := policy[0]
	if h.namespacesEnabled && namespacing.NamespaceFromQualified(roleID) != "" {
		qualified := []authorization.Policy{*policy[0]}
		if err := namespacing.QualifyRolePoliciesForCreate(principal, h.namespacesEnabled, qualified); err != nil {
			return authz.NewHasPermissionUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		policyToCheck = &qualified[0]
	}

	hasPermission, err := h.controller.HasPermission(roleID, policyToCheck)
	if err != nil {
		return authz.NewHasPermissionInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("HasPermission: %w", err)))
	}
	return authz.NewHasPermissionOK().WithPayload(hasPermission)
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	roles, err := h.controller.GetRoles()
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}

	var response []*models.Role
	for roleName, policies := range roles {
		if roleName == authorization.Root && !slices.Contains(h.rbacconfig.RootUsers, principal.Username) {
			continue
		}

		name := roleName
		if h.namespacesEnabled {
			// Hide other namespaces' roles, reserved global roles, and any whose
			// permissions the caller does not already hold; strip the caller's own
			// namespace prefix.
			if !h.roleVisibleToCaller(ctx, principal, roleName, policies) {
				continue
			}
			name = namespacing.StripOwnNamespace(principal, roleName)
		}

		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("PoliciesToPermission: %w", err)))
		}
		response = append(response, &models.Role{Name: &name, Permissions: perms})
	}

	// On NS-disabled clusters visibility stays on the role-name matcher
	// (READ_ALL, then READ_MATCH on the same set); NS-enabled was already
	// content-filtered in the loop above.
	if !h.namespacesEnabled {
		resourceFilter := filter.New[*models.Role](h.authorizer, h.rbacconfig)
		roleResource := func(role *models.Role) string {
			return authorization.Roles(*role.Name)[0]
		}
		filtered := resourceFilter.Filter(ctx, principal, response,
			authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), roleResource)
		if len(filtered) == 0 {
			// try match if all was none
			filtered = resourceFilter.Filter(ctx, principal, response,
				authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH), roleResource)
		}
		response = filtered
	}

	// Strip the caller's own namespace prefix from each permission's resource
	// names; role names were already stripped in the loop above.
	response = namespacing.StripRolesForCaller(principal, response)

	sortByName(response)

	logFields := logrus.Fields{
		"action":    "read_all_roles",
		"component": authorization.ComponentName,
	}
	if principal != nil {
		logFields["user"] = principal.Username
	}
	h.logger.WithFields(logFields).Info("roles requested")

	return authz.NewGetRolesOK().WithPayload(response)
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	// Resolve and authorize the read. NS-enabled gates on content scope and
	// returns the role's policies; a name that resolves to no role is 404 and a
	// role whose permissions the caller does not hold is 403, so a foreign
	// namespace's role never leaks.
	roleID, policies, outcome, err := h.resolveRoleForRead(ctx, principal, params.ID)
	switch outcome {
	case roleReadOK:
		// proceed
	case roleReadNotFound:
		return authz.NewGetRoleNotFound()
	case roleReadBadRequest:
		return authz.NewGetRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadForbidden:
		return authz.NewGetRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadInternalErr:
		return authz.NewGetRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	// NS-disabled authorizes on the role-name matcher without fetching the role;
	// load it here and 404 if the matcher-authorized name has no stored role.
	if !h.namespacesEnabled {
		roles, err := h.controller.GetRoles(roleID)
		if err != nil {
			return authz.NewGetRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
		}
		if len(roles) == 0 {
			return authz.NewGetRoleNotFound()
		}
		if len(roles) != 1 {
			return authz.NewGetRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: expected one role but got %d", len(roles))))
		}
		policies = roles[roleID]
	}

	perms, err := conv.PoliciesToPermission(policies...)
	if err != nil {
		return authz.NewGetRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("PoliciesToPermission: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":    "read_role",
		"component": authorization.ComponentName,
		"user":      principal.Username,
		"role_id":   roleID,
	}).Info("role requested")

	name := namespacing.StripOwnNamespace(principal, roleID)
	// Strip the caller's own namespace prefix from each permission's resource names.
	role := namespacing.StripRolesForCaller(principal, []*models.Role{{Name: &name, Permissions: perms}})[0]
	return authz.NewGetRoleOK().WithPayload(role)
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewDeleteRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("you can not delete built-in role %s", params.ID)))
	}

	roleName, err := h.resolveRoleNameForCaller(principal, params.ID)
	if errors.Is(err, namespacing.ErrRoleNotFound) {
		// Idempotent delete: a role the caller cannot resolve is already gone.
		return authz.NewDeleteRoleNoContent()
	}
	if err != nil {
		return authz.NewDeleteRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	roles, err := h.controller.GetRoles(roleName)
	if err != nil {
		h.logger.WithFields(logrus.Fields{
			"action":    "delete_role",
			"component": authorization.ComponentName,
			"user":      principal.Username,
			"roleName":  roleName,
		}).Info("role was already deleted")
		return authz.NewDeleteRoleNoContent()
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.DELETE, roles[roleName], roleName); err != nil {
		return authz.NewDeleteRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.controller.DeleteRoles(roleName); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("DeleteRoles: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":    "delete_role",
		"component": authorization.ComponentName,
		"user":      principal.Username,
		"roleName":  roleName,
	}).Info("role deleted")

	return authz.NewDeleteRoleNoContent()
}

func (h *authZHandlers) assignRoleToUser(params authz.AssignRoleToUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalID := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.ID)

	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewAssignRoleToUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the roles you want to assign is empty")))
		}

		if err := validateEnvVarRoles(role); err != nil {
			return authz.NewAssignRoleToUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("assigning: %w", err)))
		}
	}

	if err := h.validateUserIDForNamespaces(internalID); err != nil {
		return authz.NewAssignRoleToUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateUserTypeForNamespaces(params.Body.UserType); err != nil {
		return authz.NewAssignRoleToUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewAssignRoleToUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("roles can not be empty")))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(internalID)...); err != nil {
		return authz.NewAssignRoleToUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	roleNames, notFound, err := h.resolveAssignableRoles(principal, params.Body.Roles)
	if notFound {
		return authz.NewAssignRoleToUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the roles requested doesn't exist")))
	}
	if err != nil {
		return authz.NewAssignRoleToUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateLocalRoleAssignment(principal, roleNames); err != nil {
		return authz.NewAssignRoleToUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateOperatorRoleAssignmentToUser(namespacing.NamespaceFromQualified(internalID), roleNames); err != nil {
		return authz.NewAssignRoleToUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	existedRoles, err := h.controller.GetRoles(roleNames...)
	if err != nil {
		return authz.NewAssignRoleToUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(roleNames) {
		return authz.NewAssignRoleToUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the roles requested doesn't exist")))
	}

	if err := h.authorizeAssignRolePermissions(ctx, principal, namespacing.NamespaceFromQualified(internalID), existedRoles); err != nil {
		return authz.NewAssignRoleToUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	userTypes, err := h.getUserTypesAndValidateExistence(internalID, authentication.AuthType(params.Body.UserType))
	if err != nil {
		return authz.NewAssignRoleToUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user exists: %w", err)))
	}
	if userTypes == nil {
		return authz.NewAssignRoleToUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("username to assign role to doesn't exist")))
	}

	for _, userType := range userTypes {
		if err := h.controller.AddRolesForUser(conv.UserNameWithTypeFromId(internalID, userType), roleNames); err != nil {
			return authz.NewAssignRoleToUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("AddRolesForUser: %w", err)))
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "assign_roles",
		"component":               authorization.ComponentName,
		"user":                    principal.Username,
		"user_to_assign_roles_to": internalID,
		"roles":                   roleNames,
	}).Info("roles assigned to user")

	return authz.NewAssignRoleToUserOK()
}

func (h *authZHandlers) assignRoleToGroup(params authz.AssignRoleToGroupParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if h.callerConfined(principal) {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("assigning roles to groups is not allowed")))
	}

	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewAssignRoleToGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the roles you want to assign is empty")))
		}

		if err := validateEnvVarRoles(role); err != nil {
			return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("assigning: %w", err)))
		}
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewAssignRoleToGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("roles can not be empty")))
	}

	groupType, err := validateUserTypeInput(string(params.Body.GroupType))
	if err != nil || groupType != authentication.AuthTypeOIDC {
		return authz.NewAssignRoleToGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("unknown groupType: %v", params.Body.GroupType)))
	}

	// Groups are global; a namespace-local role can never be assigned to one.
	if err := h.validateLocalRoleAssignment(principal, params.Body.Roles); err != nil {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(groupType, params.ID)...); err != nil {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateRootGroup(params.ID); err != nil {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("assigning: %w", err)))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewAssignRoleToGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(params.Body.Roles) && len(params.Body.Roles) > 0 {
		return authz.NewAssignRoleToGroupNotFound()
	}

	// Group assignment is global-only (a namespaced caller is denied above), so
	// no namespace projection applies.
	if err := h.authorizeAssignRolePermissions(ctx, principal, "", existedRoles); err != nil {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.controller.AddRolesForUser(conv.PrefixGroupName(params.ID), params.Body.Roles); err != nil {
		return authz.NewAssignRoleToGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("AddRolesForUser: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                   "assign_roles",
		"component":                authorization.ComponentName,
		"user":                     principal.Username,
		"group_to_assign_roles_to": params.ID,
		"roles":                    params.Body.Roles,
	}).Info("roles assigned to group")

	return authz.NewAssignRoleToGroupOK()
}

// Delete this when 1.29 is not supported anymore
func (h *authZHandlers) getRolesForUserDeprecated(params authz.GetRolesForUserDeprecatedParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if h.namespacesEnabled {
		return authz.NewGetRolesForUserDeprecatedGone().WithPayload(cerrors.ErrPayloadFromSingleErr(principal,
			errors.New("endpoint is not supported in the current cluster configuration")))
	}

	ownUser := params.ID == principal.Username

	if !ownUser {
		if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Users(params.ID)...); err != nil {
			return authz.NewGetRolesForUserDeprecatedForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
	}

	exists, err := h.userExistsDeprecated(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user existence: %w", err)))
	}
	if !exists {
		return authz.NewGetRolesForUserDeprecatedNotFound()
	}

	existingRolesDB, err := h.controller.GetRolesForUserOrGroup(params.ID, authentication.AuthTypeDb, false)
	if err != nil {
		return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetUsersOrGroupsWithRoles: %w", err)))
	}
	existingRolesOIDC, err := h.controller.GetRolesForUserOrGroup(params.ID, authentication.AuthTypeOIDC, false)
	if err != nil {
		return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetUsersOrGroupsWithRoles: %w", err)))
	}

	var response []*models.Role
	foundRoles := map[string]struct{}{}
	var authErr error
	for _, existing := range []map[string][]authorization.Policy{existingRolesDB, existingRolesOIDC} {
		for roleName, policies := range existing {
			perms, err := conv.PoliciesToPermission(policies...)
			if err != nil {
				return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("PoliciesToPermission: %w", err)))
			}

			if !ownUser {
				if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, roleName); err != nil {
					authErr = err
					continue
				}
			}

			// no duplicates
			if _, ok := foundRoles[roleName]; ok {
				continue
			}

			foundRoles[roleName] = struct{}{}

			response = append(response, &models.Role{
				Name:        &roleName,
				Permissions: perms,
			})
		}
	}

	if (len(existingRolesDB) != 0 || len(existingRolesOIDC) != 0) && len(response) == 0 {
		return authz.NewGetRolesForUserDeprecatedForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, authErr))
	}

	sortByName(response)

	h.logger.WithFields(logrus.Fields{
		"action":                "get_roles_for_user",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"user_to_get_roles_for": params.ID,
	}).Info("roles requested")

	return authz.NewGetRolesForUserDeprecatedOK().WithPayload(response)
}

// visibleRolesForSubject filters a subject's assigned roles down to those the
// caller may see and converts them to API roles. Full reads hide foreign-namespace
// and reserved roles here but return 403 on content (via authorizeRoleRead);
// name-only reads hide all three silently. own relaxes reserved-role hiding so a
// self-read keeps a reserved role inherited via a group. Roles the caller may not
// read under a full read are collected into authErrs; a conversion failure returns
// an error for the caller to map to its own response.
func (h *authZHandlers) visibleRolesForSubject(ctx context.Context, principal *models.Principal, existingRoles map[string][]authorization.Policy, includeFullRoles, own bool) (roles []*models.Role, authErrs []error, err error) {
	for roleName, policies := range existingRoles {
		if includeFullRoles {
			if h.roleHiddenFromCaller(principal, roleName) {
				continue
			}
			if !own && h.roleOperatorReservedFromCaller(principal, roleName) {
				continue
			}
		} else if !own && !h.roleVisibleToCaller(ctx, principal, roleName, policies) {
			continue
		}

		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return nil, nil, fmt.Errorf("PoliciesToPermission: %w", err)
		}

		name := namespacing.StripOwnNamespace(principal, roleName)
		role := &models.Role{Name: &name}
		if includeFullRoles {
			if !own {
				if err := h.authorizeRoleRead(ctx, principal, roleName, policies); err != nil {
					authErrs = append(authErrs, err)
					continue
				}
			}
			role.Permissions = perms
		}
		roles = append(roles, role)
	}
	return roles, authErrs, nil
}

func (h *authZHandlers) getRolesForUser(params authz.GetRolesForUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalID := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.ID)

	if err := h.validateUserIDForNamespaces(internalID); err != nil {
		return authz.NewGetRolesForUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	ownUser := internalID == principal.Username && params.UserType == string(principal.UserType)

	if !ownUser {
		if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Users(internalID)...); err != nil {
			return authz.NewGetRolesForUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
	}

	includeFullRoles := params.IncludeFullRoles != nil && *params.IncludeFullRoles

	userType, err := validateUserTypeInput(params.UserType)
	if err != nil {
		return authz.NewGetRolesForUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("unknown userType: %v", params.UserType)))
	}

	exists, err := h.userExists(internalID, userType)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user existence: %w", err)))
	}
	if !exists {
		return authz.NewGetRolesForUserNotFound()
	}

	existingRoles, err := h.controller.GetRolesForUserOrGroup(internalID, userType, false)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetUsersOrGroupsWithRoles: %w", err)))
	}

	roles, authErrs, err := h.visibleRolesForSubject(ctx, principal, existingRoles, includeFullRoles, ownUser)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if len(authErrs) > 0 {
		return authz.NewGetRolesForUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.Join(authErrs...)))
	}

	// Strip the caller's own namespace prefix from each included permission body.
	roles = namespacing.StripRolesForCaller(principal, roles)
	sortByName(roles)

	h.logger.WithFields(logrus.Fields{
		"action":                "get_roles_for_user",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"user_to_get_roles_for": params.ID,
	}).Info("roles requested")

	return authz.NewGetRolesForUserOK().WithPayload(roles)
}

func (h *authZHandlers) getUsersForRole(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if err := validateEnvVarRoles(params.ID); err != nil && !slices.Contains(h.rbacconfig.RootUsers, principal.Username) {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	roleID, _, outcome, err := h.resolveRoleForRead(ctx, principal, params.ID)
	switch outcome {
	case roleReadOK:
		// proceed
	case roleReadNotFound:
		return authz.NewGetUsersForRoleNotFound()
	case roleReadBadRequest:
		return authz.NewGetUsersForRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadForbidden:
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadInternalErr:
		return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	var response []*authz.GetUsersForRoleOKBodyItems0
	for _, userType := range []authentication.AuthType{authentication.AuthTypeOIDC, authentication.AuthTypeDb} {
		users, err := h.controller.GetUsersOrGroupForRole(roleID, userType, false)
		if err != nil {
			return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetUsersOrGroupForRole: %w", err)))
		}

		filteredUsers := make([]string, 0, len(users))
		for _, userName := range users {
			if userName == principal.Username {
				// own username
				filteredUsers = append(filteredUsers, userName)
				continue
			}
			if err := h.authorizer.AuthorizeSilent(ctx, principal, authorization.READ, authorization.Users(userName)...); err == nil {
				filteredUsers = append(filteredUsers, userName)
			}
		}
		slices.Sort(filteredUsers)
		// The stored id is used for lookups; the response carries the caller's
		// own namespace stripped from the returned user id.
		if userType == authentication.AuthTypeOIDC {
			for _, userId := range filteredUsers {
				response = append(response, &authz.GetUsersForRoleOKBodyItems0{UserID: namespacing.StripOwnNamespace(principal, userId), UserType: models.NewUserTypeOutput(models.UserTypeOutputOidc)})
			}
		} else {
			dynamicUsers, err := h.controller.GetUsers(filteredUsers...)
			if err != nil {
				return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetUsers: %w", err)))
			}
			for _, userId := range filteredUsers {
				if _, ok := dynamicUsers[userId]; ok {
					response = append(response, &authz.GetUsersForRoleOKBodyItems0{UserID: namespacing.StripOwnNamespace(principal, userId), UserType: models.NewUserTypeOutput(models.UserTypeOutputDbUser)})
				} else {
					response = append(response, &authz.GetUsersForRoleOKBodyItems0{UserID: namespacing.StripOwnNamespace(principal, userId), UserType: models.NewUserTypeOutput(models.UserTypeOutputDbEnvUser)})
				}
			}

		}

	}

	h.logger.WithFields(logrus.Fields{
		"action":                "get_users_for_role",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"role_to_get_users_for": params.ID,
	}).Info("users requested")

	return authz.NewGetUsersForRoleOK().WithPayload(response)
}

func (h *authZHandlers) getGroupsForRole(params authz.GetGroupsForRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if err := validateEnvVarRoles(params.ID); err != nil && !slices.Contains(h.rbacconfig.RootUsers, principal.Username) {
		return authz.NewGetGroupsForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	roleID, _, outcome, err := h.resolveRoleForRead(ctx, principal, params.ID)
	switch outcome {
	case roleReadOK:
		// proceed
	case roleReadNotFound:
		return authz.NewGetGroupsForRoleNotFound()
	case roleReadBadRequest:
		return authz.NewGetGroupsForRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadForbidden:
		return authz.NewGetGroupsForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	case roleReadInternalErr:
		return authz.NewGetGroupsForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	groups, err := h.controller.GetUsersOrGroupForRole(roleID, authentication.AuthTypeOIDC, true)
	if err != nil {
		return authz.NewGetGroupsForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetUsersOrGroupForRole: %w", err)))
	}

	filteredGroups := make([]string, 0, len(groups))
	for _, groupName := range groups {
		if slices.Contains(principal.Groups, groupName) {
			// own group
			filteredGroups = append(filteredGroups, groupName)
			continue
		}
		if err := h.authorizer.AuthorizeSilent(ctx, principal, authorization.READ, authorization.Groups(authentication.AuthTypeOIDC, groupName)...); err == nil {
			filteredGroups = append(filteredGroups, groupName)
		}
	}
	slices.Sort(filteredGroups)

	// only OIDC groups so far
	oidc := models.GroupTypeOidc
	var response []*authz.GetGroupsForRoleOKBodyItems0
	for _, groupID := range filteredGroups {
		response = append(response, &authz.GetGroupsForRoleOKBodyItems0{GroupID: groupID, GroupType: &oidc})
	}

	h.logger.WithFields(logrus.Fields{
		"action":                "get_groups_for_role",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"role_to_get_users_for": params.ID,
	}).Info("groups requested")

	return authz.NewGetGroupsForRoleOK().WithPayload(response)
}

// Delete this when 1.29 is not supported anymore
func (h *authZHandlers) getUsersForRoleDeprecated(params authz.GetUsersForRoleDeprecatedParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if h.namespacesEnabled {
		return authz.NewGetUsersForRoleDeprecatedGone().WithPayload(cerrors.ErrPayloadFromSingleErr(principal,
			errors.New("endpoint is not supported in the current cluster configuration")))
	}

	if err := validateEnvVarRoles(params.ID); err != nil && !slices.Contains(h.rbacconfig.RootUsers, principal.Username) {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, params.ID); err != nil {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	foundUsers := map[string]struct{}{} // no duplicates
	filteredUsers := make([]string, 0)

	for _, userType := range []authentication.AuthType{authentication.AuthTypeDb, authentication.AuthTypeOIDC} {
		users, err := h.controller.GetUsersOrGroupForRole(params.ID, userType, false)
		if err != nil {
			return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetUsersOrGroupForRole: %w", err)))
		}

		for _, userName := range users {
			if _, ok := foundUsers[userName]; ok {
				continue
			}
			foundUsers[userName] = struct{}{}

			if userName == principal.Username {
				// own username
				filteredUsers = append(filteredUsers, userName)
				continue
			}
			if err := h.authorizer.AuthorizeSilent(ctx, principal, authorization.READ, authorization.Users(userName)...); err == nil {
				filteredUsers = append(filteredUsers, userName)
			}
		}

	}

	slices.Sort(filteredUsers)

	h.logger.WithFields(logrus.Fields{
		"action":                "get_users_for_role",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"role_to_get_users_for": params.ID,
	}).Info("users requested")

	return authz.NewGetUsersForRoleDeprecatedOK().WithPayload(filteredUsers)
}

func (h *authZHandlers) revokeRoleFromUser(params authz.RevokeRoleFromUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	internalID := namespacing.QualifyUserIDForLookup(principal, h.namespacesEnabled, params.ID)

	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewRevokeRoleFromUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the roles you want to revoke is empty")))
		}

		if err := validateEnvVarRoles(role); err != nil {
			return authz.NewRevokeRoleFromUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("revoking: %w", err)))
		}
	}

	if err := h.validateUserIDForNamespaces(internalID); err != nil {
		return authz.NewRevokeRoleFromUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateUserTypeForNamespaces(params.Body.UserType); err != nil {
		return authz.NewRevokeRoleFromUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewRevokeRoleFromUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("roles can not be empty")))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Users(internalID)...); err != nil {
		return authz.NewRevokeRoleFromUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	roleNames, notFound, err := h.resolveAssignableRoles(principal, params.Body.Roles)
	if notFound {
		return authz.NewRevokeRoleFromUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the request roles doesn't exist")))
	}
	if err != nil {
		return authz.NewRevokeRoleFromUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateLocalRoleAssignment(principal, roleNames); err != nil {
		return authz.NewRevokeRoleFromUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	existedRoles, err := h.controller.GetRoles(roleNames...)
	if err != nil {
		return authz.NewRevokeRoleFromUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(roleNames) {
		return authz.NewRevokeRoleFromUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the request roles doesn't exist")))
	}

	userTypes, err := h.getUserTypesAndValidateExistence(internalID, authentication.AuthType(params.Body.UserType))
	if err != nil {
		return authz.NewRevokeRoleFromUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("user exists: %w", err)))
	}
	if userTypes == nil {
		return authz.NewRevokeRoleFromUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("username to revoke role from doesn't exist")))
	}
	for _, userType := range userTypes {
		if err := h.controller.RevokeRolesForUser(conv.UserNameWithTypeFromId(internalID, userType), roleNames...); err != nil {
			return authz.NewRevokeRoleFromUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("RevokeRolesForUser: %w", err)))
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "revoke_roles",
		"component":               authorization.ComponentName,
		"user":                    principal.Username,
		"user_to_assign_roles_to": internalID,
		"roles":                   roleNames,
	}).Info("roles revoked from user")

	return authz.NewRevokeRoleFromUserOK()
}

func (h *authZHandlers) revokeRoleFromGroup(params authz.RevokeRoleFromGroupParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if h.callerConfined(principal) {
		return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("revoking roles from groups is not allowed")))
	}

	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewRevokeRoleFromGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("one or more of the roles you want to revoke is empty")))
		}

		if err := validateEnvVarRoles(role); err != nil {
			return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("revoking: %w", err)))
		}
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewRevokeRoleFromGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("roles can not be empty")))
	}

	groupType, err := validateUserTypeInput(string(params.Body.GroupType))
	if err != nil || groupType != authentication.AuthTypeOIDC {
		return authz.NewRevokeRoleFromGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("unknown groupType: %v", params.Body.GroupType)))
	}

	// Groups are global; a namespace-local role can never be on one.
	if err := h.validateLocalRoleAssignment(principal, params.Body.Roles); err != nil {
		return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.Groups(groupType, params.ID)...); err != nil {
		return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	if err := h.validateRootGroup(params.ID); err != nil {
		return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("revoking: %w", err)))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewRevokeRoleFromGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewRevokeRoleFromGroupNotFound()
	}

	if err := h.controller.RevokeRolesForUser(conv.PrefixGroupName(params.ID), params.Body.Roles...); err != nil {
		return authz.NewRevokeRoleFromGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("RevokeRolesForGroup: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                   "revoke_roles",
		"component":                authorization.ComponentName,
		"user":                     principal.Username,
		"group_to_assign_roles_to": params.ID,
		"roles":                    params.Body.Roles,
	}).Info("roles revoked from group")

	return authz.NewRevokeRoleFromGroupOK()
}

func (h *authZHandlers) getGroups(params authz.GetGroupsParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	groupType, err := validateUserTypeInput(params.GroupType)
	if err != nil || groupType != authentication.AuthTypeOIDC {
		return authz.NewGetGroupsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("unknown groupType: %v", params.GroupType)))
	}

	groups, err := h.controller.GetUsersOrGroupsWithRoles(true, groupType)
	if err != nil {
		return nil
	}

	// Filter roles based on authorization
	resourceFilter := filter.New[string](h.authorizer, h.rbacconfig)
	filteredGroups := resourceFilter.Filter(
		ctx,
		principal,
		groups,
		authorization.READ,
		func(group string) string {
			return authorization.Groups(authentication.AuthTypeOIDC, group)[0]
		},
	)

	h.logger.WithFields(logrus.Fields{
		"action":    "get_groups",
		"component": authorization.ComponentName,
		"user":      principal.Username,
	}).Info("groups requested")

	return authz.NewGetGroupsOK().WithPayload(filteredGroups)
}

func (h *authZHandlers) getRolesForGroup(params authz.GetRolesForGroupParams, principal *models.Principal) middleware.Responder {
	ownGroup := slices.Contains(principal.Groups, params.ID) && params.GroupType == string(principal.UserType)
	ctx := params.HTTPRequest.Context()

	groupType, err := validateUserTypeInput(params.GroupType)
	if err != nil || groupType != authentication.AuthTypeOIDC {
		return authz.NewGetRolesForGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("unknown groupType: %v", params.GroupType)))
	}

	if !ownGroup {
		if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Groups(groupType, params.ID)...); err != nil {
			return authz.NewGetRolesForGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
	}

	includeFullRoles := params.IncludeFullRoles != nil && *params.IncludeFullRoles

	existingRoles, err := h.controller.GetRolesForUserOrGroup(params.ID, groupType, true)
	if err != nil {
		return authz.NewGetRolesForGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("GetRolesForUserOrGroup: %w", err)))
	}

	roles, authErrs, err := h.visibleRolesForSubject(ctx, principal, existingRoles, includeFullRoles, ownGroup)
	if err != nil {
		return authz.NewGetRolesForGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if len(authErrs) > 0 {
		return authz.NewGetRolesForGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errors.Join(authErrs...)))
	}

	// Strip the caller's own namespace prefix from each included permission body.
	roles = namespacing.StripRolesForCaller(principal, roles)
	sortByName(roles)

	h.logger.WithFields(logrus.Fields{
		"action":                 "get_roles_for_group",
		"component":              authorization.ComponentName,
		"user":                   principal.Username,
		"group_to_get_roles_for": params.ID,
	}).Info("roles for group requested")

	return authz.NewGetRolesForGroupOK().WithPayload(roles)
}

func (h *authZHandlers) userExists(user string, userType authentication.AuthType) (bool, error) {
	switch userType {
	case authentication.AuthTypeOIDC:
		if !h.oidcConfigs.Enabled {
			return false, fmt.Errorf("oidc is not enabled")
		}
		return true, nil
	case authentication.AuthTypeDb:
		if h.apiKeysConfigs.Enabled {
			for _, apiKey := range h.apiKeysConfigs.Users {
				if apiKey == user {
					return true, nil
				}
			}
		}

		users, err := h.controller.GetUsers(user)
		if err != nil {
			return false, err
		}
		if len(users) == 1 {
			return true, nil
		} else {
			return false, nil
		}
	default:
		return false, fmt.Errorf("unknown user type")
	}
}

func (h *authZHandlers) userExistsDeprecated(user string) (bool, error) {
	// We are only able to check if a user is present on the system if APIKeys are the only auth method. For OIDC
	// users are managed in an external service and there is no general way to check if a user we have not seen yet is
	// valid.
	if h.oidcConfigs.Enabled {
		return true, nil
	}

	if h.apiKeysConfigs.Enabled {
		for _, apiKey := range h.apiKeysConfigs.Users {
			if apiKey == user {
				return true, nil
			}
		}
	}

	users, err := h.controller.GetUsers(user)
	if err != nil {
		return false, err
	}
	if len(users) == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

// validateRootGroup validates that enduser do not touch the internal root group
func (h *authZHandlers) validateRootGroup(name string) error {
	if slices.Contains(h.rbacconfig.RootGroups, name) || slices.Contains(h.rbacconfig.ReadOnlyGroups, name) {
		return fmt.Errorf("cannot assign or revoke from root group %s", name)
	}
	return nil
}

func (h *authZHandlers) getUserTypesAndValidateExistence(id string, userTypeParam authentication.AuthType) ([]authentication.AuthType, error) {
	if userTypeParam == "" {
		exists, err := h.userExistsDeprecated(id)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}

		return []authentication.AuthType{authentication.AuthTypeOIDC, authentication.AuthTypeDb}, nil
	} else {
		exists, err := h.userExists(id, userTypeParam)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}

		return []authentication.AuthType{userTypeParam}, nil
	}
}

// validateEnvVarRoles validates that enduser do not touch the internal root role
func validateEnvVarRoles(name string) error {
	if slices.Contains(authorization.EnvVarRoles, name) {
		return fmt.Errorf("modifying '%s' role or changing its assignments is not allowed", name)
	}
	return nil
}

// validateUserIDForNamespaces rejects bare-form user IDs on
// namespace-enabled clusters. Static API-key users are intentionally bare
// and global; they pass through unchanged.
func (h *authZHandlers) validateUserIDForNamespaces(userID string) error {
	if !h.namespacesEnabled {
		return nil
	}
	if h.apiKeysConfigs.Enabled && slices.Contains(h.apiKeysConfigs.Users, userID) {
		return nil
	}
	if !conv.ContainsNamespaceSeparator(userID) {
		return fmt.Errorf("user IDs on namespace-enabled clusters must be namespace-prefixed (e.g. \"customer1:alice\"); bare-form IDs are only accepted for static API-key users")
	}
	return nil
}

// validateUserTypeForNamespaces requires an explicit userType on
// namespace-enabled clusters: the empty-userType fan-out over {db,oidc} could
// resolve an ambiguous short id to the wrong subject or merge identities.
// No-op when namespaces are disabled.
func (h *authZHandlers) validateUserTypeForNamespaces(userType models.UserTypeInput) error {
	if !h.namespacesEnabled {
		return nil
	}
	if userType == "" {
		return fmt.Errorf("userType is required")
	}
	return nil
}

// resolveAssignableRoles maps caller-supplied role names to their stored form
// for assign/revoke. notFound reports that a namespaced caller named a role
// resolving to neither a local nor a global role; err carries a malformed-name
// rejection.
func (h *authZHandlers) resolveAssignableRoles(principal *models.Principal, roles []string) (names []string, notFound bool, err error) {
	names = make([]string, 0, len(roles))
	for _, role := range roles {
		stored, err := h.resolveRoleNameForCaller(principal, role)
		if errors.Is(err, namespacing.ErrRoleNotFound) {
			return nil, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		names = append(names, stored)
	}
	return names, false, nil
}

// validateNoQualifiedNamespaceInPolicies rejects role-definition policies that
// carry an explicit namespace qualification. On namespace-enabled clusters role
// definitions must stay namespace-relative templates; the matcher specializes
// them at enforce time. No-op when namespaces are disabled.
//
// A ':' in a users/<id> or groups/<...> id is part of the opaque id (e.g. an
// OIDC username), not a qualifier, so a global caller may use it. Namespaced
// callers must still submit short, unqualified forms — symmetric with the
// class-name rule that forbids them typing any "<namespace>:" prefix. So a
// namespaced caller cannot express a colon-bearing group id; acceptable while
// group grants are global-only. Revisit if namespaced callers gain them.
//
// allowGlobalQualified lifts the restriction for a global caller, used by the
// hasPermission read path so an operator can name a namespace-local role's
// qualified rows. Confined callers stay restricted.
func (h *authZHandlers) validateNoQualifiedNamespaceInPolicies(principal *models.Principal, policies []authorization.Policy, allowGlobalQualified bool) error {
	if !h.namespacesEnabled {
		return nil
	}
	global := namespacing.ConfinedNamespace(principal) == ""
	if global && allowGlobalQualified {
		return nil
	}
	for _, p := range policies {
		if !conv.ContainsNamespaceSeparator(p.Resource) {
			continue
		}
		if global && conv.IsOpaqueIDResource(p.Resource) {
			continue
		}
		return fmt.Errorf("role permissions must not contain namespace-qualified resource paths; got %q", p.Resource)
	}
	return nil
}

// validateRoleName validates that this string is a valid role name (format wise)
func validateRoleName(name string) error {
	if len(name) > roleNameMaxLength {
		return fmt.Errorf("'%s' is not a valid role name. Name should not be longer than %d characters", name, roleNameMaxLength)
	}
	if !validateRoleNameRegex.MatchString(name) {
		return fmt.Errorf("'%s' is not a valid role name", name)
	}
	return nil
}

func sortByName(roles []*models.Role) {
	slices.SortFunc(roles, func(a, b *models.Role) int {
		return strings.Compare(*a.Name, *b.Name)
	})
}

func validateUserTypeInput(userTypeInput string) (authentication.AuthType, error) {
	var userType authentication.AuthType
	if userTypeInput == string(authentication.AuthTypeOIDC) {
		userType = authentication.AuthTypeOIDC
	} else if userTypeInput == string(authentication.AuthTypeDb) {
		userType = authentication.AuthTypeDb
	} else {
		return userType, fmt.Errorf("unknown userType: %v", userTypeInput)
	}
	return userType, nil
}

// TODO-RBAC: we could expose endpoint to validate permissions as dry-run
// func (h *authZHandlers) validatePermissions(permissions []*models.Permission) error {
// 	for _, perm := range permissions {
// 		if perm == nil {
// 			continue
// 		}

// 		// collection filtration
// 		if perm.Collection != nil && *perm.Collection != "" && *perm.Collection != "*" {
// 			if class := h.schemaReader.ReadOnlyClass(*perm.Collection); class == nil {
// 				return fmt.Errorf("collection %s doesn't exists", *perm.Collection)
// 			}
// 		}

// 		// tenants filtration specific collection, specific tenant
// 		if perm.Collection != nil && *perm.Collection != "" && *perm.Collection != "*" && perm.Tenant != nil && *perm.Tenant != "" && *perm.Tenant != "*" {
// 			shardsStatus, err := h.schemaReader.TenantsShards(context.Background(), *perm.Collection, *perm.Tenant)
// 			if err != nil {
// 				return fmt.Errorf("err while fetching collection '%s', tenant '%s', %s", *perm.Collection, *perm.Tenant, err)
// 			}

// 			if _, ok := shardsStatus[*perm.Tenant]; !ok {
// 				return fmt.Errorf("tenant %s doesn't exists", *perm.Tenant)
// 			}
// 		}

// 		// tenants filtration all collections, specific tenant
// 		if (perm.Collection == nil || *perm.Collection == "" || *perm.Collection == "*") && perm.Tenant != nil && *perm.Tenant != "" && *perm.Tenant != "*" {
// 			schema := h.schemaReader.GetSchemaSkipAuth()
// 			for _, class := range schema.Objects.Classes {
// 				//NOTE: CopyShardingState not available anymore
// 				state := h.schemaReader.CopyShardingState(class.Class)
// 				if state == nil {
// 					continue
// 				}
// 				if _, ok := state.Physical[*perm.Tenant]; ok {
// 					// exists
// 					return nil
// 				}
// 			}
// 			return fmt.Errorf("tenant %s doesn't exists", *perm.Tenant)
// 		}

// 		// TODO validate mapping filter to weaviate permissions
// 		// TODO users checking
// 		// TODO roles checking
// 		// TODO object checking
// 	}

// 	return nil
// }
