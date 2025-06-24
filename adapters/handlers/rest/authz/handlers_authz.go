//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

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
)

const (
	roleNameMaxLength = 64
	roleNameRegexCore = `[A-Za-z][-_0-9A-Za-z]{0,254}`
)

var validateRoleNameRegex = regexp.MustCompile(`^` + roleNameRegexCore + `$`)

type authZHandlers struct {
	authorizer     authorization.Authorizer
	controller     ControllerAndGetUsers
	schemaReader   schemaUC.SchemaGetter
	logger         logrus.FieldLogger
	metrics        *monitoring.PrometheusMetrics
	apiKeysConfigs config.StaticAPIKey
	oidcConfigs    config.OIDC
	rbacconfig     rbacconf.Config
}

type ControllerAndGetUsers interface {
	authorization.Controller
	GetUsers(userIds ...string) (map[string]*apikey.User, error)
}

func SetupHandlers(api *operations.WeaviateAPI, controller ControllerAndGetUsers, schemaReader schemaUC.SchemaGetter,
	apiKeysConfigs config.StaticAPIKey, oidcConfigs config.OIDC, rconfig rbacconf.Config, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	h := &authZHandlers{
		controller:     controller,
		authorizer:     authorizer,
		schemaReader:   schemaReader,
		rbacconfig:     rconfig,
		oidcConfigs:    oidcConfigs,
		apiKeysConfigs: apiKeysConfigs,
		logger:         logger,
		metrics:        metrics,
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
	api.AuthzAssignRoleToGroupHandler = authz.AssignRoleToGroupHandlerFunc(h.assignRoleToGroup)
	api.AuthzRevokeRoleFromGroupHandler = authz.RevokeRoleFromGroupHandlerFunc(h.revokeRoleFromGroup)
	api.AuthzGetRolesForUserDeprecatedHandler = authz.GetRolesForUserDeprecatedHandlerFunc(h.getRolesForUserDeprecated)
}

func (h *authZHandlers) authorizeRoleScopes(ctx context.Context, principal *models.Principal, originalVerb string, policies []authorization.Policy, roleName string) error {
	// The error will be accumulated with each check. We first verify if the user has the necessary permissions.
	// If not, we check for matching permissions and authorize each permission being added or removed from the role.
	// NOTE: logic is inverted for error checks if err == nil
	var err error
	if err = h.authorizer.Authorize(ctx, principal, authorization.VerbWithScope(originalVerb, authorization.ROLE_SCOPE_ALL), authorization.Roles(roleName)...); err == nil {
		return nil
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

func (h *authZHandlers) createRole(params authz.CreateRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if *params.Body.Name == "" {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("role name is required")))
	}

	if err := validateRoleName(*params.Body.Name); err != nil {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("role name is invalid")))
	}

	if err := validatePermissions(true, params.Body.Permissions...); err != nil {
		return authz.NewCreateRoleUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("role permissions are invalid: %w", err)))
	}

	policies, err := conv.RolesToPolicies(params.Body)
	if err != nil {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid role: %w", err)))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you cannot create role with the same name as built-in role %s", *params.Body.Name)))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.CREATE, policies[*params.Body.Name], *params.Body.Name); err != nil {
		return authz.NewCreateRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles(*params.Body.Name)
	if err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	if len(roles) > 0 {
		return authz.NewCreateRoleConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("role with name %s already exists", *params.Body.Name)))
	}

	if err = h.controller.CreateRolesPermissions(policies); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "create_role",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    params.Body.Name,
		"permissions": params.Body.Permissions,
	}).Info("role created")

	return authz.NewCreateRoleCreated()
}

func (h *authZHandlers) addPermissions(params authz.AddPermissionsParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not update built-in role %s", params.ID)))
	}

	if err := validatePermissions(false, params.Body.Permissions...); err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permissions %w", err)))
	}

	policies, err := conv.RolesToPolicies(&models.Role{
		Name:        &params.ID,
		Permissions: params.Body.Permissions,
	})
	if err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permissions %w", err)))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.UPDATE, policies[params.ID], params.ID); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles(params.ID)
	if err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	if len(roles) == 0 { // i.e. new role
		return authz.NewAddPermissionsNotFound()
	}

	if err := h.controller.UpdateRolesPermissions(policies); err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "add_permissions",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    params.ID,
		"permissions": params.Body.Permissions,
	}).Info("permissions added")

	return authz.NewAddPermissionsOK()
}

func (h *authZHandlers) removePermissions(params authz.RemovePermissionsParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	// we don't validate permissions entity existence
	// in case of the permissions gets removed after the entity got removed
	// delete class ABC, then remove permissions on class ABC
	if err := validatePermissions(false, params.Body.Permissions...); err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permissions %w", err)))
	}

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you cannot update built-in role %s", params.ID)))
	}

	permissions, err := conv.PermissionToPolicies(params.Body.Permissions...)
	if err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permissions %w", err)))
	}
	// TODO-RBAC PermissionToPolicies has to be []Policy{} not slice of pointers
	policies := map[string][]authorization.Policy{
		params.ID: {},
	}
	for _, p := range permissions {
		policies[params.ID] = append(policies[params.ID], *p)
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.UPDATE, policies[params.ID], params.ID); err != nil {
		return authz.NewRemovePermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	role, err := h.controller.GetRoles(params.ID)
	if err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	if len(role) == 0 {
		return authz.NewRemovePermissionsNotFound()
	}

	if err := h.controller.RemovePermissions(params.ID, permissions); err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("RemovePermissions: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "remove_permissions",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    params.ID,
		"permissions": params.Body.Permissions,
	}).Info("permissions removed")

	return authz.NewRemovePermissionsOK()
}

func (h *authZHandlers) hasPermission(params authz.HasPermissionParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if params.Body == nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("permission is required")))
	}

	if err := validatePermissions(false, params.Body); err != nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permissions %w", err)))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, params.ID); err != nil {
		return authz.NewHasPermissionForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	policy, err := conv.PermissionToPolicies(params.Body)
	if err != nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permissions %w", err)))
	}
	if len(policy) == 0 {
		return authz.NewHasPermissionInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("unknown error occurred passing permission to policy")))
	}

	hasPermission, err := h.controller.HasPermission(params.ID, policy[0])
	if err != nil {
		return authz.NewHasPermissionInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("HasPermission: %w", err)))
	}

	return authz.NewHasPermissionOK().WithPayload(hasPermission)
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	roles, err := h.controller.GetRoles()
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	var response []*models.Role
	for roleName, policies := range roles {
		if roleName == authorization.Root && !slices.Contains(h.rbacconfig.RootUsers, principal.Username) {
			continue
		}

		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("PoliciesToPermission: %w", err)))
		}
		response = append(response, &models.Role{
			Name:        &roleName,
			Permissions: perms,
		})
	}

	// Filter roles based on authorization
	resourceFilter := filter.New[*models.Role](h.authorizer, h.rbacconfig)
	filteredRoles := resourceFilter.Filter(
		ctx,
		h.logger,
		principal,
		response,
		authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
		func(role *models.Role) string {
			return authorization.Roles(*role.Name)[0]
		},
	)
	if len(filteredRoles) == 0 {
		// try match if all was none
		filteredRoles = resourceFilter.Filter(
			ctx,
			h.logger,
			principal,
			response,
			authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH),
			func(role *models.Role) string {
				return authorization.Roles(*role.Name)[0]
			},
		)
	}

	sortByName(filteredRoles)

	logFields := logrus.Fields{
		"action":    "read_all_roles",
		"component": authorization.ComponentName,
	}

	if principal != nil {
		logFields["user"] = principal.Username
	}

	h.logger.WithFields(logFields).Info("roles requested")

	return authz.NewGetRolesOK().WithPayload(filteredRoles)
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, params.ID); err != nil {
		return authz.NewGetRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles(params.ID)
	if err != nil {
		return authz.NewGetRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}
	if len(roles) == 0 {
		return authz.NewGetRoleNotFound()
	}
	if len(roles) != 1 {
		err := fmt.Errorf("expected one role but got %d", len(roles))
		return authz.NewGetRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	perms, err := conv.PoliciesToPermission(roles[params.ID]...)
	if err != nil {
		return authz.NewGetRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("PoliciesToPermission: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":    "read_role",
		"component": authorization.ComponentName,
		"user":      principal.Username,
		"role_id":   params.ID,
	}).Info("role requested")

	return authz.NewGetRoleOK().WithPayload(&models.Role{
		Name:        &params.ID,
		Permissions: perms,
	})
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewDeleteRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not delete built-in role %s", params.ID)))
	}

	roles, err := h.controller.GetRoles(params.ID)
	if err != nil {
		h.logger.WithFields(logrus.Fields{
			"action":    "delete_role",
			"component": authorization.ComponentName,
			"user":      principal.Username,
			"roleName":  params.ID,
		}).Info("role was already deleted")
		return authz.NewDeleteRoleNoContent()
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.DELETE, roles[params.ID], params.ID); err != nil {
		return authz.NewDeleteRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.controller.DeleteRoles(params.ID); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("DeleteRoles: %w", err)))
	}

	h.logger.WithFields(logrus.Fields{
		"action":    "delete_role",
		"component": authorization.ComponentName,
		"user":      principal.Username,
		"roleName":  params.ID,
	}).Info("role deleted")

	return authz.NewDeleteRoleNoContent()
}

func (h *authZHandlers) assignRoleToUser(params authz.AssignRoleToUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewAssignRoleToUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to assign is empty")))
		}

		if err := validateRootRole(role); err != nil {
			return authz.NewAssignRoleToUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("assigning: %w", err)))
		}
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewAssignRoleToUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("roles can not be empty")))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.USER_ASSIGN_AND_REVOKE, authorization.Users(params.ID)...); err != nil {
		return authz.NewAssignRoleToUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewAssignRoleToUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewAssignRoleToUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles requested doesn't exist")))
	}

	userTypes, err := h.getUserTypesAndValidateExistence(params.ID, params.Body.UserType)
	if err != nil {
		return authz.NewAssignRoleToUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user exists: %w", err)))
	}
	if userTypes == nil {
		return authz.NewAssignRoleToUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("username to assign role to doesn't exist")))
	}
	for _, userType := range userTypes {
		if err := h.controller.AddRolesForUser(conv.UserNameWithTypeFromId(params.ID, userType), params.Body.Roles); err != nil {
			return authz.NewAssignRoleToUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("AddRolesForUser: %w", err)))
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "assign_roles",
		"component":               authorization.ComponentName,
		"user":                    principal.Username,
		"user_to_assign_roles_to": params.ID,
		"roles":                   params.Body.Roles,
	}).Info("roles assigned to user")

	return authz.NewAssignRoleToUserOK()
}

func (h *authZHandlers) assignRoleToGroup(params authz.AssignRoleToGroupParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewAssignRoleToGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to assign is empty")))
		}

		if err := validateRootRole(role); err != nil {
			return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("assigning: %w", err)))
		}
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewAssignRoleToGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("roles can not be empty")))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(params.Body.Roles...)...); err != nil {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if !h.isRootUser(principal) {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("assigning: only root users can assign roles to groups")))
	}

	if err := h.validateRootGroup(params.ID); err != nil {
		return authz.NewAssignRoleToGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("assigning: %w", err)))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewAssignRoleToGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(params.Body.Roles) && len(params.Body.Roles) > 0 {
		return authz.NewAssignRoleToGroupNotFound()
	}

	if err := h.controller.AddRolesForUser(conv.PrefixGroupName(params.ID), params.Body.Roles); err != nil {
		return authz.NewAssignRoleToGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("AddRolesForUser: %w", err)))
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

	ownUser := params.ID == principal.Username

	if !ownUser {
		if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Users(params.ID)...); err != nil {
			return authz.NewGetRolesForUserDeprecatedForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
	}

	exists, err := h.userExistsDeprecated(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user existence: %w", err)))
	}
	if !exists {
		return authz.NewGetRolesForUserDeprecatedNotFound()
	}

	existingRolesDB, err := h.controller.GetRolesForUser(params.ID, models.UserTypeInputDb)
	if err != nil {
		return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRolesForUser: %w", err)))
	}
	existingRolesOIDC, err := h.controller.GetRolesForUser(params.ID, models.UserTypeInputOidc)
	if err != nil {
		return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRolesForUser: %w", err)))
	}

	var response []*models.Role
	foundRoles := map[string]struct{}{}
	var authErr error
	for _, existing := range []map[string][]authorization.Policy{existingRolesDB, existingRolesOIDC} {
		for roleName, policies := range existing {
			perms, err := conv.PoliciesToPermission(policies...)
			if err != nil {
				return authz.NewGetRolesForUserDeprecatedInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("PoliciesToPermission: %w", err)))
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
		return authz.NewGetRolesForUserDeprecatedForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(authErr))
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

func (h *authZHandlers) getRolesForUser(params authz.GetRolesForUserParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	ownUser := params.ID == principal.Username && params.UserType == string(principal.UserType)

	if !ownUser {
		if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Users(params.ID)...); err != nil {
			return authz.NewGetRolesForUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
	}

	includeFullRoles := params.IncludeFullRoles != nil && *params.IncludeFullRoles

	userType, err := validateUserTypeInput(params.UserType)
	if err != nil {
		return authz.NewGetRolesForUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("unknown userType: %v", params.UserType)))
	}

	exists, err := h.userExists(params.ID, userType)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user existence: %w", err)))
	}
	if !exists {
		return authz.NewGetRolesForUserNotFound()
	}

	existingRoles, err := h.controller.GetRolesForUser(params.ID, userType)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRolesForUser: %w", err)))
	}

	var roles []*models.Role
	var authErrs []error
	for roleName, policies := range existingRoles {
		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("PoliciesToPermission: %w", err)))
		}

		role := &models.Role{Name: &roleName}
		if includeFullRoles {
			if !ownUser {
				if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, roleName); err != nil {
					authErrs = append(authErrs, err)
					continue
				}
			}
			role.Permissions = perms
		}
		roles = append(roles, role)
	}

	if len(authErrs) > 0 {
		return authz.NewGetRolesForUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.Join(authErrs...)))
	}

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

	if err := validateRootRole(params.ID); err != nil && !slices.Contains(h.rbacconfig.RootUsers, principal.Username) {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, params.ID); err != nil {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	var response []*authz.GetUsersForRoleOKBodyItems0
	for _, userType := range []models.UserTypeInput{models.UserTypeInputOidc, models.UserTypeInputDb} {
		users, err := h.controller.GetUsersForRole(params.ID, userType)
		if err != nil {
			return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetUsersForRole: %w", err)))
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
		if userType == models.UserTypeInputOidc {
			for _, userId := range filteredUsers {
				response = append(response, &authz.GetUsersForRoleOKBodyItems0{UserID: userId, UserType: models.NewUserTypeOutput(models.UserTypeOutputOidc)})
			}
		} else {
			dynamicUsers, err := h.controller.GetUsers(filteredUsers...)
			if err != nil {
				return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetUsers: %w", err)))
			}
			for _, userId := range filteredUsers {
				if _, ok := dynamicUsers[userId]; ok {
					response = append(response, &authz.GetUsersForRoleOKBodyItems0{UserID: userId, UserType: models.NewUserTypeOutput(models.UserTypeOutputDbUser)})
				} else {
					response = append(response, &authz.GetUsersForRoleOKBodyItems0{UserID: userId, UserType: models.NewUserTypeOutput(models.UserTypeOutputDbEnvUser)})
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

// Delete this when 1.29 is not supported anymore
func (h *authZHandlers) getUsersForRoleDeprecated(params authz.GetUsersForRoleDeprecatedParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if err := validateRootRole(params.ID); err != nil && !slices.Contains(h.rbacconfig.RootUsers, principal.Username) {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.authorizeRoleScopes(ctx, principal, authorization.READ, nil, params.ID); err != nil {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	foundUsers := map[string]struct{}{} // no duplicates
	filteredUsers := make([]string, 0)

	for _, userType := range []models.UserTypeInput{models.UserTypeInputDb, models.UserTypeInputOidc} {
		users, err := h.controller.GetUsersForRole(params.ID, userType)
		if err != nil {
			return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetUsersForRole: %w", err)))
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

	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewRevokeRoleFromUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to revoke is empty")))
		}

		if err := validateRootRole(role); err != nil {
			return authz.NewRevokeRoleFromUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("revoking: %w", err)))
		}
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewRevokeRoleFromUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("roles can not be empty")))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.USER_ASSIGN_AND_REVOKE, authorization.Users(params.ID)...); err != nil {
		return authz.NewRevokeRoleFromUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewRevokeRoleFromUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewRevokeRoleFromUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the request roles doesn't exist")))
	}

	userTypes, err := h.getUserTypesAndValidateExistence(params.ID, params.Body.UserType)
	if err != nil {
		return authz.NewRevokeRoleFromUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user exists: %w", err)))
	}
	if userTypes == nil {
		return authz.NewRevokeRoleFromUserNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("username to revoke role from doesn't exist")))
	}
	for _, userType := range userTypes {
		if err := h.controller.RevokeRolesForUser(conv.UserNameWithTypeFromId(params.ID, userType), params.Body.Roles...); err != nil {
			return authz.NewRevokeRoleFromUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("AddRolesForUser: %w", err)))
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "revoke_roles",
		"component":               authorization.ComponentName,
		"user":                    principal.Username,
		"user_to_assign_roles_to": params.ID,
		"roles":                   params.Body.Roles,
	}).Info("roles revoked from user")

	return authz.NewRevokeRoleFromUserOK()
}

func (h *authZHandlers) revokeRoleFromGroup(params authz.RevokeRoleFromGroupParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewRevokeRoleFromGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to revoke is empty")))
		}

		if err := validateRootRole(role); err != nil {
			return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("revoking: %w", err)))
		}
	}

	if len(params.Body.Roles) == 0 {
		return authz.NewRevokeRoleFromGroupBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("roles can not be empty")))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL), authorization.Roles(params.Body.Roles...)...); err != nil {
		return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if !h.isRootUser(principal) {
		return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("revoking: only root users can revoke roles from groups")))
	}

	if err := h.validateRootGroup(params.ID); err != nil {
		return authz.NewRevokeRoleFromGroupForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("revoking: %w", err)))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewRevokeRoleFromGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("GetRoles: %w", err)))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewRevokeRoleFromGroupNotFound()
	}

	if err := h.controller.RevokeRolesForUser(conv.PrefixGroupName(params.ID), params.Body.Roles...); err != nil {
		return authz.NewRevokeRoleFromGroupInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("RevokeRolesForUser: %w", err)))
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

func (h *authZHandlers) userExists(user string, userType models.UserTypeInput) (bool, error) {
	switch userType {
	case models.UserTypeInputOidc:
		if !h.oidcConfigs.Enabled {
			return false, fmt.Errorf("oidc is not enabled")
		}
		return true, nil
	case models.UserTypeInputDb:
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
	if slices.Contains(h.rbacconfig.RootGroups, name) || slices.Contains(h.rbacconfig.ViewerGroups, name) {
		return fmt.Errorf("cannot assign or revoke from root group %s", name)
	}
	return nil
}

// isRootUser checks that the provided username belongs to the root users list
func (h *authZHandlers) isRootUser(principal *models.Principal) bool {
	for _, groupName := range principal.Groups {
		if slices.Contains(h.rbacconfig.RootGroups, groupName) {
			return true
		}
	}
	return slices.Contains(h.rbacconfig.RootUsers, principal.Username)
}

func (h *authZHandlers) getUserTypesAndValidateExistence(id string, userTypeParam models.UserTypeInput) ([]models.UserTypeInput, error) {
	if userTypeParam == "" {
		exists, err := h.userExistsDeprecated(id)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}

		return []models.UserTypeInput{models.UserTypeInputOidc, models.UserTypeInputDb}, nil
	} else {
		exists, err := h.userExists(id, userTypeParam)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}

		return []models.UserTypeInput{userTypeParam}, nil
	}
}

// validateRootRole validates that enduser do not touch the internal root role
func validateRootRole(name string) error {
	if name == authorization.Root {
		return fmt.Errorf("modifying 'root' role or changing its assignments is not allowed")
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
	sort.Slice(roles, func(i, j int) bool {
		return *roles[i].Name < *roles[j].Name
	})
}

func validateUserTypeInput(userTypeInput string) (models.UserTypeInput, error) {
	var userType models.UserTypeInput
	if userTypeInput == string(models.UserTypeInputOidc) {
		userType = models.UserTypeInputOidc
	} else if userTypeInput == string(models.UserTypeInputDb) {
		userType = models.UserTypeInputDb
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
