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
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type authZHandlers struct {
	authorizer     authorization.Authorizer
	controller     authorization.Controller
	schemaReader   schemaUC.SchemaGetter
	logger         logrus.FieldLogger
	metrics        *monitoring.PrometheusMetrics
	apiKeysConfigs config.APIKey
	oidcConfigs    config.OIDC
	rbacconfig     rbacconf.Config
}

func SetupHandlers(api *operations.WeaviateAPI, controller authorization.Controller, schemaReader schemaUC.SchemaGetter,
	apiKeysConfigs config.APIKey, oidcConfigs config.OIDC, rconfig rbacconf.Config, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	h := &authZHandlers{controller: controller, authorizer: authorizer, schemaReader: schemaReader, rbacconfig: rconfig, oidcConfigs: oidcConfigs, apiKeysConfigs: apiKeysConfigs, logger: logger, metrics: metrics}

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
	api.AuthzAssignRoleHandler = authz.AssignRoleHandlerFunc(h.assignRole)
	api.AuthzRevokeRoleHandler = authz.RevokeRoleHandlerFunc(h.revokeRole)
	api.AuthzGetRolesForOwnUserHandler = authz.GetRolesForOwnUserHandlerFunc(h.getRolesForOwnUser)
}

func (h *authZHandlers) createRole(params authz.CreateRoleParams, principal *models.Principal) middleware.Responder {
	if *params.Body.Name == "" {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("role name is required")))
	}

	policies, err := conv.RolesToPolicies(params.Body)
	if err != nil {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not create role with the same name as builtin role %s", *params.Body.Name)))
	}

	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.Roles(*params.Body.Name)...); err != nil {
		return authz.NewCreateRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles(*params.Body.Name)
	if err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if len(roles) > 0 {
		return authz.NewCreateRoleConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("role with name %s already exists", *params.Body.Name)))
	}

	if err = h.controller.UpsertRolesPermissions(policies); err != nil {
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
	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not update builtin role %s", params.ID)))
	}

	if err := validatePermissions(params.Body.Permissions...); err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	policies, err := conv.RolesToPolicies(&models.Role{
		Name:        &params.ID,
		Permissions: params.Body.Permissions,
	})
	if err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(params.ID)...); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles(params.ID)
	if err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if len(roles) == 0 { // i.e. new role
		return authz.NewAddPermissionsNotFound()
	}

	if err := h.controller.UpsertRolesPermissions(policies); err != nil {
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
	// we don't validate permissions entity existence
	// in case of the permissions gets removed after the entity got removed
	// delete class ABC, then remove permissions on class ABC
	if err := validatePermissions(params.Body.Permissions...); err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not update builtin role %s", params.ID)))
	}

	permissions, err := conv.PermissionToPolicies(params.Body.Permissions...)
	if err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(params.ID)...); err != nil {
		return authz.NewRemovePermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	role, err := h.controller.GetRoles(params.ID)
	if err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if len(role) == 0 {
		return authz.NewRemovePermissionsNotFound()
	}

	if err := h.controller.RemovePermissions(params.ID, permissions); err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
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
	if params.Body == nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("permission is required")))
	}

	if err := validatePermissions(params.Body); err != nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(params.ID)...); err != nil {
		return authz.NewHasPermissionForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	policy, err := conv.PermissionToPolicies(params.Body)
	if err != nil {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}
	if len(policy) == 0 {
		return authz.NewHasPermissionBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("unknown error occurred passing permission to policy")))
	}

	hasPermission, err := h.controller.HasPermission(params.ID, policy[0])
	if err != nil {
		return authz.NewHasPermissionInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	return authz.NewHasPermissionOK().WithPayload(hasPermission)
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles()...); err != nil {
		return authz.NewGetRolesForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles()
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	response := []*models.Role{}

	for roleName, policies := range roles {
		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
		response = append(response, &models.Role{
			Name:        &roleName,
			Permissions: perms,
		})
	}
	sortByName(response)

	h.logger.WithFields(logrus.Fields{
		"action":    "read_all_roles",
		"component": authorization.ComponentName,
		"user":      principal.Username,
	}).Info("roles requested")

	return authz.NewGetRolesOK().WithPayload(response)
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(params.ID)...); err != nil {
		return authz.NewGetRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles(params.ID)
	if err != nil {
		return authz.NewGetRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}
	if len(roles) == 0 {
		return authz.NewGetRoleNotFound()
	}
	if len(roles) != 1 {
		err := fmt.Errorf("expected one role but got %d", len(roles))
		return authz.NewGetRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	perms, err := conv.PoliciesToPermission(roles[params.ID]...)
	if err != nil {
		return authz.NewGetRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
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
	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewDeleteRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not delete builtin role %s", params.ID)))
	}

	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.Roles(params.ID)...); err != nil {
		return authz.NewDeleteRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.controller.DeleteRoles(params.ID); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":    "delete_role",
		"component": authorization.ComponentName,
		"user":      principal.Username,
		"roleName":  params.ID,
	}).Info("role deleted")

	return authz.NewDeleteRoleNoContent()
}

func (h *authZHandlers) assignRole(params authz.AssignRoleParams, principal *models.Principal) middleware.Responder {
	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewAssignRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to assign is empty")))
		}
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(params.Body.Roles...)...); err != nil {
		return authz.NewAssignRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if !h.userExists(params.ID) {
		return authz.NewAssignRoleNotFound()
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if len(existedRoles) != len(params.Body.Roles) && len(params.Body.Roles) == 1 {
		return authz.NewAssignRoleNotFound()
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewAssignRoleNotFound()
	}

	if err := h.controller.AddRolesForUser(params.ID, params.Body.Roles); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "assign_roles",
		"component":               authorization.ComponentName,
		"user":                    principal.Username,
		"user_to_assign_roles_to": params.ID,
		"roles":                   params.Body.Roles,
	}).Info("roles assigned")

	return authz.NewAssignRoleOK()
}

func (h *authZHandlers) getRolesForUser(params authz.GetRolesForUserParams, principal *models.Principal) middleware.Responder {
	if !h.userExists(params.ID) {
		return authz.NewGetRolesForUserNotFound()
	}

	existedRoles, err := h.controller.GetRolesForUser(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	response := []*models.Role{}

	var authErr error
	for roleName, policies := range existedRoles {
		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesForUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}

		if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(roleName)...); err != nil {
			authErr = err
			continue
		}

		response = append(response, &models.Role{
			Name:        &roleName,
			Permissions: perms,
		})
	}

	if len(existedRoles) != 0 && len(response) == 0 {
		return authz.NewGetRolesForUserForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(authErr))
	}

	sortByName(response)

	h.logger.WithFields(logrus.Fields{
		"action":                "get_roles_for_user",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"user_to_get_roles_for": params.ID,
	}).Info("roles requested")

	return authz.NewGetRolesForUserOK().WithPayload(response)
}

func (h *authZHandlers) getRolesForOwnUser(params authz.GetRolesForOwnUserParams, principal *models.Principal) middleware.Responder {
	if principal == nil {
		return authz.NewGetRolesForOwnUserUnauthorized()
	}

	existedRoles, err := h.controller.GetRolesForUser(principal.Username)
	if err != nil {
		return authz.NewGetRolesForOwnUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	var response []*models.Role

	for roleName, policies := range existedRoles {
		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesForOwnUserInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}

		response = append(response, &models.Role{
			Name:        &roleName,
			Permissions: perms,
		})
	}

	sortByName(response)

	h.logger.WithFields(logrus.Fields{
		"action":    "get_roles_for_own_user",
		"component": authorization.ComponentName,
		"user":      principal.Username,
	}).Info("roles requested")

	return authz.NewGetRolesForOwnUserOK().WithPayload(response)
}

func (h *authZHandlers) getUsersForRole(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(params.ID)...); err != nil {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	users, err := h.controller.GetUsersForRole(params.ID)
	if err != nil {
		return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	slices.Sort(users)

	h.logger.WithFields(logrus.Fields{
		"action":                "get_users_for_role",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"role_to_get_users_for": params.ID,
	}).Info("users requested")

	return authz.NewGetUsersForRoleOK().WithPayload(users)
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	for _, role := range params.Body.Roles {
		if strings.TrimSpace(role) == "" {
			return authz.NewRevokeRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to revoke is empty")))
		}
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(params.Body.Roles...)...); err != nil {
		return authz.NewRevokeRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	adminRoles := slices.Contains(h.rbacconfig.Admins, params.ID) && slices.Contains(params.Body.Roles, authorization.Admin)
	viewerRoles := slices.Contains(h.rbacconfig.Viewers, params.ID) && slices.Contains(params.Body.Roles, authorization.Viewer)
	if adminRoles || viewerRoles {
		requestedRole := authorization.Admin
		if viewerRoles {
			requestedRole = authorization.Viewer
		}
		return authz.NewRevokeRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not revoke configured role %s", requestedRole)))
	}

	if !h.userExists(params.ID) {
		return authz.NewRevokeRoleNotFound()
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewRevokeRoleNotFound()
	}

	if err := h.controller.RevokeRolesForUser(params.ID, params.Body.Roles...); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "revoke_roles",
		"component":               authorization.ComponentName,
		"user":                    principal.Username,
		"user_to_assign_roles_to": params.ID,
		"roles":                   params.Body.Roles,
	}).Info("roles revoked")

	return authz.NewRevokeRoleOK()
}

func (h *authZHandlers) userExists(user string) bool {
	// We are only able to check if a user is present on the system if APIKeys are the only auth method. For OIDC
	// users are managed in an external service and there is no general way to check if a user we have not seen yet is
	// valid.
	if h.oidcConfigs.Enabled {
		return true
	}

	if h.apiKeysConfigs.Enabled {
		for _, apiKey := range h.apiKeysConfigs.Users {
			if apiKey == user {
				return true
			}
		}
		return false
	}
	return true
}

func sortByName(roles []*models.Role) {
	sort.Slice(roles, func(i, j int) bool {
		return *roles[i].Name < *roles[j].Name
	})
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
