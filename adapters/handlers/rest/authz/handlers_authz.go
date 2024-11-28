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
	"fmt"
	"slices"

	"github.com/go-openapi/runtime/middleware"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type authZHandlers struct {
	authorizer           authorization.Authorizer
	controller           authorization.Controller
	schemaReader         schemaUC.SchemaGetter
	logger               logrus.FieldLogger
	metrics              *monitoring.PrometheusMetrics
	existingUsersApiKeys []string
}

func SetupHandlers(api *operations.WeaviateAPI, controller authorization.Controller, schemaReader schemaUC.SchemaGetter,
	existingUsersApiKeys []string, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger) {
	h := &authZHandlers{controller: controller, authorizer: authorizer, schemaReader: schemaReader, existingUsersApiKeys: existingUsersApiKeys, logger: logger, metrics: metrics}

	// rbac role handlers
	api.AuthzCreateRoleHandler = authz.CreateRoleHandlerFunc(h.createRole)
	api.AuthzGetRolesHandler = authz.GetRolesHandlerFunc(h.getRoles)
	api.AuthzGetRoleHandler = authz.GetRoleHandlerFunc(h.getRole)
	api.AuthzDeleteRoleHandler = authz.DeleteRoleHandlerFunc(h.deleteRole)
	api.AuthzAddPermissionsHandler = authz.AddPermissionsHandlerFunc(h.addPermissions)
	api.AuthzRemovePermissionsHandler = authz.RemovePermissionsHandlerFunc(h.removePermissions)

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

	if len(params.Body.Permissions) == 0 {
		return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("role has to have at least 1 permission")))
	}

	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.Roles(*params.Body.Name)...); err != nil {
		return authz.NewCreateRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	// if err := h.validatePermissions(params.Body.Permissions); err != nil {
	// 	return authz.NewCreateRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	// }

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
	if *params.Body.Name == "" {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("role name is required")))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not update builtin role %s", *params.Body.Name)))
	}

	if len(params.Body.Permissions) == 0 {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("role has to have at least 1 permission")))
	}

	policies, err := conv.RolesToPolicies(&models.Role{
		Name:        params.Body.Name,
		Permissions: params.Body.Permissions,
	})
	if err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(*params.Body.Name)...); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	// if err := h.validatePermissions(params.Body.Permissions); err != nil {
	// 	return authz.NewAddPermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	// }

	if err := h.controller.UpsertRolesPermissions(policies); err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "add_permissions",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    params.Body.Name,
		"permissions": params.Body.Permissions,
	}).Info("permissions added")

	return authz.NewAddPermissionsOK()
}

func (h *authZHandlers) removePermissions(params authz.RemovePermissionsParams, principal *models.Principal) middleware.Responder {
	if *params.Body.Name == "" {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("role name is required")))
	}

	// we don't validate permissions entity existence
	// in case of the permissions gets removed after the entity got removed
	// delete class ABC, then remove permissions on class ABC
	if len(params.Body.Permissions) == 0 {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(errors.New("role has to have at least 1 permission")))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("you can not update builtin role %s", *params.Body.Name)))
	}

	permissions, err := conv.PermissionToPolicies(params.Body.Permissions...)
	if err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(*params.Body.Name)...); err != nil {
		return authz.NewRemovePermissionsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.controller.RemovePermissions(*params.Body.Name, permissions); err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "remove_permissions",
		"component":   authorization.ComponentName,
		"user":        principal.Username,
		"roleName":    params.Body.Name,
		"permissions": params.Body.Permissions,
	}).Info("permissions removed")

	return authz.NewRemovePermissionsOK()
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

	h.logger.WithFields(logrus.Fields{
		"action":    "read_all_roles",
		"component": authorization.ComponentName,
		"user":      principal.Username,
	}).Info("roles requested")

	return authz.NewGetRolesOK().WithPayload(response)
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	if params.ID == "" {
		return authz.NewGetRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("role id can not be empty")))
	}

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
	if params.ID == "" {
		return authz.NewDeleteRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("role id can not be empty")))
	}

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
	if params.ID == "" {
		return authz.NewAssignRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user id can not be empty")))
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(params.Body.Roles...)...); err != nil {
		return authz.NewAssignRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewAssignRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to assign doesn't exists")))
	}

	// TODO check for users when we have dynamic users

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
	if params.ID == "" {
		return authz.NewGetRolesForUserBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user name is required")))
	}

	if !h.existsUser(params.ID) {
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

		// verify the user has access
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

	h.logger.WithFields(logrus.Fields{
		"action":    "get_roles_for_own_user",
		"component": authorization.ComponentName,
		"user":      principal.Username,
	}).Info("roles requested")

	return authz.NewGetRolesForOwnUserOK().WithPayload(response)
}

func (h *authZHandlers) getUsersForRole(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	if params.ID == "" {
		return authz.NewGetUsersForRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user name is required")))
	}

	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(params.ID)...); err != nil {
		return authz.NewGetUsersForRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	users, err := h.controller.GetUsersForRole(params.ID)
	if err != nil {
		return authz.NewGetUsersForRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                "get_users_for_role",
		"component":             authorization.ComponentName,
		"user":                  principal.Username,
		"role_to_get_users_for": params.ID,
	}).Info("users requested")

	return authz.NewGetUsersForRoleOK().WithPayload(users)
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	if params.ID == "" {
		return authz.NewRevokeRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("user id can not be empty")))
	}

	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(params.Body.Roles...)...); err != nil {
		return authz.NewRevokeRoleForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewRevokeRoleBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to revoke doesn't exists")))
	}

	// TODO check for users when we have dynamic users

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

func (h *authZHandlers) existsUser(user string) bool {
	if h.existingUsersApiKeys != nil {
		for _, apiKey := range h.existingUsersApiKeys {
			if apiKey == user {
				return true
			}
		}
		return false
	}
	return true // dont block OICD for now
}

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
