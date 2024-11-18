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

package rest

import (
	"fmt"
	"slices"

	"github.com/go-openapi/runtime/middleware"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type authZHandlers struct {
	authorizer authorization.Authorizer
	controller authorization.Controller
	logger     logrus.FieldLogger
	metrics    *monitoring.PrometheusMetrics
}

func setupAuthZHandlers(api *operations.WeaviateAPI, controller authorization.Controller, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger) {
	h := &authZHandlers{controller: controller, authorizer: authorizer, logger: logger, metrics: metrics}

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
}

func (h *authZHandlers) createRole(params authz.CreateRoleParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.Roles(*params.Body.Name)...); err != nil {
		return authz.NewCreateRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if *params.Body.Name == "" {
		return authz.NewCreateRoleBadRequest().WithPayload(errPayloadFromSingleErr(errors.New("role name is required")))
	}

	if len(params.Body.Permissions) == 0 {
		return authz.NewCreateRoleBadRequest().WithPayload(errPayloadFromSingleErr(errors.New("role has to have at least 1 permission")))
	}

	policies, err := conv.RolesToPolicies(params.Body)
	if err != nil {
		return authz.NewCreateRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewCreateRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("you can not create role with the same name as builtin role %s", *params.Body.Name)))
	}

	roles, err := h.controller.GetRoles(*params.Body.Name)
	if err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if len(roles) > 0 {
		return authz.NewCreateRoleConflict().WithPayload(errPayloadFromSingleErr(fmt.Errorf("role with name %s already exists", *params.Body.Name)))
	}

	if err = h.controller.UpsertRolesPermissions(policies); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "create_role",
		"user":        principal.Username,
		"roleName":    params.Body.Name,
		"permissions": params.Body.Permissions,
	}).Info("role created")

	return authz.NewCreateRoleCreated()
}

func (h *authZHandlers) addPermissions(params authz.AddPermissionsParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(*params.Body.Name)...); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if *params.Body.Name == "" {
		return authz.NewAddPermissionsBadRequest().WithPayload(errPayloadFromSingleErr(errors.New("role name is required")))
	}

	if len(params.Body.Permissions) == 0 {
		return authz.NewAddPermissionsBadRequest().WithPayload(errPayloadFromSingleErr(errors.New("role has to have at least 1 permission")))
	}

	policies, err := conv.RolesToPolicies(&models.Role{
		Name:        params.Body.Name,
		Permissions: params.Body.Permissions,
	})
	if err != nil {
		return authz.NewAddPermissionsBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewAddPermissionsBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("you can not update builtin role %s", *params.Body.Name)))
	}

	if err := h.controller.UpsertRolesPermissions(policies); err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "add_permissions",
		"user":        principal.Username,
		"roleName":    params.Body.Name,
		"permissions": params.Body.Permissions,
	}).Info("permissions added")

	return authz.NewAddPermissionsOK()
}

func (h *authZHandlers) removePermissions(params authz.RemovePermissionsParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles(*params.Body.Name)...); err != nil {
		return authz.NewRemovePermissionsForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if *params.Body.Name == "" {
		return authz.NewRemovePermissionsBadRequest().WithPayload(errPayloadFromSingleErr(errors.New("role name is required")))
	}

	if len(params.Body.Permissions) == 0 {
		return authz.NewRemovePermissionsBadRequest().WithPayload(errPayloadFromSingleErr(errors.New("role has to have at least 1 permission")))
	}

	if slices.Contains(authorization.BuiltInRoles, *params.Body.Name) {
		return authz.NewRemovePermissionsBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("you can not update builtin role %s", *params.Body.Name)))
	}

	permissions, err := conv.PermissionToPolicies(params.Body.Permissions...)
	if err != nil {
		return authz.NewRemovePermissionsBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("invalid permission %s", err.Error())))
	}

	if err := h.controller.RemovePermissions(*params.Body.Name, permissions); err != nil {
		return authz.NewRemovePermissionsInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      "remove_permissions",
		"user":        principal.Username,
		"roleName":    params.Body.Name,
		"permissions": params.Body.Permissions,
	}).Info("permissions removed")

	return authz.NewRemovePermissionsOK()
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles()...); err != nil {
		return authz.NewGetRolesForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles()
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	response := []*models.Role{}

	for roleName, policies := range roles {
		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		response = append(response, &models.Role{
			Name:        &roleName,
			Permissions: perms,
		})
	}

	h.logger.WithFields(logrus.Fields{
		"action": "read_all_roles",
		"user":   principal.Username,
	}).Info("roles requested")

	return authz.NewGetRolesOK().WithPayload(response)
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(params.ID)...); err != nil {
		return authz.NewGetRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if params.ID == "" {
		return authz.NewGetRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("role id can not be empty")))
	}

	roles, err := h.controller.GetRoles(params.ID)
	if err != nil {
		return authz.NewGetRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	if len(roles) == 0 {
		return authz.NewGetRoleNotFound()
	}
	if len(roles) != 1 {
		err := fmt.Errorf("expected one role but got %d", len(roles))
		return authz.NewGetRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	perms, err := conv.PoliciesToPermission(roles[params.ID]...)
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":  "read_role",
		"user":    principal.Username,
		"role_id": params.ID,
	}).Info("role requested")

	return authz.NewGetRoleOK().WithPayload(&models.Role{
		Name:        &params.ID,
		Permissions: perms,
	})
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.Roles(params.ID)...); err != nil {
		return authz.NewDeleteRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if params.ID == "" {
		return authz.NewDeleteRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("role id can not be empty")))
	}

	if slices.Contains(authorization.BuiltInRoles, params.ID) {
		return authz.NewDeleteRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("you can not delete builtin role %s", params.ID)))
	}

	if err := h.controller.DeleteRoles(params.ID); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":   "delete_role",
		"user":     principal.Username,
		"roleName": params.ID,
	}).Info("role deleted")

	return authz.NewDeleteRoleNoContent()
}

func (h *authZHandlers) assignRole(params authz.AssignRoleParams, principal *models.Principal) middleware.Responder {
	multiResources := slices.Concat(authorization.Roles(params.Body.Roles...), authorization.Users(params.ID))
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, multiResources...); err != nil {
		return authz.NewAssignRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if params.ID == "" {
		return authz.NewAssignRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("user id can not be empty")))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewAssignRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to assign doesn't exists")))
	}

	// TODO check for users when we have dynamic users

	if err := h.controller.AddRolesForUser(params.ID, params.Body.Roles); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "assign_roles",
		"user":                    principal.Username,
		"user_to_assign_roles_to": params.ID,
		"roles":                   params.Body.Roles,
	}).Info("roles assigned")

	return authz.NewAssignRoleOK()
}

func (h *authZHandlers) getRolesForUser(params authz.GetRolesForUserParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(params.ID)...); err != nil {
		return authz.NewGetRolesForUserForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRolesForUser(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	response := []*models.Role{}

	for roleName, policies := range roles {
		perms, err := conv.PoliciesToPermission(policies...)
		if err != nil {
			return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		response = append(response, &models.Role{
			Name:        &roleName,
			Permissions: perms,
		})
	}

	h.logger.WithFields(logrus.Fields{
		"action":                "get_roles_for_user",
		"user":                  principal.Username,
		"user_to_get_roles_for": params.ID,
	}).Info("roles requested")

	return authz.NewGetRolesForUserOK().WithPayload(response)
}

func (h *authZHandlers) getUsersForRole(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles(params.ID)...); err != nil {
		return authz.NewGetUsersForRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	users, err := h.controller.GetUsersForRole(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                "get_users_for_role",
		"user":                  principal.Username,
		"role_to_get_users_for": params.ID,
	}).Info("users requested")

	return authz.NewGetUsersForRoleOK().WithPayload(users)
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	multiResources := slices.Concat(authorization.Roles(params.Body.Roles...), authorization.Users(params.ID))
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, multiResources...); err != nil {
		return authz.NewRevokeRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if params.ID == "" {
		return authz.NewRevokeRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("user id can not be empty")))
	}

	existedRoles, err := h.controller.GetRoles(params.Body.Roles...)
	if err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if len(existedRoles) != len(params.Body.Roles) {
		return authz.NewRevokeRoleBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf("one or more of the roles you want to revoke doesn't exists")))
	}

	// TODO check for users when we have dynamic users

	if err := h.controller.RevokeRolesForUser(params.ID, params.Body.Roles...); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":                  "revoke_roles",
		"user":                    principal.Username,
		"user_to_assign_roles_to": params.ID,
		"roles":                   params.Body.Roles,
	}).Info("roles revoked")

	return authz.NewRevokeRoleOK()
}
