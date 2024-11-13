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

	"github.com/go-openapi/runtime/middleware"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
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
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.Roles()...); err != nil {
		return authz.NewCreateRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if *params.Body.Name == "" {
		return authz.NewCreateRoleUnprocessableEntity().WithPayload(errPayloadFromSingleErr(errors.New("role name is required")))
	}

	err := h.controller.UpsertRolesPermissions(params.Body)
	if err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewCreateRoleCreated()
}

func (h *authZHandlers) addPermissions(params authz.AddPermissionsParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles()...); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	err := h.controller.UpsertRolesPermissions(&models.Role{
		Name:        params.Body.Name,
		Permissions: params.Body.Permissions,
	})
	if err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewAddPermissionsOK()
}

func (h *authZHandlers) removePermissions(params authz.RemovePermissionsParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.Roles()...); err != nil {
		return authz.NewAddPermissionsForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	err := h.controller.RemovePermissions(*params.Body.Name, params.Body.Permissions)
	if err != nil {
		return authz.NewAddPermissionsInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewRemovePermissionsOK()
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles()...); err != nil {
		return authz.NewGetRolesForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRoles()
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetRolesOK().WithPayload(roles)
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles()...); err != nil {
		return authz.NewGetRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
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

	return authz.NewGetRoleOK().WithPayload(roles[0])
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.Roles()...); err != nil {
		return authz.NewDeleteRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.controller.DeleteRoles(params.ID); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewDeleteRoleNoContent()
}

func (h *authZHandlers) assignRole(params authz.AssignRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.Roles()...); err != nil {
		return authz.NewAssignRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.controller.AddRolesForUser(params.ID, params.Body.Roles); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewAssignRoleOK()
}

func (h *authZHandlers) getRolesForUser(params authz.GetRolesForUserParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles()...); err != nil {
		return authz.NewGetRolesForUserForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	roles, err := h.controller.GetRolesForUser(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	return authz.NewGetRolesForUserOK().WithPayload(roles)
}

func (h *authZHandlers) getUsersForRole(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.Roles()...); err != nil {
		return authz.NewGetUsersForRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	users, err := h.controller.GetUsersForRole(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetUsersForRoleOK().WithPayload(users)
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.Roles()...); err != nil {
		return authz.NewRevokeRoleForbidden().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.controller.RevokeRolesForUser(params.ID, params.Body.Roles...); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewRevokeRoleOK()
}
