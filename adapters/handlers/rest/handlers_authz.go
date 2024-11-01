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

	"github.com/casbin/casbin/v2"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type authZHandlers struct {
	authorizer authorization.Authorizer
	enforcer   *casbin.SyncedCachedEnforcer
	logger     logrus.FieldLogger
	metrics    *monitoring.PrometheusMetrics
}

func setupAuthZHandlers(api *operations.WeaviateAPI, enforcer *casbin.SyncedCachedEnforcer, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger) {
	h := &authZHandlers{enforcer: enforcer, authorizer: authorizer, logger: logger, metrics: metrics}

	// role handlers
	api.AuthzCreateRoleHandler = authz.CreateRoleHandlerFunc(h.createRole)
	api.AuthzGetRolesHandler = authz.GetRolesHandlerFunc(h.getRoles)
	api.AuthzDeleteRoleHandler = authz.DeleteRoleHandlerFunc(h.deleteRole)

	// rbac users handlers
	api.AuthzGetUsersForRoleHandler = authz.GetUsersForRoleHandlerFunc(h.getUsersForRoles)
	api.AuthzAssignRoleHandler = authz.AssignRoleHandlerFunc(h.assignRole)
	api.AuthzRevokeRoleHandler = authz.RevokeRoleHandlerFunc(h.revokeRole)
}

func (h *authZHandlers) createRole(params authz.CreateRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to add policy
	if err := h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Level); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if _, err := h.enforcer.AddPolicy(params.Body.Name, fmt.Sprintf("%s/%s", params.Body.Level, params.Body.ObjectName), params.Body.Action); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	// TODO: make configurable
	if err := h.enforcer.InvalidateCache(); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewCreateRoleCreated()
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to get policy
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	policies, err := h.enforcer.GetPolicy()
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	res := models.RolesListResponse{}
	for _, policy := range policies {
		res = append(res, &models.Role{
			Action: &policy[2],
			Level:  &policy[1],
			Name:   &policy[0],
		})
	}

	return authz.NewGetRolesOK().WithPayload(res)
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to delete policy
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	if _, err := h.enforcer.RemovePolicy(params.ID); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	// TODO: make configurable
	if err := h.enforcer.InvalidateCache(); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewDeleteRoleNoContent()
}

func (h *authZHandlers) assignRole(params authz.AssignRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to add Role
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	roleUser := params.Body.User
	if roleUser == nil {
		roleUser = params.Body.Key
	}

	if _, err := h.enforcer.AddRoleForUser(*roleUser, *params.Body.Role); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewAssignRoleOK()
}

func (h *authZHandlers) getUsersForRoles(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	roleUser := params.Body.User
	if roleUser == nil {
		roleUser = params.Body.Key
	}

	users, err := h.enforcer.GetUsersForRole(*roleUser)
	if err != nil {
		return authz.NewGetUsersForRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetUsersForRoleOK().WithPayload(users)
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to remove role for users
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	roleUser := params.Body.User
	if roleUser == nil {
		roleUser = params.Body.Key
	}
	if _, err := h.enforcer.DeleteRoleForUser(*roleUser, *params.Body.Role); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewRevokeRoleOK()
}
