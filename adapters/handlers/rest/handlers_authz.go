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
	api.AuthzUpdateRoleHandler = authz.UpdateRoleHandlerFunc(h.updateRole)
	api.AuthzGetRolesHandler = authz.GetRolesHandlerFunc(h.getRoles)
	api.AuthzDeleteRoleHandler = authz.DeleteRoleHandlerFunc(h.deleteRole)

	// rbac users handlers
	api.AuthzGetUserRolesOrRoleUsersHandler = authz.GetUserRolesOrRoleUsersHandlerFunc(h.getUserRolesOrRoleUsers)
	api.AuthzAssignRoleHandler = authz.AssignRoleHandlerFunc(h.assignRole)
	api.AuthzRevokeRoleHandler = authz.RevokeRoleHandlerFunc(h.revokeRole)
}

func (h *authZHandlers) createRole(params authz.CreateRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to add policy

	// params.Body.Level
	// params.Body.Permissions
	// TODO: validate level matches permissions
	if err := h.authorizer.Authorize(principal, string(authorization.CreateRole), string(authorization.DatabaseL)); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	rules := [][]string{}
	for _, permission := range params.Body.Permissions {
		if len(permission.Resources) == 0 { // no filters
			for _, action := range permission.Actions {
				for _, verb := range authorization.Verbs(authorization.Actions[action]) {
					rules = append(rules, []string{*params.Body.Name, "*", verb})
				}
			}
		} else {
			for _, resource := range permission.Resources { // with filtering
				for _, action := range permission.Actions {
					for _, verb := range authorization.Verbs(authorization.Actions[action]) {
						rules = append(rules, []string{*params.Body.Name, *resource, verb}) // TODO: add filter to specific resource
					}
				}
			}
		}
	}

	if _, err := h.enforcer.AddPolicy(rules); err != nil {
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
	if err := h.authorizer.Authorize(principal, string(authorization.ReadRole), string(authorization.DatabaseL)); err != nil {
		return authz.NewUpdateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	policies, err := h.enforcer.GetPolicy()
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	res := models.RolesListResponse{}
	for _, policy := range policies {
		res = append(res, &models.Role{
			// Action: &policy[2], // TODO permissions
			Level: &policy[1],
			Name:  &policy[0],
		})
	}

	return authz.NewGetRolesOK().WithPayload(res)
}
func (h *authZHandlers) updateRole(params authz.UpdateRoleParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, string(authorization.UpdateRole), string(authorization.DatabaseL)); err != nil {
		return authz.NewUpdateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	return authz.NewUpdateRoleCreated()
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to delete policy
	if err := h.authorizer.Authorize(principal, string(authorization.DeleteRole), string(authorization.DatabaseL)); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
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
	if err := h.authorizer.Authorize(principal, string(authorization.ManageRole), string(authorization.DatabaseL)); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	roleUser := params.Body.User
	for _, role := range params.Body.Roles {
		if _, err := h.enforcer.AddRoleForUser(*roleUser, role); err != nil {
			return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewAssignRoleOK()
}

func (h *authZHandlers) getUserRolesOrRoleUsers(params authz.GetUserRolesOrRoleUsersParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate

	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to get policy

	if err := h.authorizer.Authorize(principal, string(authorization.ReadRole), string(authorization.DatabaseL)); err != nil {
		return authz.NewGetUserRolesOrRoleUsersInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	res := &authz.GetUserRolesOrRoleUsersOKBody{}
	users, err := h.enforcer.GetUsersForRole(*params.Role)
	if err != nil {
		return authz.NewGetUserRolesOrRoleUsersInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	res.Users = users

	roles, err := h.enforcer.GetRolesForUser(*params.User)
	if err != nil {
		return authz.NewGetUserRolesOrRoleUsersInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	res.Roles = roles

	return authz.NewGetUserRolesOrRoleUsersOK().WithPayload(res)
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to remove role for users
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	if err := h.authorizer.Authorize(principal, string(authorization.ManageRole), string(authorization.DatabaseL)); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	roleUser := params.Body.User
	for _, role := range params.Body.Roles {
		if _, err := h.enforcer.DeleteRoleForUser(*roleUser, role); err != nil {
			return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewRevokeRoleOK()
}
