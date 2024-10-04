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

	// policy handlers
	api.AuthzAddPolicyHandler = authz.AddPolicyHandlerFunc(h.addPolicy)
	api.AuthzGetPoliciesHandler = authz.GetPoliciesHandlerFunc(h.getPolicies)
	api.AuthzDeletePolicyHandler = authz.DeletePolicyHandlerFunc(h.deletePolicy)

	// roles handlers
	api.AuthzGetRolesForUsersHandler = authz.GetRolesForUsersHandlerFunc(h.getRolesForUsers)
	api.AuthzGetUsersForRoleHandler = authz.GetUsersForRoleHandlerFunc(h.getUsersForRoles)
	api.AuthzAddRoleHandler = authz.AddRoleHandlerFunc(h.addRole)
	api.AuthzRemoveRoleHandler = authz.RemoveRoleHandlerFunc(h.removeRole)
}

func (h *authZHandlers) addPolicy(params authz.AddPolicyParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to add policy
	h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	if _, err := h.enforcer.AddPolicy(params.Body.Role, params.Body.Object, params.Body.Action); err != nil {
		return authz.NewAddPolicyInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewAddPolicyInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	// TODO: make configurable
	if err := h.enforcer.InvalidateCache(); err != nil {
		return authz.NewAddPolicyInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewAddPolicyCreated()
}

func (h *authZHandlers) getPolicies(params authz.GetPoliciesParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to get policy
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	policies, err := h.enforcer.GetPolicy()
	if err != nil {
		return authz.NewGetPoliciesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	res := models.PoliciesListResponse{}
	for _, policy := range policies {
		res = append(res, &models.Policy{
			Action: &policy[2],
			Object: &policy[1],
			Role:   &policy[0],
		})
	}

	return authz.NewGetPoliciesOK().WithPayload(res)
}

func (h *authZHandlers) deletePolicy(params authz.DeletePolicyParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to delete policy
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	if _, err := h.enforcer.RemovePolicy(params.ID); err != nil {
		return authz.NewDeletePolicyInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewDeletePolicyInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	// TODO: make configurable
	if err := h.enforcer.InvalidateCache(); err != nil {
		return authz.NewDeletePolicyInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewDeletePolicyNoContent()
}

func (h *authZHandlers) addRole(params authz.AddRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to add Role
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	roleUser := params.Body.User
	if roleUser == nil {
		roleUser = params.Body.Key
	}

	if _, err := h.enforcer.AddRoleForUser(*roleUser, *params.Body.Role); err != nil {
		return authz.NewAddRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewAddRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewAddRoleOK()
}

func (h *authZHandlers) getRolesForUsers(params authz.GetRolesForUsersParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to list roles for users
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	roles, err := h.enforcer.GetUsersForRole(params.Body.Role)
	if err != nil {
		return authz.NewGetRolesForUsersInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetRolesForUsersOK().WithPayload(roles)
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
		return authz.NewGetRolesForUsersInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetRolesForUsersOK().WithPayload(users)
}

func (h *authZHandlers) removeRole(params authz.RemoveRoleParams, principal *models.Principal) middleware.Responder {
	// TODO is the logged user is admin ?
	// TODO validate
	// TODO based on the logged in user from the principal we check if that user is allowed to remove role for users
	// h.authorizer.Authorize(principal, *params.Body.Action, *params.Body.Object)
	roleUser := params.Body.User
	if roleUser == nil {
		roleUser = params.Body.Key
	}
	if _, err := h.enforcer.DeleteRoleForUser(*roleUser, *params.Body.Role); err != nil {
		return authz.NewRemoveRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.SavePolicy(); err != nil {
		return authz.NewRemoveRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewRemoveRoleOK()
}
