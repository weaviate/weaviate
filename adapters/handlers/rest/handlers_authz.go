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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type authZHandlers struct {
	authorizer authorization.Authorizer
	enforcer   *rbac.Enforcer
	logger     logrus.FieldLogger
	metrics    *monitoring.PrometheusMetrics
}

func setupAuthZHandlers(api *operations.WeaviateAPI, enforcer *rbac.Enforcer, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger) {
	h := &authZHandlers{enforcer: enforcer, authorizer: authorizer, logger: logger, metrics: metrics}

	// rbac role handlers
	api.AuthzCreateRoleHandler = authz.CreateRoleHandlerFunc(h.createRole)
	api.AuthzGetRolesHandler = authz.GetRolesHandlerFunc(h.getRoles)
	api.AuthzGetRoleHandler = authz.GetRoleHandlerFunc(h.getRole)
	api.AuthzDeleteRoleHandler = authz.DeleteRoleHandlerFunc(h.deleteRole)
	api.AuthzAddPermissionHandler = authz.AddPermissionHandlerFunc(h.addPermission)
	api.AuthzRemovedPermissionHandler = authz.RemovedPermissionHandlerFunc(h.removePermission)

	// rbac users handlers
	api.AuthzGetRolesForUserHandler = authz.GetRolesForUserHandlerFunc(h.getRolesForUser)
	api.AuthzGetUsersForRoleHandler = authz.GetUsersForRoleHandlerFunc(h.getUsersForRole)
	api.AuthzAssignRoleHandler = authz.AssignRoleHandlerFunc(h.assignRole)
	api.AuthzRevokeRoleHandler = authz.RevokeRoleHandlerFunc(h.revokeRole)
}

func (h *authZHandlers) createRole(params authz.CreateRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.ROLES); err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	policies := []*rbac.Policy{}
	for _, permission := range params.Body.Permissions {
		if !rbac.CheckLevel(permission.Level) {
			err := fmt.Errorf("permission level '%v' is not allowed", permission.Level)
			return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		level := *permission.Level

		if len(permission.Resources) == 0 { // no filters
			for _, action := range permission.Actions {
				for _, verb := range authorization.Verbs(authorization.ActionsByLevel[level][action]) {
					policies = append(policies, &rbac.Policy{Name: *params.Body.Name, Resource: "*", Verb: verb, Level: level})
				}
			}
		} else {
			for _, resource := range permission.Resources { // with filtering
				for _, action := range permission.Actions {
					for _, verb := range authorization.Verbs(authorization.ActionsByLevel[level][action]) {
						policies = append(policies, &rbac.Policy{Name: *params.Body.Name, Resource: *resource, Verb: verb, Level: level}) // TODO: add filter to specific resource
					}
				}
			}
		}
	}

	err := h.enforcer.AddPolicies(policies)
	if err != nil {
		return authz.NewCreateRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewCreateRoleCreated()
}

func (h *authZHandlers) addPermission(params authz.AddPermissionParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) removePermission(params authz.RemovedPermissionParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) rolesFromPolicies(policies []*rbac.Policy) []*models.Role {
	// TODO proper mapping between casbin and weaviate permissions
	// name, level, verb
	verbsByRole := make(map[string]map[string]map[string]struct{})
	resourcesByRole := make(map[string]map[string]map[string]struct{})
	for _, policy := range policies {
		if _, ok := verbsByRole[policy.Name]; !ok {
			verbsByRole[policy.Name] = map[string]map[string]struct{}{}
		}
		if _, ok := resourcesByRole[policy.Name]; !ok {
			resourcesByRole[policy.Name] = map[string]map[string]struct{}{}
		}
		if _, ok := verbsByRole[policy.Name][policy.Level]; !ok {
			verbsByRole[policy.Name][policy.Level] = map[string]struct{}{}
		}
		if _, ok := resourcesByRole[policy.Name][policy.Level]; !ok {
			resourcesByRole[policy.Name][policy.Level] = map[string]struct{}{}
		}

		verbsByRole[policy.Name][policy.Level][policy.Verb] = struct{}{}
		resourcesByRole[policy.Name][policy.Level][policy.Resource] = struct{}{}
	}

	out := make([]*models.Role, 0, len(verbsByRole))
	for name, verbs := range verbsByRole {
		for level := range verbs {
			allActions := authorization.AllActionsForLevel(level)

			// map verbs to actions
			actionToVerbs := map[string]map[string]struct{}{}
			for _, action := range allActions {
				for _, verb := range authorization.Verbs(authorization.ActionsByLevel[level][action]) {
					if _, ok := actionToVerbs[action]; !ok {
						actionToVerbs[action] = map[string]struct{}{}
					}

					actionToVerbs[action][verb] = struct{}{}
				}
			}

			for verb := range verbs[level] {
				for action, actionVerb := range actionToVerbs {
					if _, ok := actionVerb[verb]; ok {
						delete(actionToVerbs[action], verb)
					}
				}
			}
			var roleActions []string
			for action, actionVerb := range actionToVerbs {
				if len(actionVerb) == 0 {
					roleActions = append(roleActions, action)
				}
			}

			rs := make([]*string, 0, len(resourcesByRole[name][level]))
			for r := range resourcesByRole[name][level] {
				rs = append(rs, &r)
			}

			out = append(out, &models.Role{
				Name: &name,
				Permissions: []*models.Permission{{
					Actions:   roleActions,
					Resources: rs,
					Level:     &level,
				}},
			})

		}
	}

	return out
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.GET, authorization.ROLES); err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	policies, err := h.enforcer.GetPolicies(nil)
	if err != nil {
		return authz.NewGetRolesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	res := h.rolesFromPolicies(policies)

	return authz.NewGetRolesOK().WithPayload(res)
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.GET, authorization.ROLES); err != nil {
		return authz.NewGetRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	policies, err := h.enforcer.GetPolicies(&params.ID)
	if err != nil {
		return authz.NewGetRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	res := h.rolesFromPolicies(policies)
	if len(res) == 0 {
		return authz.NewGetRoleNotFound()
	}

	return authz.NewGetRoleOK().WithPayload(res[0])
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.ROLES); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	policies, err := h.enforcer.GetPolicies(&params.ID)
	if err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.RemovePolicies(policies); err != nil {
		return authz.NewDeleteRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewDeleteRoleNoContent()
}

func (h *authZHandlers) assignRole(params authz.AssignRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.USERS); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.AddRolesForUser(params.ID, params.Body.Roles); err != nil {
		return authz.NewAssignRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewAssignRoleOK()
}

func (h *authZHandlers) getRolesForUser(params authz.GetRolesForUserParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.GET, authorization.USERS); err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	roles, err := h.enforcer.GetRolesForUser(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetRolesForUserOK().WithPayload(roles)
}

func (h *authZHandlers) getUsersForRole(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.GET, authorization.USERS); err != nil {
		return authz.NewGetUsersForRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	users, err := h.enforcer.GetUsersForRole(params.ID)
	if err != nil {
		return authz.NewGetRolesForUserInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetRolesForUserOK().WithPayload(users)
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	// TODO validate and audit log
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.USERS); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	if err := h.enforcer.DeleteRolesForUser(params.ID, params.Body.Roles); err != nil {
		return authz.NewRevokeRoleInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewRevokeRoleOK()
}
