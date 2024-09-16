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
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type authZHandlers struct {
	enforcer *casbin.Enforcer
	logger   logrus.FieldLogger
	metrics  *monitoring.PrometheusMetrics
}

func setupAuthZHandlers(api *operations.WeaviateAPI, enforcer *casbin.Enforcer, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger,
) {

	h := &authZHandlers{enforcer: enforcer, logger: logger, metrics: metrics}

	// policy handlers
	api.AuthzAddPolicyHandler = authz.AddPolicyHandlerFunc(h.addPolicy)
	api.AuthzGetPoliciesHandler = authz.GetPoliciesHandlerFunc(h.policies)
	api.AuthzDeletePolicyHandler = authz.DeletePolicyHandlerFunc(h.deletePolicy)

	// roles handlers
	api.AuthzGetRoleHandler = authz.GetRoleHandlerFunc(h.getRole)
	api.AuthzAddRoleHandler = authz.AddRoleHandlerFunc(h.addRole)
	api.AuthzRemoveRoleHandler = authz.RemoveRoleHandlerFunc(h.removeRole)
}

func (h *authZHandlers) addPolicy(params authz.AddPolicyParams, principal *models.Principal) middleware.Responder {
	added, err := h.enforcer.AddPolicy(params.Body.Role, params.Body.Object, params.Body.Action)
	if err != nil {
		return authz.NewAddPolicyInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}
	if !added {

	}
	return authz.NewAddPolicyCreated()
}

func (h *authZHandlers) policies(params authz.GetPoliciesParams, principal *models.Principal) middleware.Responder {
	_, err := h.enforcer.GetPolicy()
	if err != nil {
		return authz.NewGetPoliciesInternalServerError().WithPayload(errPayloadFromSingleErr(err))
	}

	return authz.NewGetPoliciesOK() // with payload
}

func (h *authZHandlers) deletePolicy(params authz.DeletePolicyParams, principal *models.Principal) middleware.Responder {
	h.enforcer.RemovePolicy(params.ID)
	return authz.NewDeletePolicyNoContent()
}

func (h *authZHandlers) addRole(params authz.AddRoleParams, principal *models.Principal) middleware.Responder {
	// TODO
	h.enforcer.AddRoleForUser(*params.Body.User, params.Body.Role)
	h.enforcer.AddRoleForUser(*params.Body.Key, params.Body.Role)

	return authz.NewAddRoleOK()
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	// TODO
	// get either user/key
	// validate
	// auth ?
	h.enforcer.GetRolesForUser(*params.Body.User)
	h.enforcer.GetRolesForUser(*params.Body.Key)
	h.enforcer.GetUsersForRole(params.Body.Role)

	return authz.NewAddRoleOK()
}

func (h *authZHandlers) removeRole(params authz.RemoveRoleParams, principal *models.Principal) middleware.Responder {
	// TODO
	// get either user/key
	// validate
	// auth ?
	h.enforcer.DeleteRoleForUser(*params.Body.Key, params.Body.Role)
	h.enforcer.DeleteRoleForUser(*params.Body.User, params.Body.Role)
	return authz.NewRemoveRoleOK()
}
