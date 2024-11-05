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
	logger     logrus.FieldLogger
	metrics    *monitoring.PrometheusMetrics
}

func setupAuthZHandlers(api *operations.WeaviateAPI, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger) {
	h := &authZHandlers{authorizer: authorizer, logger: logger, metrics: metrics}

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
	panic("not implemented")
}

func (h *authZHandlers) addPermission(params authz.AddPermissionParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) removePermission(params authz.RemovedPermissionParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) getRoles(params authz.GetRolesParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) getRole(params authz.GetRoleParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) deleteRole(params authz.DeleteRoleParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) assignRole(params authz.AssignRoleParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) getRolesForUser(params authz.GetRolesForUserParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) getUsersForRole(params authz.GetUsersForRoleParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}

func (h *authZHandlers) revokeRole(params authz.RevokeRoleParams, principal *models.Principal) middleware.Responder {
	panic("not implemented")
}
