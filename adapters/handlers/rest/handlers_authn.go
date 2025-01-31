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

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzConv "github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

type authNHandlers struct {
	authzController authorization.Controller
	logger          logrus.FieldLogger
}

func setupAuthnHandlers(api *operations.WeaviateAPI, controller authorization.Controller, logger logrus.FieldLogger,
) {
	h := &authNHandlers{authzController: controller, logger: logger}

	// user handlers
	api.UsersGetOwnInfoHandler = users.GetOwnInfoHandlerFunc(h.getOwnInfo)
}

func (h *authNHandlers) getOwnInfo(_ users.GetOwnInfoParams, principal *models.Principal) middleware.Responder {
	existingRoles, err := h.authzController.GetRolesForUser(principal.Username)
	if err != nil {
		return users.NewGetOwnInfoInternalServerError()
	}
	var roles []*models.Role

	for roleName, policies := range existingRoles {
		perms, err := authzConv.PoliciesToPermission(policies...)
		if err != nil {
			return users.NewGetOwnInfoInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
		roles = append(roles, &models.Role{
			Name:        &roleName,
			Permissions: perms,
		})
	}

	h.logger.WithFields(logrus.Fields{
		"action":    "get_own_info",
		"component": "authN",
		"user":      principal.Username,
	}).Info("own info requested")

	return users.NewGetOwnInfoOK().WithPayload(&models.UserInfo{
		Groups:   principal.Groups,
		Roles:    roles,
		Username: &principal.Username,
	})
}
