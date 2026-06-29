//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"errors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	uco "github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

type aliasesHandlers struct {
	manager             *schemaUC.Manager
	metricRequestsTotal restApiRequestsTotal
}

func (s *aliasesHandlers) getAliases(params schema.AliasesGetParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	className := ""
	if params.Class != nil {
		className = *params.Class
	}
	aliases, err := s.manager.GetAliases(ctx, principal, "", className)
	if err != nil {
		s.metricRequestsTotal.logError(className, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewAliasesGetForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrValidation):
			return schema.NewAliasesGetUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewAliasesGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	if principal != nil && principal.Namespace != "" && len(aliases) > 0 {
		stripped := make([]*models.Alias, len(aliases))
		for i, a := range aliases {
			stripped[i] = namespacing.StripAliasResponse(principal, a)
		}
		aliases = stripped
	}
	aliasesResponse := &models.AliasResponse{Aliases: aliases}

	s.metricRequestsTotal.logOk(className)
	return schema.NewAliasesGetOK().WithPayload(aliasesResponse)
}

func (s *aliasesHandlers) getAlias(params schema.AliasesGetAliasParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	alias, err := s.manager.GetAlias(ctx, principal, params.AliasName)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewAliasesGetAliasNotFound()
		}

		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewAliasesGetAliasForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrValidation):
			return schema.NewAliasesGetAliasUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewAliasesGetAliasInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk("")
	return schema.NewAliasesGetAliasOK().WithPayload(namespacing.StripAliasResponse(principal, alias))
}

func (s *aliasesHandlers) addAlias(params schema.AliasesCreateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	_, _, err := s.manager.AddAlias(ctx, principal, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.Body.Class, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewAliasesCreateForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrValidation):
			return schema.NewAliasesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewAliasesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.Body.Class)
	return schema.NewAliasesCreateOK().WithPayload(namespacing.StripAliasResponse(principal, params.Body))
}

func (s *aliasesHandlers) updateAlias(params schema.AliasesUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	alias, err := s.manager.UpdateAlias(ctx, principal, params.AliasName, params.Body.Class)
	if err != nil {
		s.metricRequestsTotal.logError(params.Body.Class, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewAliasesUpdateNotFound()
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewAliasesUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrValidation):
			return schema.NewAliasesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewAliasesUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.Body.Class)
	return schema.NewAliasesUpdateOK().WithPayload(namespacing.StripAliasResponse(principal, alias))
}

func (s *aliasesHandlers) deleteAlias(params schema.AliasesDeleteParams, principal *models.Principal) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	err := s.manager.DeleteAlias(ctx, principal, params.AliasName)
	if err != nil {
		s.metricRequestsTotal.logError(params.AliasName, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewAliasesDeleteNotFound()
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewAliasesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrValidation):
			return schema.NewAliasesDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewAliasesDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.AliasName)
	return schema.NewAliasesDeleteNoContent()
}

func setupAliasesHandlers(api *operations.WeaviateAPI,
	manager *schemaUC.Manager,
	metrics *monitoring.PrometheusMetrics,
	logger logrus.FieldLogger,
) {
	h := &aliasesHandlers{manager, newAliasesRequestsTotal(metrics, logger)}

	api.SchemaAliasesGetHandler = schema.AliasesGetHandlerFunc(h.getAliases)
	api.SchemaAliasesGetAliasHandler = schema.AliasesGetAliasHandlerFunc(h.getAlias)
	api.SchemaAliasesCreateHandler = schema.AliasesCreateHandlerFunc(h.addAlias)
	api.SchemaAliasesUpdateHandler = schema.AliasesUpdateHandlerFunc(h.updateAlias)
	api.SchemaAliasesDeleteHandler = schema.AliasesDeleteHandlerFunc(h.deleteAlias)
}

type aliasesRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newAliasesRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &aliasesRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "aliases", logger},
	}
}

func (e *aliasesRequestsTotal) logError(className string, err error) {
	switch {
	case errors.As(err, &authzerrors.Forbidden{}):
		e.logUserError(className)
	case errors.As(err, &uco.ErrMultiTenancy{}):
		e.logUserError(className)
	default:
		e.logUserError(className)
	}
}
