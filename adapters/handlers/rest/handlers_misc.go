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
	"net/url"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/meta"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/well_known"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type schemaManager interface {
	GetSchema(principal *models.Principal) (schema.Schema, error)
	GetSchemaSkipAuth() schema.Schema
}

func setupMiscHandlers(api *operations.WeaviateAPI, serverConfig *config.WeaviateConfig,
	schemaManager schemaManager, modulesProvider ModulesProvider, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger,
) {
	metricRequestsTotal := newMiscRequestsTotal(metrics, logger)
	api.MetaMetaGetHandler = meta.MetaGetHandlerFunc(func(params meta.MetaGetParams, principal *models.Principal) middleware.Responder {
		var (
			metaInfos = map[string]interface{}{}
			err       error
		)

		if modulesProvider != nil {
			metaInfos, err = modulesProvider.GetMeta()
			if err != nil {
				metricRequestsTotal.logError("", err)
				return meta.NewMetaGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
			}
		}

		res := &models.Meta{
			Hostname: serverConfig.GetHostAddress(),
			Version:  config.ServerVersion,
			Modules:  metaInfos,
		}
		metricRequestsTotal.logOk("")
		return meta.NewMetaGetOK().WithPayload(res)
	})

	api.WellKnownGetWellKnownOpenidConfigurationHandler = well_known.GetWellKnownOpenidConfigurationHandlerFunc(
		func(params well_known.GetWellKnownOpenidConfigurationParams, principal *models.Principal) middleware.Responder {
			if !serverConfig.Config.Authentication.OIDC.Enabled {
				metricRequestsTotal.logUserError("")
				return well_known.NewGetWellKnownOpenidConfigurationNotFound()
			}

			target, err := url.JoinPath(serverConfig.Config.Authentication.OIDC.Issuer, "/.well-known/openid-configuration")
			if err != nil {
				metricRequestsTotal.logError("", err)
				return well_known.NewGetWellKnownOpenidConfigurationInternalServerError().WithPayload(errPayloadFromSingleErr(err))
			}
			clientID := serverConfig.Config.Authentication.OIDC.ClientID
			scopes := serverConfig.Config.Authentication.OIDC.Scopes
			body := &well_known.GetWellKnownOpenidConfigurationOKBody{
				Href:     target,
				ClientID: clientID,
				Scopes:   scopes,
			}

			metricRequestsTotal.logOk("")
			return well_known.NewGetWellKnownOpenidConfigurationOK().WithPayload(body)
		})

	api.WeaviateRootHandler = operations.WeaviateRootHandlerFunc(
		func(params operations.WeaviateRootParams, principal *models.Principal) middleware.Responder {
			origin := serverConfig.Config.Origin
			body := &operations.WeaviateRootOKBody{
				Links: []*models.Link{
					{
						Name: "Meta information about this instance/cluster",
						Href: fmt.Sprintf("%s/v1/meta", origin),
					},
					{
						Name:              "view complete schema",
						Href:              fmt.Sprintf("%s/v1/schema", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/api/rest/schema",
					},
					{
						Name:              "CRUD schema",
						Href:              fmt.Sprintf("%s/v1/schema{/:className}", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/api/rest/schema",
					},
					{
						Name:              "CRUD objects",
						Href:              fmt.Sprintf("%s/v1/objects{/:id}", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/api/rest/objects",
					},
					{
						Name:              "trigger and view status of classifications",
						Href:              fmt.Sprintf("%s/v1/classifications{/:id}", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/api/rest/classification,https://weaviate.io/developers/weaviate/api/rest/classification#knn-classification",
					},
					{
						Name:              "check if Weaviate is live (returns 200 on GET when live)",
						Href:              fmt.Sprintf("%s/v1/.well-known/live", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/api/rest/well-known#liveness",
					},
					{
						Name:              "check if Weaviate is ready (returns 200 on GET when ready)",
						Href:              fmt.Sprintf("%s/v1/.well-known/ready", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/api/rest/well-known#readiness",
					},
					{
						Name:              "view link to openid configuration (returns 404 on GET if no openid is configured)",
						Href:              fmt.Sprintf("%s/v1/.well-known/openid-configuration", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/api/rest/well-known#openid-configuration",
					},
				},
			}

			metricRequestsTotal.logOk("")
			return operations.NewWeaviateRootOK().WithPayload(body)
		})
}

type miscRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newMiscRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &miscRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "misc", logger},
	}
}

func (e *miscRequestsTotal) logError(className string, err error) {
	switch err.(type) {
	default:
		e.logServerError(className, err)
	}
}
