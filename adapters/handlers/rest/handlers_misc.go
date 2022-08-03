//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	"encoding/json"
	"fmt"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/meta"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/well_known"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
)

type schemaManager interface {
	GetSchema(principal *models.Principal) (schema.Schema, error)
	GetSchemaSkipAuth() schema.Schema
}

type swaggerJSON struct {
	Info struct {
		Version string `json:"version"`
	} `json:"info"`
}

func setupMiscHandlers(api *operations.WeaviateAPI, serverConfig *config.WeaviateConfig,
	schemaManager schemaManager, modulesProvider ModulesProvider) {
	var swj swaggerJSON
	err := json.Unmarshal(SwaggerJSON, &swj)
	if err != nil {
		panic(err)
	}

	// this is a good time for us to set ServerVersion,
	// so that the spec only needs to be parsed once.
	config.ServerVersion = swj.Info.Version

	api.MetaMetaGetHandler = meta.MetaGetHandlerFunc(func(params meta.MetaGetParams, principal *models.Principal) middleware.Responder {
		metaInfos := map[string]interface{}{}

		if modulesProvider != nil {
			metaInfos, err = modulesProvider.GetMeta()
			if err != nil {
				return meta.NewMetaGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
			}
		}

		res := &models.Meta{
			Hostname: serverConfig.GetHostAddress(),
			Version:  swj.Info.Version,
			Modules:  metaInfos,
		}
		return meta.NewMetaGetOK().WithPayload(res)
	})

	api.WellKnownGetWellKnownOpenidConfigurationHandler = well_known.GetWellKnownOpenidConfigurationHandlerFunc(
		func(params well_known.GetWellKnownOpenidConfigurationParams, principal *models.Principal) middleware.Responder {
			if !serverConfig.Config.Authentication.OIDC.Enabled {
				return well_known.NewGetWellKnownOpenidConfigurationNotFound()
			}

			target := fmt.Sprintf("%s/.well-known/openid-configuration", serverConfig.Config.Authentication.OIDC.Issuer)
			clientID := serverConfig.Config.Authentication.OIDC.ClientID
			body := &well_known.GetWellKnownOpenidConfigurationOKBody{
				Href:     target,
				ClientID: clientID,
			}

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
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/restful-api-references/schema.html",
					},
					{
						Name:              "CRUD schema",
						Href:              fmt.Sprintf("%s/v1/schema{/:className}", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/restful-api-references/schema.html",
					},
					{
						Name:              "CRUD objects",
						Href:              fmt.Sprintf("%s/v1/objects{/:id}", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/restful-api-references/objects.html",
					},
					{
						Name:              "trigger and view status of classifications",
						Href:              fmt.Sprintf("%s/v1/classifications{/:id}", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/modules/text2vec-contextionary.html#contextual-classification,https://weaviate.io/developers/weaviate/current/restful-api-references/classification.html#knn-classification",
					},
					{
						Name:              "check if Weaviate is live (returns 200 on GET when live)",
						Href:              fmt.Sprintf("%s/v1/.well-known/live", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/restful-api-references/well-known.html#liveness",
					},
					{
						Name:              "check if Weaviate is ready (returns 200 on GET when ready)",
						Href:              fmt.Sprintf("%s/v1/.well-known/ready", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/restful-api-references/well-known.html#readiness",
					},
					{
						Name:              "view link to openid configuration (returns 404 on GET if no openid is configured)",
						Href:              fmt.Sprintf("%s/v1/.well-known/openid-configuration", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/restful-api-references/well-known.html#openid-configuration",
					},

					// TODO: part of the text2vec-contextionary module
					{
						Name:              "search contextionary for concepts (part of the text2vec-contextionary module)",
						Href:              fmt.Sprintf("%s/v1/modules/text2vec-contextionary/concepts/:concept", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/retriever-vectorizer-modules/text2vec-contextionary.html#module-endpoints-api-reference",
					},

					// TODO: part of the text2vec-contextionary module
					{
						Name:              "extend contextionary with custom extensions (part of the text2vec-contextionary module)",
						Href:              fmt.Sprintf("%s/v1/modules/text2vec-contextionary/extensions", origin),
						DocumentationHref: "https://weaviate.io/developers/weaviate/current/retriever-vectorizer-modules/text2vec-contextionary.html#module-endpoints-api-reference",
					},
				},
			}

			return operations.NewWeaviateRootOK().WithPayload(body)
		})
}
