//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	"context"
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

type c11yMetaProvider interface {
	Version(ctx context.Context) (string, error)
	WordCount(ctx context.Context) (int64, error)
}

func setupMiscHandlers(api *operations.WeaviateAPI, serverConfig *config.WeaviateConfig,
	schemaManager schemaManager, modulesProvider ModulesProvider) {
	var swj swaggerJSON
	err := json.Unmarshal(SwaggerJSON, &swj)
	if err != nil {
		panic(err)
	}

	var c11y c11yMetaProvider
	if modulesProvider != nil {
		c11y = modulesProvider.GetMetaProvider()
	}

	api.MetaMetaGetHandler = meta.MetaGetHandlerFunc(func(params meta.MetaGetParams, principal *models.Principal) middleware.Responder {
		// TODO: gh-1494 make this modular

		modules := map[string]interface{}{}

		if c11y != nil {
			// this is a slightly hacky way to check if we are running with the c11y
			// module, since at startup we register the client only if the module is
			// enabled
			// Create response object
			c11yVersion, err := c11y.Version(context.Background())
			if err != nil {
				return meta.NewMetaGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
			}

			c11yWordCount, err := c11y.WordCount(context.Background())
			if err != nil {
				return meta.NewMetaGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
			}

			// TODO: gh-1494 When doing actual modularization, don't hard-code the module
			// value, but ask each module for the meta info they want to provide
			// dynamically
			modules["text2vec-contextionary"] = map[string]interface{}{
				"version":   c11yVersion,
				"wordCount": c11yWordCount,
			}
		}

		res := &models.Meta{
			Hostname: serverConfig.GetHostAddress(),
			Version:  swj.Info.Version,
			Modules:  modules,
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
					&models.Link{
						Name: "Meta information about this instance/cluster",
						Href: fmt.Sprintf("%s/v1/meta", origin),
					},
					&models.Link{
						Name:              "view complete schema",
						Href:              fmt.Sprintf("%s/v1/schema", origin),
						DocumentationHref: "https://www.semi.technology/documentation/weaviate/current/add-data/define_schema.html",
					},
					&models.Link{
						Name:              "CRUD schema",
						Href:              fmt.Sprintf("%s/v1/schema{/:className}", origin),
						DocumentationHref: "https://www.semi.technology/documentation/weaviate/current/add-data/define_schema.html",
					},
					&models.Link{
						Name:              "CRUD objects",
						Href:              fmt.Sprintf("%s/v1/objects{/:id}", origin),
						DocumentationHref: "https://www.semi.technology/documentation/weaviate/current/add-data/add_and_modify.html",
					},
					&models.Link{
						Name:              "trigger and view status of classifications",
						Href:              fmt.Sprintf("%s/v1/classifications{/:id}", origin),
						DocumentationHref: "https://www.semi.technology/documentation/weaviate/current/features/contextual-classification.html,https://www.semi.technology/documentation/weaviate/current/features/knn-classification.html",
					},
					&models.Link{
						Name:              "check if Weaviate is live (returns 200 on GET when live)",
						Href:              fmt.Sprintf("%s/v1/.well-known/live", origin),
						DocumentationHref: "https://www.semi.technology/developers/weaviate/current/restful-api-references/well-known.html",
					},
					&models.Link{
						Name:              "check if Weaviate is ready (returns 200 on GET when ready)",
						Href:              fmt.Sprintf("%s/v1/.well-known/ready", origin),
						DocumentationHref: "https://www.semi.technology/developers/weaviate/current/restful-api-references/well-known.html",
					},
					&models.Link{
						Name:              "view link to openid configuration (returns 404 on GET if no openid is configured)",
						Href:              fmt.Sprintf("%s/v1/.well-known/openid-configuration", origin),
						DocumentationHref: "https://www.semi.technology/developers/weaviate/current/restful-api-references/well-known.html",
					},

					// TODO: part of the text2vec-contextionary module
					&models.Link{
						Name:              "search contextionary for concepts (part of the text2vec-contextionary module)",
						Href:              fmt.Sprintf("%s/v1/modules/text2vec-contextionary/concepts/:concept", origin),
						DocumentationHref: "https://www.semi.technology/documentation/weaviate/current/features/adding-synonyms.html",
					},

					// TODO: part of the text2vec-contextionary module
					&models.Link{
						Name:              "extend contextionary with custom extensions (part of the text2vec-contextionary module)",
						Href:              fmt.Sprintf("%s/v1/modules/text2vec-contextionary/extensions", origin),
						DocumentationHref: "https://www.semi.technology/documentation/weaviate/current/features/adding-synonyms.html",
					},
				},
			}

			return operations.NewWeaviateRootOK().WithPayload(body)
		})
}
