//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/classifications"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/classification"
)

func setupClassificationHandlers(api *operations.WeaviateAPI,
	classifier *classification.Classifier,
) {
	api.ClassificationsClassificationsGetHandler = classifications.ClassificationsGetHandlerFunc(
		func(params classifications.ClassificationsGetParams, principal *models.Principal) middleware.Responder {
			res, err := classifier.Get(params.HTTPRequest.Context(), principal, strfmt.UUID(params.ID))
			if err != nil {
				return classifications.NewClassificationsGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
			}

			if res == nil {
				return classifications.NewClassificationsGetNotFound()
			}

			return classifications.NewClassificationsGetOK().WithPayload(res)
		},
	)

	api.ClassificationsClassificationsPostHandler = classifications.ClassificationsPostHandlerFunc(
		func(params classifications.ClassificationsPostParams, principal *models.Principal) middleware.Responder {
			res, err := classifier.Schedule(params.HTTPRequest.Context(), principal, *params.Params)
			if err != nil {
				return classifications.NewClassificationsPostBadRequest().WithPayload(errPayloadFromSingleErr(err))
			}

			return classifications.NewClassificationsPostCreated().WithPayload(res)
		},
	)
}
