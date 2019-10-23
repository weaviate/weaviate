//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package rest

import (
	"context"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/telemetry"

	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/contextionary_api"
)

type inspector interface {
	GetWords(ctx context.Context, words string) (*models.C11yWordsResponse, error)
}

func setupC11yHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, inspector inspector) {
	/*
	 * HANDLE C11Y
	 */

	api.ContextionaryAPIC11yWordsHandler = contextionary_api.C11yWordsHandlerFunc(func(params contextionary_api.C11yWordsParams, principal *models.Principal) middleware.Responder {
		ctx := params.HTTPRequest.Context()
		// Register the request

		res, err := inspector.GetWords(ctx, params.Words)
		if err != nil {
			return contextionary_api.NewC11yWordsBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalTools)
		}()

		return contextionary_api.NewC11yWordsOK().WithPayload(res)
	})

	api.ContextionaryAPIC11yConceptsHandler = contextionary_api.C11yConceptsHandlerFunc(func(params contextionary_api.C11yConceptsParams, principal *models.Principal) middleware.Responder {
		ctx := params.HTTPRequest.Context()
		// Register the request

		res, err := inspector.GetWords(ctx, params.Concept)
		if err != nil {
			return contextionary_api.NewC11yConceptsBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}

		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalTools)
		}()

		return contextionary_api.NewC11yConceptsOK().WithPayload(res)
	})

	api.ContextionaryAPIC11yCorpusGetHandler = contextionary_api.C11yCorpusGetHandlerFunc(func(params contextionary_api.C11yCorpusGetParams, principal *models.Principal) middleware.Responder {
		return middleware.NotImplemented("operation contextionary_api.C11yCorpusGet has not yet been implemented")
	})

}
