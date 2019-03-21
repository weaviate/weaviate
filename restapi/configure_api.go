/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package restapi

import (
	"net/http"

	"github.com/creativesoftwarefdn/weaviate/restapi/batch"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
)

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = appState.OIDC.ValidateAndExtract

	setupSchemaHandlers(api)
	setupThingsHandlers(api)
	setupActionsHandlers(api)
	setupBatchHandlers(api)
	setupC11yHandlers(api)
	setupGraphQLHandlers(api)
	setupMiscHandlers(api)

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

func setupBatchHandlers(api *operations.WeaviateAPI) {
	batchAPI := batch.New(appState)

	api.WeaviateBatchingThingsCreateHandler = operations.
		WeaviateBatchingThingsCreateHandlerFunc(batchAPI.ThingsCreate)
	api.WeaviateBatchingActionsCreateHandler = operations.
		WeaviateBatchingActionsCreateHandlerFunc(batchAPI.ActionsCreate)
	api.WeaviateBatchingReferencesCreateHandler = operations.
		WeaviateBatchingReferencesCreateHandlerFunc(batchAPI.References)
}
