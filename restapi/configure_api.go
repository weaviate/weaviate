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
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	telemutils "github.com/creativesoftwarefdn/weaviate/telemetry/utils"
	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
)

var requestsLog *telemetry.RequestsLog
var reporter *telemetry.Reporter

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = appState.OIDC.ValidateAndExtract

	enabled := telemutils.IsEnabled()
	interval := telemutils.GetInterval()
	url := telemutils.GetURL()

	requestsLog = telemetry.NewLog(enabled)

	reporter = telemetry.NewReporter(requestsLog, interval, url, enabled, false)

	setupSchemaHandlers(api, requestsLog)
	setupThingsHandlers(api, requestsLog)
	setupActionsHandlers(api, requestsLog)
	setupBatchHandlers(api, requestsLog)
	setupC11yHandlers(api)
	setupGraphQLHandlers(api, requestsLog)
	setupMiscHandlers(api, requestsLog)

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

func setupBatchHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	batchAPI := batch.New(appState, requestsLog)

	api.WeaviateBatchingThingsCreateHandler = operations.
		WeaviateBatchingThingsCreateHandlerFunc(batchAPI.ThingsCreate)
	api.WeaviateBatchingActionsCreateHandler = operations.
		WeaviateBatchingActionsCreateHandlerFunc(batchAPI.ActionsCreate)
	api.WeaviateBatchingReferencesCreateHandler = operations.
		WeaviateBatchingReferencesCreateHandlerFunc(batchAPI.References)
}
