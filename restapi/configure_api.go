package restapi

import (
	"net/http"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/batch"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
)

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.OidcAuth = func(token string, scopes []string) (*models.Principal, error) {
		return appState.OIDC.ValidateAndExtract(token, scopes)
	}

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
