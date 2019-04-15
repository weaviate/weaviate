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
 */
package restapi

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/creativesoftwarefdn/weaviate/validation"
	middleware "github.com/go-openapi/runtime/middleware"
)

func setupActionsHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	api.ActionsWeaviateActionsValidateHandler = actions.WeaviateActionsValidateHandlerFunc(func(params actions.WeaviateActionsValidateParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsValidateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		ctx := params.HTTPRequest.Context()
		validatedErr := validation.ValidateActionBody(ctx, params.Body, databaseSchema,
			dbConnector, network, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalQueryMeta)
		}()

		return actions.NewWeaviateActionsValidateOK()
	})

}
