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
	"encoding/json"
	"fmt"

	weaviateBroker "github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/lib/delayed_unlock"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/creativesoftwarefdn/weaviate/validation"
	jsonpatch "github.com/evanphx/json-patch"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

func setupActionsHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	api.ActionsWeaviateActionsGetHandler = actions.WeaviateActionsGetHandlerFunc(func(params actions.WeaviateActionsGetParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetAction(ctx, params.ActionID, &actionGetResponse)

		// Object is deleted
		if err != nil {
			return actions.NewWeaviateActionsGetNotFound()
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalQuery))
		}()

		// Get is successful
		return actions.NewWeaviateActionsGetOK().WithPayload(&actionGetResponse)
	})
	api.ActionsWeaviateActionHistoryGetHandler = actions.WeaviateActionHistoryGetHandlerFunc(func(params actions.WeaviateActionHistoryGetParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionHistoryGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Initialize response
		responseObject := models.ActionGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Set UUID var for easy usage
		UUID := strfmt.UUID(params.ActionID)

		// Get item from database
		ctx := params.HTTPRequest.Context()
		errGet := dbConnector.GetAction(ctx, UUID, &responseObject)

		// Init the response variables
		historyResponse := &models.ActionGetHistoryResponse{}
		historyResponse.PropertyHistory = []*models.ActionHistoryObject{}
		historyResponse.ActionID = UUID

		// Fill the history for these objects
		errHist := dbConnector.HistoryAction(ctx, UUID, &historyResponse.ActionHistory)

		// Check whether dont exist (both give an error) to return a not found
		if errGet != nil && (errHist != nil || len(historyResponse.PropertyHistory) == 0) {
			messaging.ErrorMessage(errGet)
			messaging.ErrorMessage(errHist)
			return actions.NewWeaviateActionHistoryGetNotFound()
		}

		// Action is deleted when we have an get error and no history error
		historyResponse.Deleted = errGet != nil && errHist == nil && len(historyResponse.PropertyHistory) != 0

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalQuery))
		}()

		return actions.NewWeaviateActionHistoryGetOK().WithPayload(historyResponse)
	})
	api.ActionsWeaviateActionsPatchHandler = actions.WeaviateActionsPatchHandlerFunc(func(params actions.WeaviateActionsPatchParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return actions.NewWeaviateActionsPatchInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(schemaLock)
		defer unlock(delayedLock)

		dbConnector := schemaLock.Connector()

		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get and transform object
		UUID := strfmt.UUID(params.ActionID)
		ctx := params.HTTPRequest.Context()
		errGet := dbConnector.GetAction(ctx, UUID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		actionGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Return error if UUID is not found.
		if errGet != nil {
			return actions.NewWeaviateActionsPatchNotFound()
		}

		// Get PATCH params in format RFC 6902
		jsonBody, marshalErr := json.Marshal(params.Body)
		patchObject, decodeErr := jsonpatch.DecodePatch([]byte(jsonBody))

		if marshalErr != nil || decodeErr != nil {
			return actions.NewWeaviateActionsPatchBadRequest()
		}

		// Convert ActionGetResponse object to JSON
		actionUpdateJSON, marshalErr := json.Marshal(actionGetResponse)
		if marshalErr != nil {
			return actions.NewWeaviateActionsPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply(actionUpdateJSON)

		if applyErr != nil {
			return actions.NewWeaviateActionsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(applyErr.Error()))
		}

		// Turn it into a Action object
		action := &models.Action{}
		json.Unmarshal([]byte(updatedJSON), &action)

		// Validate schema made after patching with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(schemaLock.GetSchema())
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), &action.ActionCreate,
			databaseSchema, dbConnector, network, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		err = newReferenceSchemaUpdater(ctx, schemaLock.SchemaManager(), network, action.AtClass, kind.ACTION_KIND).
			addNetworkDataTypes(action.Schema)
		if err != nil {
			return actions.NewWeaviateActionsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		if params.Async != nil && *params.Async == true {
			// Move the current properties to the history
			delayedLock.IncSteps()
			go func() {
				defer delayedLock.Unlock()
				dbConnector.MoveToHistoryAction(ctx, &oldAction.Action, params.ActionID, false)
			}()

			// Update the database
			delayedLock.IncSteps()
			go func() {
				defer delayedLock.Unlock()
				err := dbConnector.UpdateAction(ctx, action, UUID)
				if err != nil {
					fmt.Printf("Update action failed, because %s", err)
				}
			}()

			// Create return Object
			actionGetResponse.Action = *action

			// Register the function call
			go func() {
				requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulate))
			}()

			// Returns accepted so a Go routine can process in the background
			return actions.NewWeaviateActionsPatchAccepted().WithPayload(&actionGetResponse)
		} else {
			// Move the current properties to the history
			dbConnector.MoveToHistoryAction(ctx, &oldAction.Action, params.ActionID, false)

			err := dbConnector.UpdateAction(ctx, action, UUID)
			if err != nil {
				return actions.NewWeaviateActionUpdateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
			}

			// Create return Object
			actionGetResponse.Action = *action

			// Register the function call
			go func() {
				requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulate))
			}()

			// Returns accepted so a Go routine can process in the background
			return actions.NewWeaviateActionsPatchOK().WithPayload(&actionGetResponse)
		}
	})
	api.ActionsWeaviateActionsPropertiesCreateHandler = actions.WeaviateActionsPropertiesCreateHandlerFunc(func(params actions.WeaviateActionsPropertiesCreateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ActionID)

		class := models.ActionGetResponse{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetAction(ctx, UUID, &class)

		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find action"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.ACTION_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		// Look up the single ref.
		err = validation.ValidateSingleRef(ctx, serverConfig, params.Body, dbConnector, network,
			"reference not found")
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(err.Error()))
		}

		if class.Action.Schema == nil {
			class.Action.Schema = map[string]interface{}{}
		}

		schema := class.Action.Schema.(map[string]interface{})

		_, schemaPropPresent := schema[params.PropertyName]
		if !schemaPropPresent {
			schema[params.PropertyName] = []interface{}{}
		}

		schemaProp := schema[params.PropertyName]
		schemaPropList, ok := schemaProp.([]interface{})
		if !ok {
			panic("Internal error; this should be a liast")
		}

		// Add the reference
		schemaPropList = append(schemaPropList, params.Body)

		// Patch it back
		schema[params.PropertyName] = schemaPropList
		class.Action.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateAction(ctx, &(class.Action), UUID)
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulate))
		}()

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPropertiesCreateOK()
	})
	api.ActionsWeaviateActionsPropertiesDeleteHandler = actions.WeaviateActionsPropertiesDeleteHandlerFunc(func(params actions.WeaviateActionsPropertiesDeleteParams) middleware.Responder {
		if params.Body == nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a no valid reference", params.PropertyName)))
		}

		// Delete a specific SingleRef from the selected property.
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsPropertiesDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ActionID)

		class := models.ActionGetResponse{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetAction(ctx, UUID, &class)

		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find action"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.ACTION_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		//NOTE: we are _not_ verifying the reference; otherwise we cannot delete broken references.

		if class.Action.Schema == nil {
			class.Action.Schema = map[string]interface{}{}
		}

		schema := class.Action.Schema.(map[string]interface{})

		_, schemaPropPresent := schema[params.PropertyName]
		if !schemaPropPresent {
			schema[params.PropertyName] = []interface{}{}
		}

		schemaProp := schema[params.PropertyName]
		schemaPropList, ok := schemaProp.([]interface{})
		if !ok {
			panic("Internal error; this should be a liast")
		}

		crefStr := string(params.Body.NrDollarCref)

		// Remove if this reference is found.
		for idx, schemaPropItem := range schemaPropList {
			schemaRef := schemaPropItem.(map[string]interface{})

			if schemaRef["$cref"].(string) != crefStr {
				continue
			}

			// remove this one!
			schemaPropList = append(schemaPropList[:idx], schemaPropList[idx+1:]...)
			break // we can only remove one at the same time, so break the loop.
		}

		// Patch it back
		schema[params.PropertyName] = schemaPropList
		class.Action.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateAction(ctx, &(class.Action), UUID)
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulate))
		}()

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPropertiesDeleteNoContent()
	})
	api.ActionsWeaviateActionsPropertiesUpdateHandler = actions.WeaviateActionsPropertiesUpdateHandlerFunc(func(params actions.WeaviateActionsPropertiesUpdateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsPropertiesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ActionID)

		class := models.ActionGetResponse{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetAction(ctx, UUID, &class)

		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find action"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.ACTION_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		// Look up the single ref.
		err = validation.ValidateMultipleRef(ctx, serverConfig, &params.Body, dbConnector, network,
			"reference not found")
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("validation failed: %s", err.Error())))
		}

		if class.Action.Schema == nil {
			class.Action.Schema = map[string]interface{}{}
		}

		schema := class.Action.Schema.(map[string]interface{})

		// (Over)write with multiple ref
		schema[params.PropertyName] = &params.Body
		class.Action.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateAction(ctx, &(class.Action), UUID)
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("could not perform db update query: %s", err.Error())))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulate))
		}()

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPropertiesCreateOK()
	})
	api.ActionsWeaviateActionUpdateHandler = actions.WeaviateActionUpdateHandlerFunc(func(params actions.WeaviateActionUpdateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()
		dbConnector := dbLock.Connector()

		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		UUID := params.ActionID
		ctx := params.HTTPRequest.Context()
		errGet := dbConnector.GetAction(ctx, UUID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		// If there are no results, there is an error
		if errGet != nil {
			// Object not found response.
			return actions.NewWeaviateActionUpdateNotFound()
		}

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), &params.Body.ActionCreate,
			databaseSchema, dbConnector, network, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionUpdateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		delayedLock.IncSteps()
		go func() {
			defer delayedLock.Unlock()
			dbConnector.MoveToHistoryAction(ctx, &oldAction.Action, params.ActionID, false)
		}()

		// Update the database
		params.Body.LastUpdateTimeUnix = connutils.NowUnix()
		params.Body.CreationTimeUnix = actionGetResponse.CreationTimeUnix

		delayedLock.IncSteps()
		go func() {
			defer delayedLock.Unlock()
			dbConnector.UpdateAction(ctx, &params.Body.Action, UUID)
		}()

		// Create object to return
		responseObject := &models.ActionGetResponse{}
		responseObject.Action = params.Body.Action
		responseObject.ActionID = UUID

		// broadcast to MQTT
		mqttJson, _ := json.Marshal(responseObject)
		weaviateBroker.Publish("/actions/"+string(responseObject.ActionID), string(mqttJson[:]))

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulate))
		}()

		// Return SUCCESS (NOTE: this is ACCEPTED, so the dbConnector.Add should have a go routine)
		return actions.NewWeaviateActionUpdateAccepted().WithPayload(responseObject)
	})
	api.ActionsWeaviateActionsValidateHandler = actions.WeaviateActionsValidateHandlerFunc(func(params actions.WeaviateActionsValidateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsValidateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		ctx := params.HTTPRequest.Context()
		validatedErr := validation.ValidateActionBody(ctx, &params.Body.ActionCreate, databaseSchema,
			dbConnector, network, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalQueryMeta))
		}()

		return actions.NewWeaviateActionsValidateOK()
	})
	api.ActionsWeaviateActionsCreateHandler = actions.WeaviateActionsCreateHandlerFunc(func(params actions.WeaviateActionsCreateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return actions.NewWeaviateActionsCreateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(schemaLock)
		defer delayedLock.Unlock()
		dbConnector := schemaLock.Connector()

		// Generate UUID for the new object
		UUID := connutils.GenerateUUID()

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(schemaLock.GetSchema())
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), params.Body.Action,
			databaseSchema, dbConnector, network, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		ctx := params.HTTPRequest.Context()
		err = newReferenceSchemaUpdater(ctx, schemaLock.SchemaManager(), network, params.Body.Action.AtClass, kind.ACTION_KIND).
			addNetworkDataTypes(params.Body.Action.Schema)
		if err != nil {
			return actions.NewWeaviateActionsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Make Action-Object
		action := &models.Action{}
		action.AtClass = params.Body.Action.AtClass
		action.AtContext = params.Body.Action.AtContext
		action.Schema = params.Body.Action.Schema
		action.CreationTimeUnix = connutils.NowUnix()
		action.LastUpdateTimeUnix = 0

		responseObject := &models.ActionGetResponse{}
		responseObject.Action = *action
		responseObject.ActionID = UUID

		if params.Body.Async {
			delayedLock.IncSteps()
			go func() {
				defer delayedLock.Unlock()
				dbConnector.AddAction(ctx, action, UUID)
			}()

			// Register the function call
			go func() {
				requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalAdd))
			}()

			return actions.NewWeaviateActionsCreateAccepted().WithPayload(responseObject)
		} else {
			//TODO gh-617: handle errors
			err := dbConnector.AddAction(ctx, action, UUID)
			if err != nil {
				panic(err)
			}

			// Register the function call
			go func() {
				requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalAdd))
			}()

			return actions.NewWeaviateActionsCreateOK().WithPayload(responseObject)
		}
	})
	api.ActionsWeaviateActionsDeleteHandler = actions.WeaviateActionsDeleteHandlerFunc(func(params actions.WeaviateActionsDeleteParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		ctx := params.HTTPRequest.Context()
		errGet := dbConnector.GetAction(ctx, params.ActionID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		// Not found
		if errGet != nil {
			return actions.NewWeaviateActionsDeleteNotFound()
		}

		actionGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Move the current properties to the history
		delayedLock.IncSteps()
		go func() {
			defer delayedLock.Unlock()
			dbConnector.MoveToHistoryAction(ctx, &oldAction.Action, params.ActionID, false)
		}()

		// Add new row as GO-routine
		delayedLock.IncSteps()
		go func() {
			defer delayedLock.Unlock()
			dbConnector.DeleteAction(ctx, &actionGetResponse.Action, params.ActionID)
		}()

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalManipulate))
		}()

		// Return 'No Content'
		return actions.NewWeaviateActionsDeleteNoContent()
	})

	api.ActionsWeaviateActionsListHandler = actions.WeaviateActionsListHandlerFunc(func(params actions.WeaviateActionsListParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsListInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// Initialize response
		actionsResponse := models.ActionsListResponse{}
		actionsResponse.Actions = []*models.ActionGetResponse{}

		// List all results
		ctx := params.HTTPRequest.Context()
		err = dbConnector.ListActions(ctx, limit, (page-1)*limit, []*connutils.WhereQuery{}, &actionsResponse)
		if err != nil {
			return actions.NewWeaviateActionsListInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.NewRequestTypeLog(telemetry.TypeREST, telemetry.LocalQuery))
		}()

		return actions.NewWeaviateActionsListOK().WithPayload(&actionsResponse)
	})
}
