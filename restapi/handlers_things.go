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
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/things"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/creativesoftwarefdn/weaviate/validation"
	jsonpatch "github.com/evanphx/json-patch"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

func setupThingsHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	/*
	 * HANDLE THINGS
	 */
	api.ThingsWeaviateThingsCreateHandler = things.WeaviateThingsCreateHandlerFunc(func(params things.WeaviateThingsCreateParams, principal *models.Principal) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return things.NewWeaviateThingsCreateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(schemaLock)
		defer unlock(delayedLock)

		dbConnector := schemaLock.Connector()

		// Generate UUID for the new object
		UUID := connutils.GenerateUUID()

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(schemaLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), params.Body.Thing, databaseSchema,
			dbConnector, network, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Make Thing-Object
		thing := &models.Thing{}
		thing.Schema = params.Body.Thing.Schema
		thing.AtClass = params.Body.Thing.AtClass
		thing.AtContext = params.Body.Thing.AtContext
		thing.CreationTimeUnix = connutils.NowUnix()
		thing.LastUpdateTimeUnix = 0

		responseObject := &models.ThingGetResponse{}
		responseObject.Thing = *thing
		responseObject.ThingID = UUID

		ctx := params.HTTPRequest.Context()
		refSchemaUpdater := newReferenceSchemaUpdater(ctx, schemaLock.SchemaManager(), network, params.Body.Thing.AtClass, kind.THING_KIND)

		if params.Body.Async {
			delayedLock.IncSteps()
			go func() {
				defer unlock(delayedLock)
				dbConnector.AddThing(ctx, thing, UUID)
				refSchemaUpdater.addNetworkDataTypes(params.Body.Thing.Schema)
			}()

			// Register the function call
			go func() {
				requestsLog.Register(telemetry.TypeREST, telemetry.LocalAdd)
			}()

			return things.NewWeaviateThingsCreateAccepted().WithPayload(responseObject)
		}

		dbConnector.AddThing(ctx, thing, UUID)
		err = refSchemaUpdater.addNetworkDataTypes(params.Body.Thing.Schema)
		if err != nil {

			// Register the function call
			go func() {
				requestsLog.Register(telemetry.TypeREST, telemetry.LocalAdd)
			}()

			return things.NewWeaviateThingsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}
		return things.NewWeaviateThingsCreateOK().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsDeleteHandler = things.WeaviateThingsDeleteHandlerFunc(func(params things.WeaviateThingsDeleteParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		errGet := dbConnector.GetThing(params.HTTPRequest.Context(), params.ThingID, &thingGetResponse)

		// Save the old-thing in a variable
		oldThing := thingGetResponse

		// Not found
		if errGet != nil {
			return things.NewWeaviateThingsDeleteNotFound()
		}

		thingGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Move the current properties to the history
		delayedLock.IncSteps()
		ctx := params.HTTPRequest.Context()
		go func() {
			delayedLock.Unlock()
			dbConnector.MoveToHistoryThing(ctx, &oldThing.Thing, params.ThingID, true)
		}()

		// Add new row as GO-routine
		delayedLock.IncSteps()
		go func() {
			delayedLock.Unlock()
			dbConnector.DeleteThing(ctx, &thingGetResponse.Thing, params.ThingID)
		}()

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Return 'No Content'
		return things.NewWeaviateThingsDeleteNoContent()
	})
	api.ThingsWeaviateThingsGetHandler = things.WeaviateThingsGetHandlerFunc(func(params things.WeaviateThingsGetParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(dbLock)
		dbConnector := dbLock.Connector()

		// Initialize response
		responseObject := models.ThingGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Get item from database
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetThing(ctx, strfmt.UUID(params.ThingID), &responseObject)

		// Object is not found
		if err != nil {
			messaging.ErrorMessage(err)
			return things.NewWeaviateThingsGetNotFound()
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalQuery)
		}()

		// Get is successful
		return things.NewWeaviateThingsGetOK().WithPayload(&responseObject)
	})

	api.ThingsWeaviateThingHistoryGetHandler = things.WeaviateThingHistoryGetHandlerFunc(func(params things.WeaviateThingHistoryGetParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingHistoryGetInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(dbLock)
		dbConnector := dbLock.Connector()

		// Initialize response
		responseObject := models.ThingGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Set UUID var for easy usage
		UUID := strfmt.UUID(params.ThingID)

		// Get item from database
		errGet := dbConnector.GetThing(params.HTTPRequest.Context(), UUID, &responseObject)

		// Init the response variables
		historyResponse := &models.ThingGetHistoryResponse{}
		historyResponse.PropertyHistory = []*models.ThingHistoryObject{}
		historyResponse.ThingID = UUID

		// Fill the history for these objects
		ctx := params.HTTPRequest.Context()
		errHist := dbConnector.HistoryThing(ctx, UUID, &historyResponse.ThingHistory)

		// Check whether dont exist (both give an error) to return a not found
		if errGet != nil && (errHist != nil || len(historyResponse.PropertyHistory) == 0) {
			messaging.ErrorMessage(errGet)
			messaging.ErrorMessage(errHist)
			return things.NewWeaviateThingHistoryGetNotFound()
		}

		// Thing is deleted when we have an get error and no history error
		historyResponse.Deleted = errGet != nil && errHist == nil && len(historyResponse.PropertyHistory) != 0

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalQuery)
		}()

		return things.NewWeaviateThingHistoryGetOK().WithPayload(historyResponse)
	})

	api.ThingsWeaviateThingsListHandler = things.WeaviateThingsListHandlerFunc(func(params things.WeaviateThingsListParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsListInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(dbLock)
		dbConnector := dbLock.Connector()

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// Initialize response
		thingsResponse := models.ThingsListResponse{}
		thingsResponse.Things = []*models.ThingGetResponse{}

		// List all results
		ctx := params.HTTPRequest.Context()
		err = dbConnector.ListThings(ctx, limit, (page-1)*limit, []*connutils.WhereQuery{}, &thingsResponse)
		if err != nil {
			return things.NewWeaviateThingsListInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalQuery)
		}()

		return things.NewWeaviateThingsListOK().WithPayload(&thingsResponse)
	})
	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal *models.Principal) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return things.NewWeaviateThingsPatchInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(schemaLock)
		defer unlock(delayedLock)

		dbConnector := schemaLock.Connector()

		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get and transform object
		UUID := strfmt.UUID(params.ThingID)
		errGet := dbConnector.GetThing(params.HTTPRequest.Context(), UUID, &thingGetResponse)

		// Save the old-thing in a variable
		oldThing := thingGetResponse

		// Add update time
		thingGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Return error if UUID is not found.
		if errGet != nil {
			return things.NewWeaviateThingsPatchNotFound()
		}

		// Get PATCH params in format RFC 6902
		jsonBody, marshalErr := json.Marshal(params.Body)
		patchObject, decodeErr := jsonpatch.DecodePatch([]byte(jsonBody))

		if marshalErr != nil || decodeErr != nil {
			return things.NewWeaviateThingsPatchBadRequest()
		}

		// Convert ThingGetResponse object to JSON
		thingUpdateJSON, marshalErr := json.Marshal(thingGetResponse)
		if marshalErr != nil {
			return things.NewWeaviateThingsPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply(thingUpdateJSON)
		if applyErr != nil {
			fmt.Printf("patch attempt on %#v failed. Patch: %#v", thingUpdateJSON, patchObject)
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(applyErr.Error()))
		}

		// Turn it into a Thing object
		thing := &models.Thing{}
		json.Unmarshal([]byte(updatedJSON), &thing)

		// Validate schema made after patching with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(schemaLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), &thing.ThingCreate,
			databaseSchema, dbConnector, network, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(
				createErrorResponseObject(fmt.Sprintf("validation failed: %s", validatedErr.Error())),
			)
		}

		ctx := params.HTTPRequest.Context()
		err = newReferenceSchemaUpdater(ctx, schemaLock.SchemaManager(), network, thing.AtClass, kind.THING_KIND).
			addNetworkDataTypes(thing.Schema)
		if err != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(
				createErrorResponseObject(err.Error()),
			)
		}

		if params.Async != nil && *params.Async == true {
			// Move the current properties to the history
			delayedLock.IncSteps()
			go func() {
				delayedLock.Unlock()
				dbConnector.MoveToHistoryThing(ctx, &oldThing.Thing, UUID, false)
			}()

			// Update the database
			delayedLock.IncSteps()
			go func() {
				delayedLock.Unlock()
				dbConnector.UpdateThing(ctx, thing, UUID)
			}()

			// Create return Object
			thingGetResponse.Thing = *thing

			// Register the function call
			go func() {
				requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
			}()

			// Returns accepted so a Go routine can process in the background
			return things.NewWeaviateThingsPatchAccepted().WithPayload(&thingGetResponse)
		}

		// Move the current properties to the history
		dbConnector.MoveToHistoryThing(ctx, &oldThing.Thing, UUID, false)

		// Update the database
		err = dbConnector.UpdateThing(ctx, thing, UUID)

		if err != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Create return Object
		thingGetResponse.Thing = *thing

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsPatchOK().WithPayload(&thingGetResponse)
	})
	api.ThingsWeaviateThingsReferencesCreateHandler = things.WeaviateThingsReferencesCreateHandlerFunc(func(params things.WeaviateThingsReferencesCreateParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		// Look up the single ref.
		err = validation.ValidateSingleRef(ctx, serverConfig, params.Body, dbConnector, network,
			"reference not found")
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(err.Error()))
		}

		if class.Thing.Schema == nil {
			class.Thing.Schema = map[string]interface{}{}
		}

		schema := class.Thing.Schema.(map[string]interface{})

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
		class.Thing.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateThing(ctx, &(class.Thing), UUID)
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsReferencesCreateOK()
	})
	api.ThingsWeaviateThingsReferencesDeleteHandler = things.WeaviateThingsReferencesDeleteHandlerFunc(func(params things.WeaviateThingsReferencesDeleteParams, principal *models.Principal) middleware.Responder {
		if params.Body == nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a no valid reference", params.PropertyName)))
		}

		// Delete a specific SingleRef from the selected property.
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsReferencesDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		//NOTE: we are _not_ verifying the reference; otherwise we cannot delete broken references.

		if class.Thing.Schema == nil {
			class.Thing.Schema = map[string]interface{}{}
		}

		schema := class.Thing.Schema.(map[string]interface{})

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
		class.Thing.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateThing(ctx, &(class.Thing), UUID)
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsReferencesDeleteNoContent()
	})
	api.ThingsWeaviateThingsReferencesUpdateHandler = things.WeaviateThingsReferencesUpdateHandlerFunc(func(params things.WeaviateThingsReferencesUpdateParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsReferencesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		// Look up the single ref.
		err = validation.ValidateMultipleRef(ctx, serverConfig, &params.Body, dbConnector, network,
			"reference not found")
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(err.Error()))
		}

		if class.Thing.Schema == nil {
			class.Thing.Schema = map[string]interface{}{}
		}

		schema := class.Thing.Schema.(map[string]interface{})

		// (Over)write with multiple ref
		schema[params.PropertyName] = &params.Body
		class.Thing.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateThing(ctx, &(class.Thing), UUID)
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsReferencesCreateOK()
	})
	api.ThingsWeaviateThingsUpdateHandler = things.WeaviateThingsUpdateHandlerFunc(func(params things.WeaviateThingsUpdateParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		UUID := params.ThingID
		errGet := dbConnector.GetThing(params.HTTPRequest.Context(), UUID, &thingGetResponse)

		// Save the old-thing in a variable
		oldThing := thingGetResponse

		// If there are no results, there is an error
		if errGet != nil {
			// Object not found response.
			return things.NewWeaviateThingsUpdateNotFound()
		}

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), &params.Body.ThingCreate,
			databaseSchema, dbConnector, network, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsUpdateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		ctx := params.HTTPRequest.Context()
		delayedLock.IncSteps()
		go func() {
			delayedLock.Unlock()
			dbConnector.MoveToHistoryThing(ctx, &oldThing.Thing, UUID, false)
		}()

		// Update the database
		params.Body.LastUpdateTimeUnix = connutils.NowUnix()
		params.Body.CreationTimeUnix = thingGetResponse.CreationTimeUnix
		params.Body.Key = thingGetResponse.Key
		delayedLock.IncSteps()
		go func() {
			delayedLock.Unlock()
			dbConnector.UpdateThing(ctx, &params.Body.Thing, UUID)
		}()

		// Create object to return
		responseObject := &models.ThingGetResponse{}
		responseObject.Thing = params.Body.Thing
		responseObject.ThingID = UUID

		// broadcast to MQTT
		mqttJson, _ := json.Marshal(responseObject)
		weaviateBroker.Publish("/things/"+string(responseObject.ThingID), string(mqttJson[:]))

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Return SUCCESS (NOTE: this is ACCEPTED, so the dbConnector.Add should have a go routine)
		return things.NewWeaviateThingsUpdateAccepted().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsValidateHandler = things.WeaviateThingsValidateHandlerFunc(func(params things.WeaviateThingsValidateParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return things.NewWeaviateThingsValidateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		defer unlock(dbLock)
		dbConnector := dbLock.Connector()

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), params.Body, databaseSchema,
			dbConnector, network, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalQueryMeta)
		}()

		return things.NewWeaviateThingsValidateOK()
	})
}
