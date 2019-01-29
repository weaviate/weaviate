/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package restapi

import (
	"context"
	"encoding/json"
	"fmt"

	weaviateBroker "github.com/creativesoftwarefdn/weaviate/broker"
	connutils "github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/lib/delayed_unlock"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/things"
	"github.com/creativesoftwarefdn/weaviate/validation"
	jsonpatch "github.com/evanphx/json-patch"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

func setupThingsHandlers(api *operations.WeaviateAPI) {
	/*
	 * HANDLE THINGS
	 */
	api.ThingsWeaviateThingsCreateHandler = things.WeaviateThingsCreateHandlerFunc(func(ctx context.Context, params things.WeaviateThingsCreateParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil { //TODO: gh-685
			panic(err)
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

		refSchemaUpdater := newReferenceSchemaUpdater(ctx, schemaLock.SchemaManager(), network, params.Body.Thing.AtClass, kind.THING_KIND)

		if params.Body.Async {
			delayedLock.IncSteps()
			go func() {
				defer unlock(delayedLock)
				dbConnector.AddThing(ctx, thing, UUID)
				refSchemaUpdater.addNetworkDataTypes(params.Body.Thing.Schema)
			}()
			return things.NewWeaviateThingsCreateAccepted().WithPayload(responseObject)
		}

		dbConnector.AddThing(ctx, thing, UUID)
		err = refSchemaUpdater.addNetworkDataTypes(params.Body.Thing.Schema)
		if err != nil {
			return things.NewWeaviateThingsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}
		return things.NewWeaviateThingsCreateOK().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsDeleteHandler = things.WeaviateThingsDeleteHandlerFunc(func(ctx context.Context, params things.WeaviateThingsDeleteParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
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

		// Return 'No Content'
		return things.NewWeaviateThingsDeleteNoContent()
	})
	api.ThingsWeaviateThingsGetHandler = things.WeaviateThingsGetHandlerFunc(func(ctx context.Context, params things.WeaviateThingsGetParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
		}
		defer unlock(dbLock)
		dbConnector := dbLock.Connector()

		// Initialize response
		responseObject := models.ThingGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Get item from database
		err = dbConnector.GetThing(ctx, strfmt.UUID(params.ThingID), &responseObject)

		// Object is not found
		if err != nil {
			messaging.ErrorMessage(err)
			return things.NewWeaviateThingsGetNotFound()
		}

		// Get is successful
		return things.NewWeaviateThingsGetOK().WithPayload(&responseObject)
	})

	api.ThingsWeaviateThingHistoryGetHandler = things.WeaviateThingHistoryGetHandlerFunc(func(ctx context.Context, params things.WeaviateThingHistoryGetParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
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
		errHist := dbConnector.HistoryThing(ctx, UUID, &historyResponse.ThingHistory)

		// Check whether dont exist (both give an error) to return a not found
		if errGet != nil && (errHist != nil || len(historyResponse.PropertyHistory) == 0) {
			messaging.ErrorMessage(errGet)
			messaging.ErrorMessage(errHist)
			return things.NewWeaviateThingHistoryGetNotFound()
		}

		// Thing is deleted when we have an get error and no history error
		historyResponse.Deleted = errGet != nil && errHist == nil && len(historyResponse.PropertyHistory) != 0

		return things.NewWeaviateThingHistoryGetOK().WithPayload(historyResponse)
	})

	api.ThingsWeaviateThingsListHandler = things.WeaviateThingsListHandlerFunc(func(ctx context.Context, params things.WeaviateThingsListParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
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
		err = dbConnector.ListThings(ctx, limit, (page-1)*limit, []*connutils.WhereQuery{}, &thingsResponse)

		if err != nil {
			messaging.ErrorMessage(err)
		}

		return things.NewWeaviateThingsListOK().WithPayload(&thingsResponse)
	})
	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(ctx context.Context, params things.WeaviateThingsPatchParams) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil { //TODO: gh-685
			panic(err)
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

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsPatchOK().WithPayload(&thingGetResponse)
	})
	api.ThingsWeaviateThingsPropertiesCreateHandler = things.WeaviateThingsPropertiesCreateHandlerFunc(func(ctx context.Context, params things.WeaviateThingsPropertiesCreateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		// Look up the single ref.
		err = validation.ValidateSingleRef(ctx, serverConfig, params.Body, dbConnector, network,
			"reference not found")
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
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
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsPropertiesCreateOK()
	})
	api.ThingsWeaviateThingsPropertiesDeleteHandler = things.WeaviateThingsPropertiesDeleteHandlerFunc(func(ctx context.Context, params things.WeaviateThingsPropertiesDeleteParams) middleware.Responder {
		if params.Body == nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a no valid reference", params.PropertyName)))
		}

		// Delete a specific SingleRef from the selected property.
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
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
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsPropertiesDeleteNoContent()
	})
	api.ThingsWeaviateThingsPropertiesUpdateHandler = things.WeaviateThingsPropertiesUpdateHandlerFunc(func(ctx context.Context, params things.WeaviateThingsPropertiesUpdateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer unlock(delayedLock)

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.AtClass), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.AtDataType)
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		// Look up the single ref.
		err = validation.ValidateMultipleRef(ctx, serverConfig, &params.Body, dbConnector, network,
			"reference not found")
		if err != nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
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
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsPropertiesCreateOK()
	})
	api.ThingsWeaviateThingsUpdateHandler = things.WeaviateThingsUpdateHandlerFunc(func(ctx context.Context, params things.WeaviateThingsUpdateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
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

		// Return SUCCESS (NOTE: this is ACCEPTED, so the dbConnector.Add should have a go routine)
		return things.NewWeaviateThingsUpdateAccepted().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsValidateHandler = things.WeaviateThingsValidateHandlerFunc(func(ctx context.Context, params things.WeaviateThingsValidateParams) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil { //TODO: gh-685
			panic(err)
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

		return things.NewWeaviateThingsValidateOK()
	})
}
