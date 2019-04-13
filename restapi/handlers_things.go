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

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/kinds"
	"github.com/creativesoftwarefdn/weaviate/lib/delayed_unlock"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/things"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/creativesoftwarefdn/weaviate/validation"
	jsonpatch "github.com/evanphx/json-patch"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

type kindHandlers struct {
	manager     *kinds.Manager
	requestsLog *telemetry.RequestsLog
}

func (h *kindHandlers) createThing(params things.WeaviateThingsCreateParams,
	principal *models.Principal) middleware.Responder {
	thing, err := h.manager.AddThing(params.HTTPRequest.Context(), params.Body)
	if err != nil {
		switch err.(type) {
		case kinds.ErrInvalidUserInput:
			return things.NewWeaviateThingsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewWeaviateThingsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAdd)
	return things.NewWeaviateThingsCreateOK().WithPayload(thing)
}

func (h *kindHandlers) createAction(params actions.WeaviateActionsCreateParams,
	principal *models.Principal) middleware.Responder {
	action, err := h.manager.AddAction(params.HTTPRequest.Context(), params.Body)
	if err != nil {
		switch err.(type) {
		case kinds.ErrInvalidUserInput:
			return actions.NewWeaviateActionsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewWeaviateActionsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAdd)
	return actions.NewWeaviateActionsCreateOK().WithPayload(action)
}

func (h *kindHandlers) getThing(params things.WeaviateThingsGetParams,
	principal *models.Principal) middleware.Responder {
	thing, err := h.manager.GetThing(params.HTTPRequest.Context(), params.ID)
	if err != nil {
		switch err.(type) {
		case kinds.ErrNotFound:
			return things.NewWeaviateThingsGetNotFound()
		default:
			return things.NewWeaviateThingsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	return things.NewWeaviateThingsGetOK().WithPayload(thing)
}

func (h *kindHandlers) getAction(params actions.WeaviateActionsGetParams,
	principal *models.Principal) middleware.Responder {
	action, err := h.manager.GetAction(params.HTTPRequest.Context(), params.ID)
	if err != nil {
		switch err.(type) {
		case kinds.ErrNotFound:
			return actions.NewWeaviateActionsGetNotFound()
		default:
			return actions.NewWeaviateActionsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	return actions.NewWeaviateActionsGetOK().WithPayload(action)
}

func (h *kindHandlers) getThings(params things.WeaviateThingsListParams,
	principal *models.Principal) middleware.Responder {
	list, err := h.manager.GetThings(params.HTTPRequest.Context(), getLimit(params.Limit))
	if err != nil {
		return things.NewWeaviateThingsListInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	// TODO: unify response with other verbs
	return things.NewWeaviateThingsListOK().
		WithPayload(&models.ThingsListResponse{
			Things:       list,
			TotalResults: int64(len(list)),
		})
}

func (h *kindHandlers) getActions(params actions.WeaviateActionsListParams,
	principal *models.Principal) middleware.Responder {
	list, err := h.manager.GetActions(params.HTTPRequest.Context(), getLimit(params.Limit))
	if err != nil {
		return actions.NewWeaviateActionsListInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	// TODO: unify response with other verbs
	return actions.NewWeaviateActionsListOK().
		WithPayload(&models.ActionsListResponse{
			Actions:      list,
			TotalResults: int64(len(list)),
		})
}

func (h *kindHandlers) updateThing(params things.WeaviateThingsUpdateParams,
	principal *models.Principal) middleware.Responder {
	thing, err := h.manager.UpdateThing(params.HTTPRequest.Context(), params.ID, params.Body)
	if err != nil {
		switch err.(type) {
		case kinds.ErrInvalidUserInput:
			return things.NewWeaviateThingsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewWeaviateThingsUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return things.NewWeaviateThingsUpdateOK().WithPayload(thing)
}

func (h *kindHandlers) updateAction(params actions.WeaviateActionUpdateParams,
	principal *models.Principal) middleware.Responder {
	action, err := h.manager.UpdateAction(params.HTTPRequest.Context(), params.ID, params.Body)
	if err != nil {
		switch err.(type) {
		case kinds.ErrInvalidUserInput:
			return actions.NewWeaviateActionUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewWeaviateActionUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return actions.NewWeaviateActionUpdateOK().WithPayload(action)
}

func (h *kindHandlers) deleteThing(params things.WeaviateThingsDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.DeleteThing(params.HTTPRequest.Context(), params.ID)
	if err != nil {
		switch err.(type) {
		case kinds.ErrNotFound:
			return things.NewWeaviateThingsDeleteNotFound()
		default:
			return things.NewWeaviateThingsDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return things.NewWeaviateThingsDeleteNoContent()
}

func (h *kindHandlers) deleteAction(params actions.WeaviateActionsDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.DeleteAction(params.HTTPRequest.Context(), params.ID)
	if err != nil {
		switch err.(type) {
		case kinds.ErrNotFound:
			return actions.NewWeaviateActionsDeleteNotFound()
		default:
			return actions.NewWeaviateActionsDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return actions.NewWeaviateActionsDeleteNoContent()
}

func setupThingsHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, manager *kinds.Manager) {
	h := &kindHandlers{manager, requestsLog}

	api.ThingsWeaviateThingsCreateHandler = things.
		WeaviateThingsCreateHandlerFunc(h.createThing)
	api.ThingsWeaviateThingsGetHandler = things.
		WeaviateThingsGetHandlerFunc(h.getThing)
	api.ThingsWeaviateThingsDeleteHandler = things.
		WeaviateThingsDeleteHandlerFunc(h.deleteThing)
	api.ThingsWeaviateThingsListHandler = things.
		WeaviateThingsListHandlerFunc(h.getThings)
	api.ThingsWeaviateThingsUpdateHandler = things.
		WeaviateThingsUpdateHandlerFunc(h.updateThing)

	api.ActionsWeaviateActionsCreateHandler = actions.
		WeaviateActionsCreateHandlerFunc(h.createAction)
	api.ActionsWeaviateActionsGetHandler = actions.
		WeaviateActionsGetHandlerFunc(h.getAction)
	api.ActionsWeaviateActionsDeleteHandler = actions.
		WeaviateActionsDeleteHandlerFunc(h.deleteAction)
	api.ActionsWeaviateActionsListHandler = actions.
		WeaviateActionsListHandlerFunc(h.getActions)
	api.ActionsWeaviateActionUpdateHandler = actions.
		WeaviateActionUpdateHandlerFunc(h.updateAction)

	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal *models.Principal) middleware.Responder {
		schemaLock, err := db.SchemaLock()
		if err != nil {
			return things.NewWeaviateThingsPatchInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(schemaLock)
		defer unlock(delayedLock)

		dbConnector := schemaLock.Connector()

		// Initialize response
		thingGetResponse := models.Thing{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get and transform object
		UUID := strfmt.UUID(params.ID)
		errGet := dbConnector.GetThing(params.HTTPRequest.Context(), UUID, &thingGetResponse)

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

		// Convert Thing object to JSON
		thingUpdateJSON, marshalErr := json.Marshal(thingGetResponse)
		if marshalErr != nil {
			return things.NewWeaviateThingsPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply(thingUpdateJSON)
		if applyErr != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(applyErr.Error()))
		}

		// Turn it into a Thing object
		thing := &models.Thing{}
		json.Unmarshal([]byte(updatedJSON), &thing)

		// Validate schema made after patching with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(schemaLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), thing,
			databaseSchema, dbConnector, network, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(
				createErrorResponseObject(fmt.Sprintf("validation failed: %s", validatedErr.Error())),
			)
		}

		ctx := params.HTTPRequest.Context()
		err = newReferenceSchemaUpdater(ctx, schemaLock.SchemaManager(), network, thing.Class, kind.THING_KIND).
			addNetworkDataTypes(thing.Schema)
		if err != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(
				createErrorResponseObject(err.Error()),
			)
		}

		// Update the database
		err = dbConnector.UpdateThing(ctx, thing, UUID)

		if err != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Create return Object
		thingGetResponse = *thing

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

		UUID := strfmt.UUID(params.ID)

		class := models.Thing{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.Class), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.DataType)
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

		if class.Schema == nil {
			class.Schema = map[string]interface{}{}
		}

		schema := class.Schema.(map[string]interface{})

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
		class.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateThing(ctx, &(class), UUID)
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

		UUID := strfmt.UUID(params.ID)

		class := models.Thing{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.Class), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.DataType)
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

		if class.Schema == nil {
			class.Schema = map[string]interface{}{}
		}

		schema := class.Schema.(map[string]interface{})

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
		class.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateThing(ctx, &(class), UUID)
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

		UUID := strfmt.UUID(params.ID)

		class := models.Thing{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetThing(ctx, UUID, &class)

		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find thing"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.THING_KIND, schema.AssertValidClassName(class.Class), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return things.NewWeaviateThingsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.DataType)
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

		if class.Schema == nil {
			class.Schema = map[string]interface{}{}
		}

		schema := class.Schema.(map[string]interface{})

		// (Over)write with multiple ref
		schema[params.PropertyName] = &params.Body
		class.Schema = schema

		// And update the last modified time.
		class.LastUpdateTimeUnix = connutils.NowUnix()

		err = dbConnector.UpdateThing(ctx, &(class), UUID)
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

func (h *kindHandlers) telemetryLogAsync(requestType, identifier string) {
	go func() {
		h.requestsLog.Register(requestType, identifier)
	}()
}
