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
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/lib/delayed_unlock"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/creativesoftwarefdn/weaviate/validation"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

func setupActionsHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog) {
	api.ActionsWeaviateActionsReferencesDeleteHandler = actions.WeaviateActionsReferencesDeleteHandlerFunc(func(params actions.WeaviateActionsReferencesDeleteParams, principal *models.Principal) middleware.Responder {
		if params.Body == nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a no valid reference", params.PropertyName)))
		}

		// Delete a specific SingleRef from the selected property.
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsReferencesDeleteInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ID)

		class := models.Action{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetAction(ctx, UUID, &class)

		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find action"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.ACTION_KIND, schema.AssertValidClassName(class.Class), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.DataType)
		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
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

		err = dbConnector.UpdateAction(ctx, &class, UUID)
		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsReferencesDeleteNoContent()
	})
	api.ActionsWeaviateActionsReferencesUpdateHandler = actions.WeaviateActionsReferencesUpdateHandlerFunc(func(params actions.WeaviateActionsReferencesUpdateParams, principal *models.Principal) middleware.Responder {
		dbLock, err := db.ConnectorLock()
		if err != nil {
			return actions.NewWeaviateActionsReferencesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(err))
		}
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		UUID := strfmt.UUID(params.ID)

		class := models.Action{}
		ctx := params.HTTPRequest.Context()
		err = dbConnector.GetAction(ctx, UUID, &class)

		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject("Could not find action"))
		}

		dbSchema := dbLock.GetSchema()

		// Find property and see if it has a max cardinality of >1
		err, prop := dbSchema.GetProperty(kind.ACTION_KIND, schema.AssertValidClassName(class.Class), schema.AssertValidPropertyName(params.PropertyName))
		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find property '%s'; %s", params.PropertyName, err.Error())))
		}
		propertyDataType, err := dbSchema.FindPropertyDataType(prop.DataType)
		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Could not find datatype of property '%s'; %s", params.PropertyName, err.Error())))
		}
		if propertyDataType.IsPrimitive() {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' is a primitive datatype", params.PropertyName)))
		}
		if prop.Cardinality == nil || *prop.Cardinality != "many" {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a cardinality of atMostOne", params.PropertyName)))
		}

		// Look up the single ref.
		err = validation.ValidateMultipleRef(ctx, serverConfig, &params.Body, dbConnector, network,
			"reference not found")
		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("validation failed: %s", err.Error())))
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

		err = dbConnector.UpdateAction(ctx, &(class), UUID)
		if err != nil {
			return actions.NewWeaviateActionsReferencesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("could not perform db update query: %s", err.Error())))
		}

		// Register the function call
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalManipulate)
		}()

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsReferencesCreateOK()
	})
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
