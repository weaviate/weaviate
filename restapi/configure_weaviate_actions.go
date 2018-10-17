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

// Package restapi with all rest API functions.
package restapi

import (
	"encoding/json"

	jsonpatch "github.com/evanphx/json-patch"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/auth"
	"github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/things"
	"github.com/creativesoftwarefdn/weaviate/validation"
)

func configureAPIActions(api *operations.WeaviateAPI) {
	api.ActionsWeaviateActionsGetHandler = actions.WeaviateActionsGetHandlerFunc(func(params actions.WeaviateActionsGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
		err := dbConnector.GetAction(ctx, params.ActionID, &actionGetResponse)

		// Object is deleted
		if err != nil || actionGetResponse.Key == nil {
			return actions.NewWeaviateActionsGetNotFound()
		}

		// This is a read function, validate if allowed to read?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionsGetForbidden()
		}

		// Get is successful
		return actions.NewWeaviateActionsGetOK().WithPayload(&actionGetResponse)
	})
	api.ActionsWeaviateActionHistoryGetHandler = actions.WeaviateActionHistoryGetHandlerFunc(func(params actions.WeaviateActionHistoryGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		responseObject := models.ActionGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Set UUID var for easy usage
		UUID := strfmt.UUID(params.ActionID)

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
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

		if errHist == nil {
			if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, historyResponse.Key.NrDollarCref); !allowed {
				return actions.NewWeaviateActionHistoryGetForbidden()
			}
		} else if errGet == nil {
			if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, responseObject.Key.NrDollarCref); !allowed {
				return actions.NewWeaviateActionHistoryGetForbidden()
			}
		}

		// Action is deleted when we have an get error and no history error
		historyResponse.Deleted = errGet != nil && errHist == nil && len(historyResponse.PropertyHistory) != 0

		return actions.NewWeaviateActionHistoryGetOK().WithPayload(historyResponse)
	})
	api.ActionsWeaviateActionsPatchHandler = actions.WeaviateActionsPatchHandlerFunc(func(params actions.WeaviateActionsPatchParams, principal interface{}) middleware.Responder {
		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get and transform object
		UUID := strfmt.UUID(params.ActionID)
		errGet := dbConnector.GetAction(ctx, UUID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		actionGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Return error if UUID is not found.
		if errGet != nil {
			return actions.NewWeaviateActionsPatchNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionsPatchForbidden()
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
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), &action.ActionCreate, databaseSchema, dbConnector, serverConfig, principal.(*models.KeyTokenGetResponse))
		if validatedErr != nil {
			return actions.NewWeaviateActionsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		go dbConnector.MoveToHistoryAction(ctx, &oldAction.Action, params.ActionID, false)

		// Update the database
		go dbConnector.UpdateAction(ctx, action, UUID)

		// Create return Object
		actionGetResponse.Action = *action

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPatchAccepted().WithPayload(&actionGetResponse)
	})
	api.ActionsWeaviateActionsPropertiesCreateHandler = actions.WeaviateActionsPropertiesCreateHandlerFunc(func(params actions.WeaviateActionsPropertiesCreateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation actions.WeaviateActionsPropertiesCreate has not yet been implemented")
	})
	api.ActionsWeaviateActionsPropertiesDeleteHandler = actions.WeaviateActionsPropertiesDeleteHandlerFunc(func(params actions.WeaviateActionsPropertiesDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation actions.WeaviateActionsPropertiesDelete has not yet been implemented")
	})
	api.ActionsWeaviateActionsPropertiesUpdateHandler = actions.WeaviateActionsPropertiesUpdateHandlerFunc(func(params actions.WeaviateActionsPropertiesUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation actions.WeaviateActionsPropertiesUpdate has not yet been implemented")
	})
	api.ActionsWeaviateActionUpdateHandler = actions.WeaviateActionUpdateHandlerFunc(func(params actions.WeaviateActionUpdateParams, principal interface{}) middleware.Responder {
		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
		UUID := params.ActionID
		errGet := dbConnector.GetAction(ctx, UUID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		// If there are no results, there is an error
		if errGet != nil {
			// Object not found response.
			return actions.NewWeaviateActionUpdateNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionUpdateForbidden()
		}

		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), &params.Body.ActionCreate, databaseSchema, dbConnector, serverConfig, principal.(*models.KeyTokenGetResponse))
		if validatedErr != nil {
			return actions.NewWeaviateActionUpdateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		go dbConnector.MoveToHistoryAction(ctx, &oldAction.Action, params.ActionID, false)

		// Update the database
		params.Body.LastUpdateTimeUnix = connutils.NowUnix()
		params.Body.CreationTimeUnix = actionGetResponse.CreationTimeUnix
		params.Body.Key = actionGetResponse.Key
		go dbConnector.UpdateAction(ctx, &params.Body.Action, UUID)

		// Create object to return
		responseObject := &models.ActionGetResponse{}
		responseObject.Action = params.Body.Action
		responseObject.ActionID = UUID

		// broadcast to MQTT
		mqttJson, _ := json.Marshal(responseObject)
		weaviateBroker.Publish("/actions/"+string(responseObject.ActionID), string(mqttJson[:]))

		// Return SUCCESS (NOTE: this is ACCEPTED, so the dbConnector.Add should have a go routine)
		return actions.NewWeaviateActionUpdateAccepted().WithPayload(responseObject)
	})
	api.ActionsWeaviateActionsValidateHandler = actions.WeaviateActionsValidateHandlerFunc(func(params actions.WeaviateActionsValidateParams, principal interface{}) middleware.Responder {
		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateActionBody(ctx, &params.Body.ActionCreate, databaseSchema, dbConnector, serverConfig, principal.(*models.KeyTokenGetResponse))
		if validatedErr != nil {
			return actions.NewWeaviateActionsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		return actions.NewWeaviateActionsValidateOK()
	})
	api.ActionsWeaviateActionsCreateHandler = actions.WeaviateActionsCreateHandlerFunc(func(params actions.WeaviateActionsCreateParams, principal interface{}) middleware.Responder {
		// Get context from request
		ctx := params.HTTPRequest.Context()

		// This is a read function, validate if allowed to read?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, nil); !allowed {
			return actions.NewWeaviateActionsCreateForbidden()
		}

		// Generate UUID for the new object
		UUID := connutils.GenerateUUID()

		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), params.Body.Action, databaseSchema, dbConnector, serverConfig, principal.(*models.KeyTokenGetResponse))
		if validatedErr != nil {
			return actions.NewWeaviateActionsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Create Key-ref-Object
		url := serverConfig.GetHostAddress()
		keyRef := &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(*models.KeyTokenGetResponse).KeyID,
			Type:         string(connutils.RefTypeKey),
		}

		// Make Action-Object
		action := &models.Action{}
		action.AtClass = params.Body.Action.AtClass
		action.AtContext = params.Body.Action.AtContext
		action.Schema = params.Body.Action.Schema
		action.CreationTimeUnix = connutils.NowUnix()
		action.LastUpdateTimeUnix = 0
		action.Key = keyRef

		responseObject := &models.ActionGetResponse{}
		responseObject.Action = *action
		responseObject.ActionID = UUID

		if params.Body.Async {
			go dbConnector.AddAction(ctx, action, UUID)
			return actions.NewWeaviateActionsCreateAccepted().WithPayload(responseObject)
		} else {
			dbConnector.AddAction(ctx, action, UUID)
			return actions.NewWeaviateActionsCreateOK().WithPayload(responseObject)
		}
	})
	api.ActionsWeaviateActionsDeleteHandler = actions.WeaviateActionsDeleteHandlerFunc(func(params actions.WeaviateActionsDeleteParams, principal interface{}) middleware.Responder {
		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
		errGet := dbConnector.GetAction(ctx, params.ActionID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		// Not found
		if errGet != nil {
			return actions.NewWeaviateActionsDeleteNotFound()
		}

		// This is a delete function, validate if allowed to delete?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"delete"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsDeleteForbidden()
		}

		actionGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Move the current properties to the history
		go dbConnector.MoveToHistoryAction(ctx, &oldAction.Action, params.ActionID, false)

		// Add new row as GO-routine
		go dbConnector.DeleteAction(ctx, &actionGetResponse.Action, params.ActionID)

		// Return 'No Content'
		return actions.NewWeaviateActionsDeleteNoContent()
	})
}
