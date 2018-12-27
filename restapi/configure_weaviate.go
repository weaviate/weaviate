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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/creativesoftwarefdn/weaviate/restapi/operations/graphql"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/knowledge_tools"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/meta"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/p2_p"

	jsonpatch "github.com/evanphx/json-patch"
	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/rs/cors"
	"google.golang.org/grpc/grpclog"

	"github.com/creativesoftwarefdn/weaviate/auth"
	weaviateBroker "github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	dblisting "github.com/creativesoftwarefdn/weaviate/database/connectors/listing"
	connutils "github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	db_local_schema_manager "github.com/creativesoftwarefdn/weaviate/database/schema_manager/local"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	graphqlnetwork "github.com/creativesoftwarefdn/weaviate/graphqlapi/network"
	"github.com/creativesoftwarefdn/weaviate/lib/delayed_unlock"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/keys"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/things"
	rest_api_utils "github.com/creativesoftwarefdn/weaviate/restapi/rest_api_utils"
	"github.com/creativesoftwarefdn/weaviate/validation"

	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/graphiql"
	"github.com/creativesoftwarefdn/weaviate/lib/feature_flags"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	libnetworkFake "github.com/creativesoftwarefdn/weaviate/network/fake"
	libnetworkP2P "github.com/creativesoftwarefdn/weaviate/network/p2p"
	"github.com/creativesoftwarefdn/weaviate/restapi/swagger_middleware"
)

const pageOverride int = 1
const error422 string = "The request is well-formed but was unable to be followed due to semantic errors."

var connectorOptionGroup *swag.CommandLineOptionsGroup
var contextionary libcontextionary.Contextionary
var network libnetwork.Network
var serverConfig *config.WeaviateConfig
var graphQL graphqlapi.GraphQL
var messaging *messages.Messaging

var db database.Database

type dbAndNetwork struct {
	database.Database
	libnetwork.Network
}

func (d dbAndNetwork) GetNetworkResolver() graphqlnetwork.Resolver {
	return d.Network
}

type keyTokenHeader struct {
	Key   strfmt.UUID `json:"key"`
	Token strfmt.UUID `json:"token"`
}

func init() {
	discard := ioutil.Discard
	myGRPCLogger := log.New(discard, "", log.LstdFlags)
	grpclog.SetLogger(myGRPCLogger)

	// Create temp folder if it does not exist
	tempFolder := "temp"
	if _, err := os.Stat(tempFolder); os.IsNotExist(err) {
		messaging.InfoMessage("Temp folder created...")
		os.Mkdir(tempFolder, 0766)
	}
}

// getLimit returns the maximized limit
func getLimit(paramMaxResults *int64) int {
	maxResults := serverConfig.Environment.Limit
	// Get the max results from params, if exists
	if paramMaxResults != nil {
		maxResults = *paramMaxResults
	}

	// Max results form URL, otherwise max = config.Limit.
	return int(math.Min(float64(maxResults), float64(serverConfig.Environment.Limit)))
}

// getPage returns the page if set
func getPage(paramPage *int64) int {
	page := int64(pageOverride)
	// Get the page from params, if exists
	if paramPage != nil {
		page = *paramPage
	}

	// Page form URL, otherwise max = config.Limit.
	return int(page)
}

func generateMultipleRefObject(keyIDs []strfmt.UUID) models.MultipleRef {
	// Init the response
	refs := models.MultipleRef{}

	// Init localhost
	url := serverConfig.GetHostAddress()

	// Generate SingleRefs
	for _, keyID := range keyIDs {
		refs = append(refs, &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: keyID,
			Type:         string(connutils.RefTypeKey),
		})
	}

	return refs
}

func deleteKey(ctx context.Context, databaseConnector dbconnector.DatabaseConnector, parentUUID strfmt.UUID) {
	// Find its children
	var allIDs []strfmt.UUID

	// Get all the children to remove
	allIDs, _ = auth.GetKeyChildrenUUIDs(ctx, databaseConnector, parentUUID, false, allIDs, 0, 0)

	// Append the children to the parent UUIDs to remove all
	allIDs = append(allIDs, parentUUID)

	// Delete for every child
	for _, keyID := range allIDs {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get the key to delete
		databaseConnector.GetKey(ctx, keyID, &keyResponse)

		databaseConnector.DeleteKey(ctx, &keyResponse.Key, keyID)
	}
}

func configureFlags(api *operations.WeaviateAPI) {
	connectorOptionGroup = config.GetConfigOptionGroup()

	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		*connectorOptionGroup,
	}
}

// createErrorResponseObject is a common function to create an error response
func createErrorResponseObject(messages ...string) *models.ErrorResponse {
	// Initialize return value
	er := &models.ErrorResponse{}

	// appends all error messages to the error
	for _, message := range messages {
		er.Error = append(er.Error, &models.ErrorResponseErrorItems0{
			Message: message,
		})
	}

	return er
}

func headerAPIKeyHandling(ctx context.Context, keyToken string) (*models.KeyTokenGetResponse, error) {
	dbLock := db.ConnectorLock()
	defer dbLock.Unlock()
	dbConnector := dbLock.Connector()

	// Convert JSON string to struct
	kth := keyTokenHeader{}
	json.Unmarshal([]byte(keyToken), &kth)

	// Validate both headers
	if kth.Key == "" || kth.Token == "" {
		return nil, errors.New(401, connutils.StaticMissingHeader)
	}

	// Create key
	validatedKey := models.KeyGetResponse{}

	// Check if the user has access, true if yes
	hashed, err := dbConnector.ValidateToken(ctx, kth.Key, &validatedKey)

	// Error printing
	if err != nil {
		return nil, errors.New(401, err.Error())
	}

	// Check token
	if !connutils.TokenHashCompare(hashed, kth.Token) {
		return nil, errors.New(401, connutils.StaticInvalidToken)
	}

	// Validate the key on expiry time
	currentUnix := connutils.NowUnix()

	if validatedKey.KeyExpiresUnix != -1 && validatedKey.KeyExpiresUnix < currentUnix {
		return nil, errors.New(401, connutils.StaticKeyExpired)
	}

	// Init response object
	validatedKeyToken := models.KeyTokenGetResponse{}
	validatedKeyToken.KeyGetResponse = validatedKey
	validatedKeyToken.Token = kth.Token

	// key is valid, next step is allowing per Handler handling
	return &validatedKeyToken, nil
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	setupSchemaHandlers(api)

	/*
	 * HANDLE X-API-KEY
	 */
	// Applies when the "X-API-KEY" header is set
	api.APIKeyAuth = func(token string) (interface{}, error) {
		ctx := context.Background()
		return headerAPIKeyHandling(ctx, token)
	}

	/*
	 * HANDLE X-API-TOKEN
	 */
	// Applies when the "X-API-TOKEN" header is set
	api.APITokenAuth = func(token string) (interface{}, error) {
		ctx := context.Background()
		return headerAPIKeyHandling(ctx, token)
	}

	/*
	 * HANDLE EVENTS
	 */
	api.ActionsWeaviateActionsGetHandler = actions.WeaviateActionsGetHandlerFunc(func(params actions.WeaviateActionsGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

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
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

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
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

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
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), &action.ActionCreate,
			databaseSchema, dbConnector, network, serverConfig, principal.(*models.KeyTokenGetResponse))
		if validatedErr != nil {
			return actions.NewWeaviateActionsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
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

			// Returns accepted so a Go routine can process in the background
			return actions.NewWeaviateActionsPatchOK().WithPayload(&actionGetResponse)
		}
	})
	api.ActionsWeaviateActionsPropertiesCreateHandler = actions.WeaviateActionsPropertiesCreateHandlerFunc(func(params actions.WeaviateActionsPropertiesCreateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		ctx := params.HTTPRequest.Context()

		UUID := strfmt.UUID(params.ActionID)

		class := models.ActionGetResponse{}
		err := dbConnector.GetAction(ctx, UUID, &class)

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, class.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionsPatchForbidden()
		}

		// Look up the single ref.
		err = validation.ValidateSingleRef(ctx, serverConfig, params.Body, dbConnector, network,
			"reference not found", principal.(*models.KeyTokenGetResponse))
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

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPropertiesCreateOK()
	})
	api.ActionsWeaviateActionsPropertiesDeleteHandler = actions.WeaviateActionsPropertiesDeleteHandlerFunc(func(params actions.WeaviateActionsPropertiesDeleteParams, principal interface{}) middleware.Responder {
		if params.Body == nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a no valid reference", params.PropertyName)))
		}

		// Delete a specific SingleRef from the selected property.
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		ctx := params.HTTPRequest.Context()

		UUID := strfmt.UUID(params.ActionID)

		class := models.ActionGetResponse{}
		err := dbConnector.GetAction(ctx, UUID, &class)

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, class.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionsPatchForbidden()
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
		locationUrl := string(*params.Body.LocationURL)
		bodyType := string(params.Body.Type)

		// Remove if this reference is found.
		for idx, schemaPropItem := range schemaPropList {
			schemaRef := schemaPropItem.(map[string]interface{})

			if schemaRef["$cref"].(string) != crefStr {
				continue
			}

			if schemaRef["locationUrl"].(string) != locationUrl {
				continue
			}

			if schemaRef["type"].(string) != bodyType {
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

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPropertiesDeleteNoContent()
	})
	api.ActionsWeaviateActionsPropertiesUpdateHandler = actions.WeaviateActionsPropertiesUpdateHandlerFunc(func(params actions.WeaviateActionsPropertiesUpdateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		ctx := params.HTTPRequest.Context()

		UUID := strfmt.UUID(params.ActionID)

		class := models.ActionGetResponse{}
		err := dbConnector.GetAction(ctx, UUID, &class)

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, class.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionsPatchForbidden()
		}

		// Look up the single ref.
		err = validation.ValidateMultipleRef(ctx, serverConfig, &params.Body, dbConnector, network,
			"reference not found", principal.(*models.KeyTokenGetResponse))
		if err != nil {
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(err.Error()))
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
			return actions.NewWeaviateActionsPropertiesCreateUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
		}

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPropertiesCreateOK()
	})
	api.ActionsWeaviateActionUpdateHandler = actions.WeaviateActionUpdateHandlerFunc(func(params actions.WeaviateActionUpdateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()
		dbConnector := dbLock.Connector()

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
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), &params.Body.ActionCreate,
			databaseSchema, dbConnector, network, serverConfig, principal.(*models.KeyTokenGetResponse))
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
		params.Body.Key = actionGetResponse.Key

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

		// Return SUCCESS (NOTE: this is ACCEPTED, so the dbConnector.Add should have a go routine)
		return actions.NewWeaviateActionUpdateAccepted().WithPayload(responseObject)
	})
	api.ActionsWeaviateActionsValidateHandler = actions.WeaviateActionsValidateHandlerFunc(func(params actions.WeaviateActionsValidateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateActionBody(ctx, &params.Body.ActionCreate, databaseSchema,
			dbConnector, network, serverConfig, principal.(*models.KeyTokenGetResponse))
		if validatedErr != nil {
			return actions.NewWeaviateActionsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		return actions.NewWeaviateActionsValidateOK()
	})
	api.ActionsWeaviateActionsCreateHandler = actions.WeaviateActionsCreateHandlerFunc(func(params actions.WeaviateActionsCreateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()
		dbConnector := dbLock.Connector()

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// This is a read function, validate if allowed to read?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, nil); !allowed {
			return actions.NewWeaviateActionsCreateForbidden()
		}

		// Generate UUID for the new object
		UUID := connutils.GenerateUUID()

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateActionBody(params.HTTPRequest.Context(), params.Body.Action,
			databaseSchema, dbConnector, network, serverConfig, principal.(*models.KeyTokenGetResponse))
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
			delayedLock.IncSteps()
			go func() {
				defer delayedLock.Unlock()
				dbConnector.AddAction(ctx, action, UUID)
			}()
			return actions.NewWeaviateActionsCreateAccepted().WithPayload(responseObject)
		} else {
			//TODO gh-617: handle errors
			err := dbConnector.AddAction(ctx, action, UUID)
			if err != nil {
				panic(err)
			}
			return actions.NewWeaviateActionsCreateOK().WithPayload(responseObject)
		}
	})
	api.ActionsWeaviateActionsDeleteHandler = actions.WeaviateActionsDeleteHandlerFunc(func(params actions.WeaviateActionsDeleteParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

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

		// Return 'No Content'
		return actions.NewWeaviateActionsDeleteNoContent()
	})

	/*
	 * HANDLE KEYS
	 */
	api.KeysWeaviateKeyCreateHandler = keys.WeaviateKeyCreateHandlerFunc(func(params keys.WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Create current User object from principal
		key := principal.(*models.KeyTokenGetResponse)

		// Fill the new User object
		url := serverConfig.GetHostAddress()
		newKey := &models.KeyTokenGetResponse{}
		newKey.KeyID = connutils.GenerateUUID()
		newKey.Token = connutils.GenerateUUID()
		newKey.Parent = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(*models.KeyTokenGetResponse).KeyID,
			Type:         string(connutils.RefTypeKey),
		}
		newKey.KeyCreate = *params.Body

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Key expiry time is in the past
		currentUnix := connutils.NowUnix()
		if newKey.KeyExpiresUnix != -1 && newKey.KeyExpiresUnix < currentUnix {
			return keys.NewWeaviateKeyCreateUnprocessableEntity().WithPayload(createErrorResponseObject("Key expiry time is in the past."))
		}

		// Key expiry time is later than the expiry time of parent
		if key.KeyExpiresUnix != -1 && key.KeyExpiresUnix < newKey.KeyExpiresUnix {
			return keys.NewWeaviateKeyCreateUnprocessableEntity().WithPayload(createErrorResponseObject("Key expiry time is later than the expiry time of parent."))
		}

		// Save to DB
		insertErr := dbConnector.AddKey(ctx, &newKey.Key, newKey.KeyID, connutils.TokenHasher(newKey.Token))
		if insertErr != nil {
			messaging.ErrorMessage(insertErr)
			return keys.NewWeaviateKeyCreateUnprocessableEntity().WithPayload(createErrorResponseObject(insertErr.Error()))
		}

		// Return SUCCESS
		return keys.NewWeaviateKeyCreateOK().WithPayload(newKey)

	})
	api.KeysWeaviateKeysChildrenGetHandler = keys.WeaviateKeysChildrenGetHandlerFunc(func(params keys.WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysChildrenGetNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysChildrenGetForbidden()
		}

		// Get the children
		childIDs := []strfmt.UUID{}
		childIDs, _ = auth.GetKeyChildrenUUIDs(ctx, dbConnector, params.KeyID, true, childIDs, 1, 0)

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = generateMultipleRefObject(childIDs)

		// Return children with 'OK'
		return keys.NewWeaviateKeysChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysDeleteHandler = keys.WeaviateKeysDeleteHandlerFunc(func(params keys.WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysDeleteNotFound()
		}

		// Check on permissions, only delete allowed if lower in tree (not own key)
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) || keyObject.KeyID == params.KeyID {
			return keys.NewWeaviateKeysDeleteForbidden()
		}

		// Remove key from database if found
		deleteKey(ctx, dbConnector, params.KeyID)

		// Return 'No Content'
		return keys.NewWeaviateKeysDeleteNoContent()
	})
	api.KeysWeaviateKeysGetHandler = keys.WeaviateKeysGetHandlerFunc(func(params keys.WeaviateKeysGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
		err := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Object is deleted or not-existing
		if err != nil {
			return keys.NewWeaviateKeysGetNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysGetForbidden()
		}

		// Get is successful
		return keys.NewWeaviateKeysGetOK().WithPayload(&keyResponse)
	})
	api.KeysWeaviateKeysMeChildrenGetHandler = keys.WeaviateKeysMeChildrenGetHandlerFunc(func(params keys.WeaviateKeysMeChildrenGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		currentKey := principal.(*models.KeyTokenGetResponse)

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get the children
		childIDs := []strfmt.UUID{}
		childIDs, _ = auth.GetKeyChildrenUUIDs(ctx, dbConnector, currentKey.KeyID, true, childIDs, 1, 0)

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = generateMultipleRefObject(childIDs)

		// Return children with 'OK'
		return keys.NewWeaviateKeysMeChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysMeGetHandler = keys.WeaviateKeysMeGetHandlerFunc(func(params keys.WeaviateKeysMeGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response object
		responseObject := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		isRoot := false
		responseObject.Key.IsRoot = &isRoot

		// Get item from database
		err := dbConnector.GetKey(ctx, principal.(*models.KeyTokenGetResponse).KeyID, &responseObject)

		// Object is deleted or not-existing
		if err != nil {
			return keys.NewWeaviateKeysGetNotFound()
		}

		// Get is successful
		return keys.NewWeaviateKeysMeGetOK().WithPayload(&responseObject)
	})
	api.KeysWeaviateKeysRenewTokenHandler = keys.WeaviateKeysRenewTokenHandlerFunc(func(params keys.WeaviateKeysRenewTokenParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(ctx, params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysRenewTokenNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyTokenGetResponse)
		if !auth.IsOwnKeyOrLowerInTree(ctx, keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysRenewTokenForbidden()
		}

		// Can't renew own unless root
		if keyObject.KeyID == params.KeyID && keyObject.Parent != nil {
			return keys.NewWeaviateKeysRenewTokenForbidden()
		}

		// Generate new token
		newToken := connutils.GenerateUUID()

		// Update the key in the database
		insertErr := dbConnector.UpdateKey(ctx, &keyResponse.Key, keyResponse.KeyID, connutils.TokenHasher(newToken))
		if insertErr != nil {
			messaging.ErrorMessage(insertErr)
			return keys.NewWeaviateKeysRenewTokenUnprocessableEntity().WithPayload(createErrorResponseObject(insertErr.Error()))
		}

		// Build new token response object
		renewObject := &models.KeyTokenGetResponse{}
		renewObject.KeyGetResponse = keyResponse
		renewObject.Token = newToken

		return keys.NewWeaviateKeysRenewTokenOK().WithPayload(renewObject)
	})

	/*
	 * HANDLE THINGS
	 */
	api.ThingsWeaviateThingsCreateHandler = things.WeaviateThingsCreateHandlerFunc(func(params things.WeaviateThingsCreateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, nil); !allowed {
			return things.NewWeaviateThingsCreateForbidden()
		}

		// Generate UUID for the new object
		UUID := connutils.GenerateUUID()

		// Convert principal to object
		keyToken := principal.(*models.KeyTokenGetResponse)

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), params.Body.Thing, databaseSchema,
			dbConnector, network, serverConfig, keyToken)
		if validatedErr != nil {
			return things.NewWeaviateThingsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Create Key-ref-Object
		url := serverConfig.GetHostAddress()
		keyRef := &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: keyToken.KeyID,
			Type:         string(connutils.RefTypeKey),
		}

		// Make Thing-Object
		thing := &models.Thing{}
		thing.Schema = params.Body.Thing.Schema
		thing.AtClass = params.Body.Thing.AtClass
		thing.AtContext = params.Body.Thing.AtContext
		thing.CreationTimeUnix = connutils.NowUnix()
		thing.LastUpdateTimeUnix = 0
		thing.Key = keyRef

		responseObject := &models.ThingGetResponse{}
		responseObject.Thing = *thing
		responseObject.ThingID = UUID

		if params.Body.Async {
			delayedLock.IncSteps()
			go func() {
				defer delayedLock.Unlock()
				dbConnector.AddThing(ctx, thing, UUID)
			}()
			return things.NewWeaviateThingsCreateAccepted().WithPayload(responseObject)
		} else {
			dbConnector.AddThing(ctx, thing, UUID)
			return things.NewWeaviateThingsCreateOK().WithPayload(responseObject)
		}
	})
	api.ThingsWeaviateThingsDeleteHandler = things.WeaviateThingsDeleteHandlerFunc(func(params things.WeaviateThingsDeleteParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
		errGet := dbConnector.GetThing(params.HTTPRequest.Context(), params.ThingID, &thingGetResponse)

		// Save the old-thing in a variable
		oldThing := thingGetResponse

		// Not found
		if errGet != nil {
			return things.NewWeaviateThingsDeleteNotFound()
		}

		// This is a delete function, validate if allowed to delete?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"delete"}, principal, dbConnector, thingGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsDeleteForbidden()
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
	api.ThingsWeaviateThingsGetHandler = things.WeaviateThingsGetHandlerFunc(func(params things.WeaviateThingsGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Initialize response
		responseObject := models.ThingGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// Get item from database
		err := dbConnector.GetThing(ctx, strfmt.UUID(params.ThingID), &responseObject)

		// Object is not found
		if err != nil || responseObject.Key == nil {
			messaging.ErrorMessage(err)
			return things.NewWeaviateThingsGetNotFound()
		}

		// This is a read function, validate if allowed to read?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, responseObject.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsGetForbidden()
		}

		// Get is successful
		return things.NewWeaviateThingsGetOK().WithPayload(&responseObject)
	})

	api.ThingsWeaviateThingHistoryGetHandler = things.WeaviateThingHistoryGetHandlerFunc(func(params things.WeaviateThingHistoryGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Initialize response
		responseObject := models.ThingGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

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

		// This is a read function, validate if allowed to read?
		if errHist == nil {
			if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, historyResponse.Key.NrDollarCref); !allowed {
				return things.NewWeaviateThingHistoryGetForbidden()
			}
		} else if errGet == nil {
			if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, responseObject.Key.NrDollarCref); !allowed {
				return things.NewWeaviateThingHistoryGetForbidden()
			}
		}

		// Thing is deleted when we have an get error and no history error
		historyResponse.Deleted = errGet != nil && errHist == nil && len(historyResponse.PropertyHistory) != 0

		return things.NewWeaviateThingHistoryGetOK().WithPayload(historyResponse)
	})

	api.ThingsWeaviateThingsListHandler = things.WeaviateThingsListHandlerFunc(func(params things.WeaviateThingsListParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// Get user out of principal
		keyID := principal.(*models.KeyTokenGetResponse).KeyID

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// This is a read function, validate if allowed to read?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, keyID); !allowed {
			return things.NewWeaviateThingsListForbidden()
		}

		// Initialize response
		thingsResponse := models.ThingsListResponse{}
		thingsResponse.Things = []*models.ThingGetResponse{}

		// List all results
		err := dbConnector.ListThings(ctx, limit, (page-1)*limit, keyID, []*connutils.WhereQuery{}, &thingsResponse)

		if err != nil {
			messaging.ErrorMessage(err)
		}

		return things.NewWeaviateThingsListOK().WithPayload(&thingsResponse)
	})
	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, thingGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsPatchForbidden()
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

		// Convert principal to object
		keyToken := principal.(*models.KeyTokenGetResponse)

		// Validate schema made after patching with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), &thing.ThingCreate,
			databaseSchema, dbConnector, network, serverConfig, keyToken)
		if validatedErr != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		go func() {
			schemaLock := db.SchemaLock()
			defer schemaLock.Unlock()

			err := newReferenceSchemaUpdater(schemaLock.SchemaManager(), network, thing.AtClass, kind.THING_KIND).
				addNetworkDataTypes(thing.Schema)
			if err != nil {
				messaging.DebugMessage(fmt.Sprintf("Async network ref update failed: %s", err.Error()))
			}
		}()

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
		} else {
			// Move the current properties to the history
			dbConnector.MoveToHistoryThing(ctx, &oldThing.Thing, UUID, false)

			// Update the database
			err := dbConnector.UpdateThing(ctx, thing, UUID)

			if err != nil {
				return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(err.Error()))
			}

			// Create return Object
			thingGetResponse.Thing = *thing

			// Returns accepted so a Go routine can process in the background
			return things.NewWeaviateThingsPatchOK().WithPayload(&thingGetResponse)
		}
	})
	api.ThingsWeaviateThingsPropertiesCreateHandler = things.WeaviateThingsPropertiesCreateHandlerFunc(func(params things.WeaviateThingsPropertiesCreateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		ctx := params.HTTPRequest.Context()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		err := dbConnector.GetThing(ctx, UUID, &class)

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, class.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsPatchForbidden()
		}

		// Look up the single ref.
		err = validation.ValidateSingleRef(ctx, serverConfig, params.Body, dbConnector, network,
			"reference not found", principal.(*models.KeyTokenGetResponse))
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
	api.ThingsWeaviateThingsPropertiesDeleteHandler = things.WeaviateThingsPropertiesDeleteHandlerFunc(func(params things.WeaviateThingsPropertiesDeleteParams, principal interface{}) middleware.Responder {
		if params.Body == nil {
			return things.NewWeaviateThingsPropertiesCreateUnprocessableEntity().
				WithPayload(createErrorResponseObject(fmt.Sprintf("Property '%s' has a no valid reference", params.PropertyName)))
		}

		// Delete a specific SingleRef from the selected property.
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		ctx := params.HTTPRequest.Context()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		err := dbConnector.GetThing(ctx, UUID, &class)

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, class.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsPatchForbidden()
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
		locationUrl := string(*params.Body.LocationURL)
		bodyType := string(params.Body.Type)

		// Remove if this reference is found.
		for idx, schemaPropItem := range schemaPropList {
			schemaRef := schemaPropItem.(map[string]interface{})

			if schemaRef["$cref"].(string) != crefStr {
				continue
			}

			if schemaRef["locationUrl"].(string) != locationUrl {
				continue
			}

			if schemaRef["type"].(string) != bodyType {
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
	api.ThingsWeaviateThingsPropertiesUpdateHandler = things.WeaviateThingsPropertiesUpdateHandlerFunc(func(params things.WeaviateThingsPropertiesUpdateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		ctx := params.HTTPRequest.Context()

		UUID := strfmt.UUID(params.ThingID)

		class := models.ThingGetResponse{}
		err := dbConnector.GetThing(ctx, UUID, &class)

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, class.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsPatchForbidden()
		}

		// Look up the single ref.
		err = validation.ValidateMultipleRef(ctx, serverConfig, &params.Body, dbConnector, network,
			"reference not found", principal.(*models.KeyTokenGetResponse))
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
	api.ThingsWeaviateThingsUpdateHandler = things.WeaviateThingsUpdateHandlerFunc(func(params things.WeaviateThingsUpdateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		delayedLock := delayed_unlock.New(dbLock)
		defer delayedLock.Unlock()

		dbConnector := dbLock.Connector()

		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get context from request
		ctx := params.HTTPRequest.Context()

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

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, dbConnector, thingGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsUpdateForbidden()
		}

		// Convert principal to object
		keyToken := principal.(*models.KeyTokenGetResponse)

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), &params.Body.ThingCreate,
			databaseSchema, dbConnector, network, serverConfig, keyToken)
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
	api.ThingsWeaviateThingsValidateHandler = things.WeaviateThingsValidateHandlerFunc(func(params things.WeaviateThingsValidateParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		dbConnector := dbLock.Connector()

		// Convert principal to object
		keyToken := principal.(*models.KeyTokenGetResponse)

		// Validate schema given in body with the weaviate schema
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		validatedErr := validation.ValidateThingBody(params.HTTPRequest.Context(), params.Body, databaseSchema,
			dbConnector, network, serverConfig, keyToken)
		if validatedErr != nil {
			return things.NewWeaviateThingsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		return things.NewWeaviateThingsValidateOK()
	})
	api.MetaWeaviateMetaGetHandler = meta.WeaviateMetaGetHandlerFunc(func(params meta.WeaviateMetaGetParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()
		databaseSchema := schema.HackFromDatabaseSchema(dbLock.GetSchema())
		// Create response object
		metaResponse := &models.Meta{}

		// Set the response object's values
		metaResponse.Hostname = serverConfig.GetHostAddress()
		metaResponse.ActionsSchema = databaseSchema.ActionSchema.Schema
		metaResponse.ThingsSchema = databaseSchema.ThingSchema.Schema

		return meta.NewWeaviateMetaGetOK().WithPayload(metaResponse)
	})

	api.P2PWeaviateP2pGenesisUpdateHandler = p2_p.WeaviateP2pGenesisUpdateHandlerFunc(func(params p2_p.WeaviateP2pGenesisUpdateParams) middleware.Responder {
		newPeers := make([]peers.Peer, 0)

		for _, genesisPeer := range params.Peers {
			peer := peers.Peer{
				ID:         genesisPeer.ID,
				Name:       genesisPeer.Name,
				URI:        genesisPeer.URI,
				SchemaHash: genesisPeer.SchemaHash,
			}

			newPeers = append(newPeers, peer)
		}

		err := network.UpdatePeers(newPeers)

		if err == nil {
			return p2_p.NewWeaviateP2pGenesisUpdateOK()
		}
		return p2_p.NewWeaviateP2pGenesisUpdateInternalServerError()
	})

	api.P2PWeaviateP2pHealthHandler = p2_p.WeaviateP2pHealthHandlerFunc(func(params p2_p.WeaviateP2pHealthParams) middleware.Responder {
		// For now, always just return success.
		return middleware.NotImplemented("operation P2PWeaviateP2pHealth has not yet been implemented")
	})

	api.ActionsWeaviateActionsListHandler = actions.WeaviateActionsListHandlerFunc(func(params actions.WeaviateActionsListParams, principal interface{}) middleware.Responder {
		dbLock := db.ConnectorLock()
		defer dbLock.Unlock()

		dbConnector := dbLock.Connector()

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// Get key-object
		keyObject := principal.(*models.KeyTokenGetResponse)

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// This is a read function, validate if allowed to read?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"read"}, principal, dbConnector, keyObject.KeyID); !allowed {
			return actions.NewWeaviateActionsListForbidden()
		}

		// Initialize response
		actionsResponse := models.ActionsListResponse{}
		actionsResponse.Actions = []*models.ActionGetResponse{}

		// List all results
		err := dbConnector.ListActions(ctx, limit, (page-1)*limit, keyObject.KeyID, []*connutils.WhereQuery{}, &actionsResponse)

		if err != nil {
			messaging.ErrorMessage(err)
		}

		return actions.NewWeaviateActionsListOK().WithPayload(&actionsResponse)
	})
	api.GraphqlWeaviateGraphqlPostHandler = graphql.WeaviateGraphqlPostHandlerFunc(func(params graphql.WeaviateGraphqlPostParams, principal interface{}) middleware.Responder {
		defer messaging.TimeTrack(time.Now())
		messaging.DebugMessage("Starting GraphQL resolving")

		errorResponse := &models.ErrorResponse{}

		// Get all input from the body of the request, as it is a POST.
		query := params.Body.Query
		operationName := params.Body.OperationName

		// If query is empty, the request is unprocessable
		if query == "" {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Only set variables if exists in request
		var variables map[string]interface{}
		if params.Body.Variables != nil {
			variables = params.Body.Variables.(map[string]interface{})
		}

		// Add security principal to context that we pass on to the GraphQL resolver.
		graphql_context := context.WithValue(params.HTTPRequest.Context(), "principal", (principal.(*models.KeyTokenGetResponse)))

		if graphQL == nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: "no graphql provider present, " +
						"this is most likely because no schema is present. Import a schema first!",
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		result := graphQL.Resolve(query, operationName, variables, graphql_context)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)

		// If json gave error, return nothing.
		if jsonErr != nil {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Put the data in a response ready object
		graphQLResponse := &models.GraphQLResponse{}
		marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

		// If json gave error, return nothing.
		if marshallErr != nil {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Return the response
		return graphql.NewWeaviateGraphqlPostOK().WithPayload(graphQLResponse)
	})

	/*
	 * HANDLE BATCHING
	 */

	api.GraphqlWeaviateGraphqlBatchHandler = graphql.WeaviateGraphqlBatchHandlerFunc(func(params graphql.WeaviateGraphqlBatchParams, principal interface{}) middleware.Responder {
		defer messaging.TimeTrack(time.Now())
		messaging.DebugMessage("Starting GraphQL batch resolving")

		// Add security principal to context that we pass on to the GraphQL resolver.
		graphql_context := context.WithValue(params.HTTPRequest.Context(), "principal", (principal.(*models.KeyTokenGetResponse)))

		amountOfBatchedRequests := len(params.Body)
		errorResponse := &models.ErrorResponse{}

		if amountOfBatchedRequests == 0 {
			return graphql.NewWeaviateGraphqlBatchUnprocessableEntity().WithPayload(errorResponse)
		}
		requestResults := make(chan rest_api_utils.UnbatchedRequestResponse, amountOfBatchedRequests)

		wg := new(sync.WaitGroup)

		// Generate a goroutine for each separate request
		for requestIndex, unbatchedRequest := range params.Body {
			wg.Add(1)
			go handleUnbatchedGraphQLRequest(wg, graphql_context, unbatchedRequest, requestIndex, &requestResults)
		}

		wg.Wait()

		close(requestResults)

		batchedRequestResponse := make([]*models.GraphQLResponse, amountOfBatchedRequests)

		// Add the requests to the result array in the correct order
		for unbatchedRequestResult := range requestResults {
			batchedRequestResponse[unbatchedRequestResult.RequestIndex] = unbatchedRequestResult.Response
		}

		return graphql.NewWeaviateGraphqlBatchOK().WithPayload(batchedRequestResponse)
	})

	api.WeaviateBatchingActionsCreateHandler = operations.WeaviateBatchingActionsCreateHandlerFunc(func(params operations.WeaviateBatchingActionsCreateParams, principal interface{}) middleware.Responder {
		defer messaging.TimeTrack(time.Now())

		dbLock := db.ConnectorLock()
		requestLocks := rest_api_utils.RequestLocks{
			DBLock:      dbLock,
			DelayedLock: delayed_unlock.New(dbLock),
			DBConnector: dbLock.Connector(),
		}

		defer requestLocks.DelayedLock.Unlock()

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, requestLocks.DBConnector, nil); !allowed {
			return operations.NewWeaviateBatchingActionsCreateForbidden()
		}

		amountOfBatchedRequests := len(params.Body.Actions)
		errorResponse := &models.ErrorResponse{}

		if amountOfBatchedRequests == 0 {
			return operations.NewWeaviateBatchingActionsCreateUnprocessableEntity().WithPayload(errorResponse)
		}

		isThingsCreate := false
		fieldsToKeep := determineResponseFields(params.Body.Fields, isThingsCreate)

		requestResults := make(chan rest_api_utils.BatchedActionsCreateRequestResponse, amountOfBatchedRequests)

		wg := new(sync.WaitGroup)

		async := params.Body.Async

		// Generate a goroutine for each separate request
		for requestIndex, batchedRequest := range params.Body.Actions {
			wg.Add(1)
			go handleBatchedActionsCreateRequest(wg, ctx, batchedRequest, requestIndex, &requestResults, async, principal, &requestLocks, fieldsToKeep)
		}

		wg.Wait()

		close(requestResults)

		batchedRequestResponse := make([]*models.ActionsGetResponse, amountOfBatchedRequests)

		// Add the requests to the result array in the correct order
		for batchedRequestResult := range requestResults {
			batchedRequestResponse[batchedRequestResult.RequestIndex] = batchedRequestResult.Response
		}

		return operations.NewWeaviateBatchingActionsCreateOK().WithPayload(batchedRequestResponse)

	})

	api.WeaviateBatchingThingsCreateHandler = operations.WeaviateBatchingThingsCreateHandlerFunc(func(params operations.WeaviateBatchingThingsCreateParams, principal interface{}) middleware.Responder {
		defer messaging.TimeTrack(time.Now())

		dbLock := db.ConnectorLock()
		requestLocks := rest_api_utils.RequestLocks{
			DBLock:      dbLock,
			DelayedLock: delayed_unlock.New(dbLock),
			DBConnector: dbLock.Connector(),
		}

		defer requestLocks.DelayedLock.Unlock()

		// Get context from request
		ctx := params.HTTPRequest.Context()

		// This is a write function, validate if allowed to write?
		if allowed, _ := auth.ActionsAllowed(ctx, []string{"write"}, principal, requestLocks.DBConnector, nil); !allowed {
			return operations.NewWeaviateBatchingThingsCreateForbidden()
		}

		amountOfBatchedRequests := len(params.Body.Things)
		errorResponse := &models.ErrorResponse{}

		if amountOfBatchedRequests == 0 {
			return operations.NewWeaviateBatchingThingsCreateUnprocessableEntity().WithPayload(errorResponse)
		}

		isThingsCreate := true
		fieldsToKeep := determineResponseFields(params.Body.Fields, isThingsCreate)

		requestResults := make(chan rest_api_utils.BatchedThingsCreateRequestResponse, amountOfBatchedRequests)

		wg := new(sync.WaitGroup)

		async := params.Body.Async

		// Generate a goroutine for each separate request
		for requestIndex, batchedRequest := range params.Body.Things {
			wg.Add(1)
			go handleBatchedThingsCreateRequest(wg, ctx, batchedRequest, requestIndex, &requestResults, async, principal, &requestLocks, fieldsToKeep)
		}

		wg.Wait()

		close(requestResults)

		batchedRequestResponse := make([]*models.ThingsGetResponse, amountOfBatchedRequests)

		// Add the requests to the result array in the correct order
		for batchedRequestResult := range requestResults {
			batchedRequestResponse[batchedRequestResult.RequestIndex] = batchedRequestResult.Response
		}

		return operations.NewWeaviateBatchingThingsCreateOK().WithPayload(batchedRequestResponse)
	})

	/*
	 * HANDLE SCHEMA: NOTE, CAN BE FOUND IN /DATABASE
	 */

	/*
	 * HANDLE KNOWLEDGE TOOLS
	 */
	api.KnowledgeToolsWeaviateToolsMapHandler = knowledge_tools.WeaviateToolsMapHandlerFunc(func(params knowledge_tools.WeaviateToolsMapParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation knowledge_tools.WeaviateToolsMap has not yet been implemented")
	})

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// Handle a single unbatched GraphQL request, return a tuple containing the index of the request in the batch and either the response or an error
func handleUnbatchedGraphQLRequest(wg *sync.WaitGroup, ctx context.Context, unbatchedRequest *models.GraphQLQuery, requestIndex int, requestResults *chan rest_api_utils.UnbatchedRequestResponse) {
	defer wg.Done()

	// Get all input from the body of the request
	query := unbatchedRequest.Query
	operationName := unbatchedRequest.OperationName
	graphQLResponse := &models.GraphQLResponse{}

	// Return an unprocessable error if the query is empty
	if query == "" {

		// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
		errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
		errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
		errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
		graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
		*requestResults <- rest_api_utils.UnbatchedRequestResponse{
			requestIndex,
			&graphQLResponse,
		}
	} else {

		// Extract any variables from the request
		var variables map[string]interface{}
		if unbatchedRequest.Variables != nil {
			variables = unbatchedRequest.Variables.(map[string]interface{})
		}

		if graphQL == nil {
			panic("graphql is nil!")
		}
		result := graphQL.Resolve(query, operationName, variables, ctx)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)

		// Return an unprocessable error if marshalling the result to JSON failed
		if jsonErr != nil {

			// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
			errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
			errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
			errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
			graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
			*requestResults <- rest_api_utils.UnbatchedRequestResponse{
				requestIndex,
				&graphQLResponse,
			}
		} else {

			// Put the result data in a response ready object
			marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

			// Return an unprocessable error if unmarshalling the result to JSON failed
			if marshallErr != nil {

				// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
				errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
				errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
				errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
				graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
				*requestResults <- rest_api_utils.UnbatchedRequestResponse{
					requestIndex,
					&graphQLResponse,
				}
			} else {

				// Return the GraphQL response
				*requestResults <- rest_api_utils.UnbatchedRequestResponse{
					requestIndex,
					graphQLResponse,
				}
			}
		}
	}

}

func handleBatchedActionsCreateRequest(wg *sync.WaitGroup, ctx context.Context, batchedRequest *models.ActionCreate, requestIndex int, requestResults *chan rest_api_utils.BatchedActionsCreateRequestResponse, async bool, principal interface{}, requestLocks *rest_api_utils.RequestLocks, fieldsToKeep map[string]int) {
	defer wg.Done()

	// Generate UUID for the new object
	UUID := connutils.GenerateUUID()

	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(requestLocks.DBLock.GetSchema())

	// Create Key-ref object
	url := serverConfig.GetHostAddress()
	keyRef := &models.SingleRef{
		LocationURL:  &url,
		NrDollarCref: principal.(*models.KeyTokenGetResponse).KeyID,
		Type:         string(connutils.RefTypeKey),
	}

	// Create Action object
	action := &models.Action{}
	action.AtContext = batchedRequest.AtContext
	action.LastUpdateTimeUnix = 0

	if _, ok := fieldsToKeep["@class"]; ok {
		action.AtClass = batchedRequest.AtClass
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		action.Schema = batchedRequest.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		action.CreationTimeUnix = connutils.NowUnix()
	}
	if _, ok := fieldsToKeep["key"]; ok {
		action.Key = keyRef
	}

	// Create request result object
	result := &models.ActionsGetResponseAO1Result{}
	result.Errors = nil

	// Create request response object
	responseObject := &models.ActionsGetResponse{}
	responseObject.Action = *action
	if _, ok := fieldsToKeep["actionid"]; ok {
		responseObject.ActionID = UUID
	}
	responseObject.Result = result

	resultStatus := models.ActionsGetResponseAO1ResultStatusSUCCESS

	validatedErr := validation.ValidateActionBody(ctx, batchedRequest, databaseSchema, requestLocks.DBConnector,
		network, serverConfig, principal.(*models.KeyTokenGetResponse))

	if validatedErr != nil {
		// Edit request result status
		responseObject.Result.Errors = createErrorResponseObject(validatedErr.Error())
		resultStatus = models.ActionsGetResponseAO1ResultStatusFAILED
		responseObject.Result.Status = &resultStatus
	} else {
		// Handle asynchronous requests
		if async {
			requestLocks.DelayedLock.IncSteps()
			resultStatus = models.ActionsGetResponseAO1ResultStatusPENDING
			responseObject.Result.Status = &resultStatus

			go func() {
				defer requestLocks.DelayedLock.Unlock()
				err := requestLocks.DBConnector.AddAction(ctx, action, UUID)

				if err != nil {
					// Edit request result status
					resultStatus = models.ActionsGetResponseAO1ResultStatusFAILED
					responseObject.Result.Status = &resultStatus
					responseObject.Result.Errors = createErrorResponseObject(err.Error())
				}
			}()
		} else {
			// Handle synchronous requests
			err := requestLocks.DBConnector.AddAction(ctx, action, UUID)

			if err != nil {
				// Edit request result status
				resultStatus = models.ActionsGetResponseAO1ResultStatusFAILED
				responseObject.Result.Status = &resultStatus
				responseObject.Result.Errors = createErrorResponseObject(err.Error())
			}
		}
	}

	// Send this batched request's response and its place in the batch request to the channel
	*requestResults <- rest_api_utils.BatchedActionsCreateRequestResponse{
		requestIndex,
		responseObject,
	}
}

func handleBatchedThingsCreateRequest(wg *sync.WaitGroup, ctx context.Context, batchedRequest *models.ThingCreate, requestIndex int, requestResults *chan rest_api_utils.BatchedThingsCreateRequestResponse, async bool, principal interface{}, requestLocks *rest_api_utils.RequestLocks, fieldsToKeep map[string]int) {
	defer wg.Done()

	// Generate UUID for the new object
	UUID := connutils.GenerateUUID()

	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(requestLocks.DBLock.GetSchema())

	// Create Key-ref object
	url := serverConfig.GetHostAddress()
	keyRef := &models.SingleRef{
		LocationURL:  &url,
		NrDollarCref: principal.(*models.KeyTokenGetResponse).KeyID,
		Type:         string(connutils.RefTypeKey),
	}

	// Create Thing object
	thing := &models.Thing{}
	thing.AtContext = batchedRequest.AtContext
	thing.LastUpdateTimeUnix = 0

	if _, ok := fieldsToKeep["@class"]; ok {
		thing.AtClass = batchedRequest.AtClass
	}
	if _, ok := fieldsToKeep["schema"]; ok {
		thing.Schema = batchedRequest.Schema
	}
	if _, ok := fieldsToKeep["creationtimeunix"]; ok {
		thing.CreationTimeUnix = connutils.NowUnix()
	}
	if _, ok := fieldsToKeep["key"]; ok {
		thing.Key = keyRef
	}

	// Create request result object
	result := &models.ThingsGetResponseAO1Result{}
	result.Errors = nil

	// Create request response object
	responseObject := &models.ThingsGetResponse{}

	responseObject.Thing = *thing
	if _, ok := fieldsToKeep["thingid"]; ok {
		responseObject.ThingID = UUID
	}
	responseObject.Result = result

	resultStatus := models.ThingsGetResponseAO1ResultStatusSUCCESS

	validatedErr := validation.ValidateThingBody(ctx, batchedRequest, databaseSchema, requestLocks.DBConnector,
		network, serverConfig, principal.(*models.KeyTokenGetResponse))

	if validatedErr != nil {
		// Edit request result status
		responseObject.Result.Errors = createErrorResponseObject(validatedErr.Error())
		resultStatus = models.ThingsGetResponseAO1ResultStatusFAILED
		responseObject.Result.Status = &resultStatus
	} else {
		// Handle asynchronous requests
		if async {
			requestLocks.DelayedLock.IncSteps()
			resultStatus = models.ThingsGetResponseAO1ResultStatusPENDING
			responseObject.Result.Status = &resultStatus

			go func() {
				defer requestLocks.DelayedLock.Unlock()
				err := requestLocks.DBConnector.AddThing(ctx, thing, UUID)

				if err != nil {
					// Edit request result status
					resultStatus = models.ThingsGetResponseAO1ResultStatusFAILED
					responseObject.Result.Status = &resultStatus
					responseObject.Result.Errors = createErrorResponseObject(err.Error())
				}
			}()
		} else {
			// Handle synchronous requests
			err := requestLocks.DBConnector.AddThing(ctx, thing, UUID)

			if err != nil {
				// Edit request result status
				resultStatus = models.ThingsGetResponseAO1ResultStatusFAILED
				responseObject.Result.Status = &resultStatus
				responseObject.Result.Errors = createErrorResponseObject(err.Error())
			}
		}
	}

	// Send this batched request's response and its place in the batch request to the channel
	*requestResults <- rest_api_utils.BatchedThingsCreateRequestResponse{
		requestIndex,
		responseObject,
	}
}

// determine which field values not to return
func determineResponseFields(fields []*string, isThingsCreate bool) map[string]int {
	fieldsToKeep := map[string]int{"@class": 0, "schema": 0, "creationtimeunix": 0, "key": 0, "actionid": 0}

	// convert to things instead of actions
	if isThingsCreate {
		delete(fieldsToKeep, "actionid")
		fieldsToKeep["thingid"] = 0
	}

	if len(fields) > 0 {

		// check if "ALL" option is provided
		for _, field := range fields {
			fieldToKeep := strings.ToLower(*field)
			if fieldToKeep == "all" {
				return fieldsToKeep
			}
		}

		fieldsToKeep = make(map[string]int)
		// iterate over the provided fields
		for _, field := range fields {
			fieldToKeep := strings.ToLower(*field)
			fieldsToKeep[fieldToKeep] = 0
		}
	}

	return fieldsToKeep
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *http.Server, scheme, addr string) {
	// Create message service
	messaging = &messages.Messaging{}

	// Load the config using the flags
	serverConfig = &config.WeaviateConfig{}
	err := serverConfig.LoadConfig(connectorOptionGroup, messaging)

	// Add properties to the config
	serverConfig.Hostname = addr
	serverConfig.Scheme = scheme

	// Fatal error loading config file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	loadContextionary()

	connectToNetwork()

	// Connect to MQTT via Broker
	weaviateBroker.ConnectToMqtt(serverConfig.Environment.Broker.Host, serverConfig.Environment.Broker.Port)

	// Create the database connector usint the config
	err, dbConnector := dblisting.NewConnector(serverConfig.Environment.Database.Name, serverConfig.Environment.Database.DatabaseConfig)

	// Could not find, or configure connector.
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	// Construct a (distributed lock)
	localMutex := sync.RWMutex{}
	dbLock := database.RWLocker(&localMutex)

	// Configure schema manager
	if serverConfig.Environment.Database.LocalSchemaConfig == nil {
		messaging.ExitError(78, "Local schema manager is not configured.")
	}

	manager, err := db_local_schema_manager.New(
		serverConfig.Environment.Database.LocalSchemaConfig.StateDir, dbConnector, network)
	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not initialize local database state: %v", err))
	}

	manager.RegisterSchemaUpdateCallback(func(updatedSchema schema.Schema) {
		// Note that this is thread safe; we're running in a single go-routine, because the event
		// handlers are called when the SchemaLock is still held.

		fmt.Printf("UPDATESCHEMA DB: %#v\n", db)
		peers, err := network.ListPeers()
		if err != nil {
			graphQL = nil
			messaging.ErrorMessage(fmt.Sprintf("could not list network peers to regenerate schema:\n%#v\n", err))
			return
		}

		updatedGraphQL, err := graphqlapi.Build(&updatedSchema, peers, dbAndNetwork{Database: db, Network: network})
		if err != nil {
			// TODO: turn on safe mode gh-520
			graphQL = nil
			messaging.ErrorMessage(fmt.Sprintf("Could not re-generate GraphQL schema, because:\n%#v\n", err))
		} else {
			messaging.InfoMessage("Updated GraphQL schema")
			graphQL = updatedGraphQL
		}
	})

	// Now instantiate a database, with the configured lock, manager and connector.
	err, db = database.New(messaging, dbLock, manager, dbConnector, contextionary)
	if err != nil {
		messaging.ExitError(1, fmt.Sprintf("Could not initialize the database: %s", err.Error()))
	}
	manager.TriggerSchemaUpdateCallbacks()

	network.RegisterUpdatePeerCallback(func(peers peers.Peers) {
		manager.TriggerSchemaUpdateCallbacks()
	})

	network.RegisterSchemaGetter(&schemaGetter{db: db})
}

type schemaGetter struct {
	db database.Database
}

func (s *schemaGetter) Schema() schema.Schema {
	dbLock := s.db.ConnectorLock()
	defer dbLock.Unlock()
	return dbLock.GetSchema()
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
func setupMiddlewares(handler http.Handler) http.Handler {
	// Rewrite / workaround because of issue with handling two API keys
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kth := keyTokenHeader{
			Key:   strfmt.UUID(r.Header.Get("X-API-KEY")),
			Token: strfmt.UUID(r.Header.Get("X-API-TOKEN")),
		}
		jkth, _ := json.Marshal(kth)
		r.Header.Set("X-API-KEY", string(jkth))
		r.Header.Set("X-API-TOKEN", string(jkth))

		messaging.InfoMessage("generated both headers X-API-KEY and X-API-TOKEN")

		handler.ServeHTTP(w, r)
	})
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	handleCORS := cors.Default().Handler
	handler = handleCORS(handler)

	if feature_flags.EnableDevUI {
		handler = graphiql.AddMiddleware(handler)
		handler = swagger_middleware.AddMiddleware([]byte(SwaggerJSON), handler)
	}

	return addLogging(handler)
}

func addLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if serverConfig.Environment.Debug {
			log.Printf("Received request: %+v %+v\n", r.Method, r.URL)
		}
		next.ServeHTTP(w, r)
	})
}

// This function loads the Contextionary database, and creates
// an in-memory database for the centroids of the classes / properties in the Schema.
func loadContextionary() {
	// First load the file backed contextionary
	if serverConfig.Environment.Contextionary.KNNFile == "" {
		messaging.ExitError(78, "Contextionary KNN file not specified")
	}

	if serverConfig.Environment.Contextionary.IDXFile == "" {
		messaging.ExitError(78, "Contextionary IDX file not specified")
	}

	mmaped_contextionary, err := libcontextionary.LoadVectorFromDisk(serverConfig.Environment.Contextionary.KNNFile, serverConfig.Environment.Contextionary.IDXFile)

	if err != nil {
		messaging.ExitError(78, fmt.Sprintf("Could not load Contextionary; %+v", err))
	}

	messaging.InfoMessage("Contextionary loaded from disk")

	//TODO gh-618: update on schema change.
	//// Now create the in-memory contextionary based on the classes / properties.
	//databaseSchema :=
	//in_memory_contextionary, err := databaseSchema.BuildInMemoryContextionaryFromSchema(mmaped_contextionary)
	//if err != nil {
	//	messaging.ExitError(78, fmt.Sprintf("Could not build in-memory contextionary from schema; %+v", err))
	//}

	//// Combine contextionaries
	//contextionaries := []libcontextionary.Contextionary{*in_memory_contextionary, *mmaped_contextionary}
	//combined, err := libcontextionary.CombineVectorIndices(contextionaries)
	//
	// if err != nil {
	// 	messaging.ExitError(78, fmt.Sprintf("Could not combine the contextionary database with the in-memory generated contextionary; %+v", err))
	// }

	// messaging.InfoMessage("Contextionary extended with names in the schema")

	// // urgh, go.
	// x := libcontextionary.Contextionary(combined)
	// contextionary = &x

	// // whoop!

	contextionary = mmaped_contextionary
}

func connectToNetwork() {
	if serverConfig.Environment.Network == nil {
		messaging.InfoMessage(fmt.Sprintf("No network configured, not joining one"))
		network = libnetworkFake.FakeNetwork{}
	} else {
		genesis_url := strfmt.URI(serverConfig.Environment.Network.GenesisURL)
		public_url := strfmt.URI(serverConfig.Environment.Network.PublicURL)
		peer_name := serverConfig.Environment.Network.PeerName

		messaging.InfoMessage(fmt.Sprintf("Network configured, connecting to Genesis '%v'", genesis_url))
		new_net, err := libnetworkP2P.BootstrapNetwork(messaging, genesis_url, public_url, peer_name)
		if err != nil {
			messaging.ExitError(78, fmt.Sprintf("Could not connect to network! Reason: %+v", err))
		} else {
			network = *new_net
		}
	}
}
