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
	"crypto/tls"
	"encoding/json"
	errors_ "errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/creativesoftwarefdn/weaviate/restapi/operations/graphql"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/meta"

	jsonpatch "github.com/evanphx/json-patch"
	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/runtime/yamlpc"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	gographql "github.com/graphql-go/graphql"
	graceful "github.com/tylerb/graceful"
	"google.golang.org/grpc/grpclog"

	"github.com/creativesoftwarefdn/weaviate/broker"
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/connectors"
	"github.com/creativesoftwarefdn/weaviate/connectors/cassandra"
	"github.com/creativesoftwarefdn/weaviate/connectors/foobar"
	"github.com/creativesoftwarefdn/weaviate/connectors/kvcache"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/keys"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/things"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/creativesoftwarefdn/weaviate/validation"
)

const pageOverride int = 1

var connectorOptionGroup *swag.CommandLineOptionsGroup
var databaseSchema schema.WeaviateSchema
var serverConfig *config.WeaviateConfig
var dbConnector dbconnector.DatabaseConnector
var graphQLSchema *graphqlapi.GraphQLSchema
var messaging *messages.Messaging

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

// isOwnKeyOrLowerInTree returns whether a key is his own or in his children
func isOwnKeyOrLowerInTree(currentKey *models.KeyGetResponse, userKeyID strfmt.UUID, databaseConnector dbconnector.DatabaseConnector) bool {
	// If is own key, return true
	if strings.EqualFold(string(userKeyID), string(currentKey.KeyID)) {
		return true
	}

	// Get all child id's
	childIDs := []strfmt.UUID{}
	childIDs, _ = GetKeyChildrenUUIDs(databaseConnector, currentKey.KeyID, true, childIDs, 0, 0)

	// Check ID is in childIds
	isChildID := false
	for _, childID := range childIDs {
		if childID == userKeyID {
			isChildID = true
		}
	}

	// If ID is in the child ID's, you are allowed to do the action
	if isChildID {
		return true
	}

	return false
}

// GetKeyChildrenUUIDs returns children recursivly based on its parameters.
func GetKeyChildrenUUIDs(databaseConnector dbconnector.DatabaseConnector, parentUUID strfmt.UUID, filterOutDeleted bool, allIDs []strfmt.UUID, maxDepth int, depth int) ([]strfmt.UUID, error) {
	// Append on every depth
	if depth > 0 {
		allIDs = append(allIDs, parentUUID)
	}

	// Init children var
	children := []*models.KeyGetResponse{}

	// Get children from the db-connector
	err := databaseConnector.GetKeyChildren(parentUUID, &children)

	// Return error
	if err != nil {
		return allIDs, err
	}

	// For every depth, get the ID's
	if maxDepth == 0 || depth < maxDepth {
		for _, child := range children {
			allIDs, err = GetKeyChildrenUUIDs(databaseConnector, child.KeyID, filterOutDeleted, allIDs, maxDepth, depth+1)
		}
	}

	return allIDs, err
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
			Type:         connutils.RefTypeKey,
		})
	}

	return refs
}

func deleteKey(databaseConnector dbconnector.DatabaseConnector, parentUUID strfmt.UUID) {
	// Find its children
	var allIDs []strfmt.UUID

	// Get all the children to remove
	allIDs, _ = GetKeyChildrenUUIDs(databaseConnector, parentUUID, false, allIDs, 0, 0)

	// Append the children to the parent UUIDs to remove all
	allIDs = append(allIDs, parentUUID)

	// Delete for every child
	for _, keyID := range allIDs {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get the key to delete
		dbConnector.GetKey(keyID, &keyResponse)

		databaseConnector.DeleteKey(&keyResponse.Key, keyID)
	}
}

// GetAllConnectors contains all available connectors
func GetAllConnectors() []dbconnector.DatabaseConnector {
	// Set all existing connectors
	connectors := []dbconnector.DatabaseConnector{
		&foobar.Foobar{},
		&cassandra.Cassandra{},
	}

	return connectors
}

// GetAllCacheConnectors contains all available cache-connectors
func GetAllCacheConnectors() []dbconnector.CacheConnector {
	// Set all existing connectors
	connectors := []dbconnector.CacheConnector{
		&kvcache.KVCache{},
	}

	return connectors
}

// CreateDatabaseConnector gets the database connector by name from config
func CreateDatabaseConnector(env *config.Environment) dbconnector.DatabaseConnector {
	// Get all connectors
	connectors := GetAllConnectors()
	cacheConnectors := GetAllCacheConnectors()

	// Init the db-connector variable
	var connector dbconnector.DatabaseConnector

	// Loop through all connectors and determine its name
	for _, c := range connectors {
		if c.GetName() == env.Database.Name {
			messaging.InfoMessage(fmt.Sprintf("Using database '%s'", env.Database.Name))
			connector = c
			break
		}
	}

	// Loop through all cache-connectors and determine its name
	for _, cc := range cacheConnectors {
		if cc.GetName() == env.Cache.Name {
			messaging.InfoMessage(fmt.Sprintf("Using cache layer '%s'", env.Cache.Name))
			cc.SetDatabaseConnector(connector)
			connector = cc
			break
		}
	}

	return connector
}

// ActionsAllowed returns information whether an action is allowed based on given several input vars.
func ActionsAllowed(actions []string, validateObject interface{}, databaseConnector dbconnector.DatabaseConnector, objectOwnerUUID interface{}) (bool, error) {
	// Get the user by the given principal
	keyObject := validateObject.(*models.KeyGetResponse)

	// Check whether the given owner of the object is in the children, if the ownerID is given
	correctChild := false
	if objectOwnerUUID != nil {
		correctChild = isOwnKeyOrLowerInTree(keyObject, objectOwnerUUID.(strfmt.UUID), databaseConnector)
	} else {
		correctChild = true
	}

	// Return false if the object's owner is not the logged in user or one of its childs.
	if !correctChild {
		return false, errors_.New("the object does not belong to the given token or to one of the token's children")
	}

	// All possible actions in a map to check it more easily
	actionsToCheck := map[string]bool{
		"read":    false,
		"write":   false,
		"execute": false,
		"delete":  false,
	}

	// Add 'true' if an action has to be checked on its rights.
	for _, action := range actions {
		actionsToCheck[action] = true
	}

	// Check every action on its rights, if rights are needed and the key has not that kind of rights, return false.
	if actionsToCheck["read"] && !keyObject.Read {
		return false, errors_.New("read rights are needed to perform this action")
	}

	// Idem
	if actionsToCheck["write"] && !keyObject.Write {
		return false, errors_.New("write rights are needed to perform this action")
	}

	// Idem
	if actionsToCheck["delete"] && !keyObject.Delete {
		return false, errors_.New("delete rights are needed to perform this action")
	}

	// Idem
	if actionsToCheck["execute"] && !keyObject.Execute {
		return false, errors_.New("execute rights are needed to perform this action")
	}

	return true, nil
}

func configureFlags(api *operations.WeaviateAPI) {
	connectorOptionGroup = config.GetConfigOptionGroup()

	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		*connectorOptionGroup,
	}
}

// createErrorResponseObject is a common function to create an error response
func createErrorResponseObject(message string) *models.ErrorResponse {
	// Initialize return value
	er := &models.ErrorResponse{}

	// Fill the error with the message
	er.Error = &models.ErrorResponseError{
		Message: message,
	}

	return er
}

func headerAPIKeyHandling(keyToken string) (*models.KeyGetResponse, error) {
	// Convert JSON string to struct
	kth := keyTokenHeader{}
	json.Unmarshal([]byte(keyToken), &kth)

	// Validate both headers
	if kth.Key == "" || kth.Token == "" {
		return nil, errors.New(401, "Please provide both X-API-KEY and X-API-TOKEN headers.")
	}

	// Create key
	validatedKey := models.KeyGetResponse{}

	// Check if the user has access, true if yes
	hashed, err := dbConnector.ValidateToken(kth.Key, &validatedKey)

	// Error printing
	if err != nil {
		return nil, errors.New(401, err.Error())
	}

	// Check token
	if !connutils.TokenHashCompare(hashed, kth.Token) {
		return nil, errors.New(401, "Provided token is invalid.")
	}

	// Validate the key on expiry time
	currentUnix := connutils.NowUnix()

	if validatedKey.KeyExpiresUnix != -1 && validatedKey.KeyExpiresUnix < currentUnix {
		return nil, errors.New(401, "Provided key has expired.")
	}

	// key is valid, next step is allowing per Handler handling
	return &validatedKey, nil
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.BinConsumer = runtime.ByteStreamConsumer()

	api.UrlformConsumer = runtime.DiscardConsumer

	api.YamlConsumer = yamlpc.YAMLConsumer()

	api.XMLConsumer = runtime.XMLConsumer()

	api.MultipartformConsumer = runtime.DiscardConsumer

	api.TxtConsumer = runtime.TextConsumer()

	api.JSONProducer = runtime.JSONProducer()

	api.BinProducer = runtime.ByteStreamProducer()

	api.UrlformProducer = runtime.DiscardProducer

	api.YamlProducer = yamlpc.YAMLProducer()

	api.XMLProducer = runtime.XMLProducer()

	api.MultipartformProducer = runtime.DiscardProducer

	api.TxtProducer = runtime.TextProducer()

	/*
	 * HANDLE X-API-KEY
	 */
	// Applies when the "X-API-KEY" header is set
	api.APIKeyAuth = func(token string) (interface{}, error) {
		return headerAPIKeyHandling(token)
	}

	/*
	 * HANDLE X-API-TOKEN
	 */
	// Applies when the "X-API-TOKEN" header is set
	api.APITokenAuth = func(token string) (interface{}, error) {
		return headerAPIKeyHandling(token)
	}

	/*
	 * HANDLE EVENTS
	 */
	api.ActionsWeaviateActionsGetHandler = actions.WeaviateActionsGetHandlerFunc(func(params actions.WeaviateActionsGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}
		actionGetResponse.Things = &models.ObjectSubject{}

		// Get item from database
		err := dbConnector.GetAction(params.ActionID, &actionGetResponse)

		// Object is deleted
		if err != nil {
			return actions.NewWeaviateActionsGetNotFound()
		}

		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
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

		// Get item from database
		errGet := dbConnector.GetAction(UUID, &responseObject)

		// Init the response variables
		historyResponse := &models.ActionGetHistoryResponse{}
		historyResponse.PropertyHistory = []*models.ActionHistoryObject{}
		historyResponse.ActionID = UUID

		// Fill the history for these objects
		errHist := dbConnector.HistoryAction(UUID, &historyResponse.ActionHistory)

		// Check whether dont exist (both give an error) to return a not found
		if errGet != nil && (errHist != nil || len(historyResponse.PropertyHistory) == 0) {
			messaging.ErrorMessage(errGet)
			messaging.ErrorMessage(errHist)
			return actions.NewWeaviateActionHistoryGetNotFound()
		}

		if errHist == nil {
			if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, historyResponse.Key.NrDollarCref); !allowed {
				return actions.NewWeaviateActionHistoryGetForbidden()
			}
		} else if errGet == nil {
			if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, responseObject.Key.NrDollarCref); !allowed {
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
		actionGetResponse.Things = &models.ObjectSubject{}

		// Get and transform object
		UUID := strfmt.UUID(params.ActionID)
		errGet := dbConnector.GetAction(UUID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		actionGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Return error if UUID is not found.
		if errGet != nil {
			return actions.NewWeaviateActionsPatchNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
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
		validatedErr := validation.ValidateActionBody(&action.ActionCreate, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		go dbConnector.MoveToHistoryAction(&oldAction.Action, params.ActionID, false)

		// Update the database
		go dbConnector.UpdateAction(action, UUID)

		// Create return Object
		actionGetResponse.Action = *action

		// Returns accepted so a Go routine can process in the background
		return actions.NewWeaviateActionsPatchAccepted().WithPayload(&actionGetResponse)
	})
	api.ActionsWeaviateActionUpdateHandler = actions.WeaviateActionUpdateHandlerFunc(func(params actions.WeaviateActionUpdateParams, principal interface{}) middleware.Responder {
		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		UUID := params.ActionID
		errGet := dbConnector.GetAction(UUID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		// If there are no results, there is an error
		if errGet != nil {
			// Object not found response.
			return actions.NewWeaviateActionUpdateNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionUpdateForbidden()
		}

		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateActionBody(&params.Body.ActionCreate, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionUpdateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		go dbConnector.MoveToHistoryAction(&oldAction.Action, params.ActionID, false)

		// Update the database
		params.Body.LastUpdateTimeUnix = connutils.NowUnix()
		params.Body.CreationTimeUnix = actionGetResponse.CreationTimeUnix
		params.Body.Key = actionGetResponse.Key
		go dbConnector.UpdateAction(&params.Body.Action, UUID)

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
		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateActionBody(&params.Body.ActionCreate, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		return actions.NewWeaviateActionsValidateOK()
	})
	api.ActionsWeaviateActionsCreateHandler = actions.WeaviateActionsCreateHandlerFunc(func(params actions.WeaviateActionsCreateParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, dbConnector, nil); !allowed {
			return actions.NewWeaviateActionsCreateForbidden()
		}

		// Generate UUID for the new object
		UUID := connutils.GenerateUUID()

		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateActionBody(params.Body, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return actions.NewWeaviateActionsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Create Key-ref-Object
		url := serverConfig.GetHostAddress()
		keyRef := &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(*models.KeyGetResponse).KeyID,
			Type:         connutils.RefTypeKey,
		}

		// Make Action-Object
		action := &models.Action{}
		action.AtClass = params.Body.AtClass
		action.AtContext = params.Body.AtContext
		action.Schema = params.Body.Schema
		action.Things = params.Body.Things
		action.CreationTimeUnix = connutils.NowUnix()
		action.LastUpdateTimeUnix = 0
		action.Key = keyRef

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go dbConnector.AddAction(action, UUID)

		// Initialize a response object
		responseObject := &models.ActionGetResponse{}
		responseObject.Action = *action
		responseObject.ActionID = UUID

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return actions.NewWeaviateActionsCreateAccepted().WithPayload(responseObject)
	})
	api.ActionsWeaviateActionsDeleteHandler = actions.WeaviateActionsDeleteHandlerFunc(func(params actions.WeaviateActionsDeleteParams, principal interface{}) middleware.Responder {
		// Initialize response
		actionGetResponse := models.ActionGetResponse{}
		actionGetResponse.Schema = map[string]models.JSONObject{}
		actionGetResponse.Things = &models.ObjectSubject{}

		// Get item from database
		errGet := dbConnector.GetAction(params.ActionID, &actionGetResponse)

		// Save the old-aciton in a variable
		oldAction := actionGetResponse

		// Not found
		if errGet != nil {
			return actions.NewWeaviateActionsDeleteNotFound()
		}

		// This is a delete function, validate if allowed to delete?
		if allowed, _ := ActionsAllowed([]string{"delete"}, principal, dbConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsDeleteForbidden()
		}

		actionGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Move the current properties to the history
		go dbConnector.MoveToHistoryAction(&oldAction.Action, params.ActionID, false)

		// Add new row as GO-routine
		go dbConnector.DeleteAction(&actionGetResponse.Action, params.ActionID)

		// Return 'No Content'
		return actions.NewWeaviateActionsDeleteNoContent()
	})

	/*
	 * HANDLE KEYS
	 */
	api.KeysWeaviateKeyCreateHandler = keys.WeaviateKeyCreateHandlerFunc(func(params keys.WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
		// Create current User object from principal
		key := principal.(*models.KeyGetResponse)

		// Fill the new User object
		url := serverConfig.GetHostAddress()
		newKey := &models.KeyTokenGetResponse{}
		newKey.KeyID = connutils.GenerateUUID()
		newKey.Token = connutils.GenerateUUID()
		newKey.Parent = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(*models.KeyGetResponse).KeyID,
			Type:         connutils.RefTypeKey,
		}
		newKey.KeyCreate = *params.Body

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
		insertErr := dbConnector.AddKey(&newKey.Key, newKey.KeyID, connutils.TokenHasher(newKey.Token))
		if insertErr != nil {
			messaging.ErrorMessage(insertErr)
			return keys.NewWeaviateKeyCreateUnprocessableEntity().WithPayload(createErrorResponseObject(insertErr.Error()))
		}

		// Return SUCCESS
		return keys.NewWeaviateKeyCreateOK().WithPayload(newKey)

	})
	api.KeysWeaviateKeysChildrenGetHandler = keys.WeaviateKeysChildrenGetHandlerFunc(func(params keys.WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysChildrenGetNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyGetResponse)
		if !isOwnKeyOrLowerInTree(keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysChildrenGetForbidden()
		}

		// Get the children
		childIDs := []strfmt.UUID{}
		childIDs, _ = GetKeyChildrenUUIDs(dbConnector, params.KeyID, true, childIDs, 1, 0)

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = generateMultipleRefObject(childIDs)

		// Return children with 'OK'
		return keys.NewWeaviateKeysChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysDeleteHandler = keys.WeaviateKeysDeleteHandlerFunc(func(params keys.WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysDeleteNotFound()
		}

		// Check on permissions, only delete allowed if lower in tree (not own key)
		keyObject, _ := principal.(*models.KeyGetResponse)
		if !isOwnKeyOrLowerInTree(keyObject, params.KeyID, dbConnector) || keyObject.KeyID == params.KeyID {
			return keys.NewWeaviateKeysDeleteForbidden()
		}

		// Remove key from database if found
		deleteKey(dbConnector, params.KeyID)

		// Return 'No Content'
		return keys.NewWeaviateKeysDeleteNoContent()
	})
	api.KeysWeaviateKeysGetHandler = keys.WeaviateKeysGetHandlerFunc(func(params keys.WeaviateKeysGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// Get item from database
		err := dbConnector.GetKey(params.KeyID, &keyResponse)

		// Object is deleted or not-existing
		if err != nil {
			return keys.NewWeaviateKeysGetNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyGetResponse)
		if !isOwnKeyOrLowerInTree(keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysGetForbidden()
		}

		// Get is successful
		return keys.NewWeaviateKeysGetOK().WithPayload(&keyResponse)
	})
	api.KeysWeaviateKeysMeChildrenGetHandler = keys.WeaviateKeysMeChildrenGetHandlerFunc(func(params keys.WeaviateKeysMeChildrenGetParams, principal interface{}) middleware.Responder {
		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		currentKey := principal.(*models.KeyGetResponse)

		// Get the children
		childIDs := []strfmt.UUID{}
		childIDs, _ = GetKeyChildrenUUIDs(dbConnector, currentKey.KeyID, true, childIDs, 1, 0)

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = generateMultipleRefObject(childIDs)

		// Return children with 'OK'
		return keys.NewWeaviateKeysMeChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysMeGetHandler = keys.WeaviateKeysMeGetHandlerFunc(func(params keys.WeaviateKeysMeGetParams, principal interface{}) middleware.Responder {
		// Initialize response object
		responseObject := models.KeyGetResponse{}

		isRoot := false
		responseObject.Key.IsRoot = &isRoot

		// Get item from database
		err := dbConnector.GetKey(principal.(*models.KeyGetResponse).KeyID, &responseObject)

		// Object is deleted or not-existing
		if err != nil {
			return keys.NewWeaviateKeysGetNotFound()
		}

		// Get is successful
		return keys.NewWeaviateKeysMeGetOK().WithPayload(&responseObject)
	})
	api.KeysWeaviateKeysRenewTokenHandler = keys.WeaviateKeysRenewTokenHandlerFunc(func(params keys.WeaviateKeysRenewTokenParams, principal interface{}) middleware.Responder {
		// Initialize response
		keyResponse := models.KeyGetResponse{}

		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		errGet := dbConnector.GetKey(params.KeyID, &keyResponse)

		// Not found
		if errGet != nil {
			return keys.NewWeaviateKeysRenewTokenNotFound()
		}

		// Check on permissions
		keyObject, _ := principal.(*models.KeyGetResponse)
		if !isOwnKeyOrLowerInTree(keyObject, params.KeyID, dbConnector) {
			return keys.NewWeaviateKeysRenewTokenForbidden()
		}

		// Can't renew own unless root
		if keyObject.KeyID == params.KeyID && keyObject.Parent != nil {
			return keys.NewWeaviateKeysRenewTokenForbidden()
		}

		// Generate new token
		newToken := connutils.GenerateUUID()

		// Update the key in the database
		insertErr := dbConnector.UpdateKey(&keyResponse.Key, keyResponse.KeyID, connutils.TokenHasher(newToken))
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
		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, dbConnector, nil); !allowed {
			return things.NewWeaviateThingsCreateForbidden()
		}

		// Generate UUID for the new object
		UUID := connutils.GenerateUUID()

		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateThingBody(params.Body, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsCreateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Create Key-ref-Object
		url := serverConfig.GetHostAddress()
		keyRef := &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(*models.KeyGetResponse).KeyID,
			Type:         connutils.RefTypeKey,
		}

		// Make Thing-Object
		thing := &models.Thing{}
		thing.Schema = params.Body.Schema
		thing.AtClass = params.Body.AtClass
		thing.AtContext = params.Body.AtContext
		thing.CreationTimeUnix = connutils.NowUnix()
		thing.LastUpdateTimeUnix = 0
		thing.Key = keyRef

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go dbConnector.AddThing(thing, UUID)

		// Create response Object from create object.
		responseObject := &models.ThingGetResponse{}
		responseObject.Thing = *thing
		responseObject.ThingID = UUID

		// Return SUCCESS (NOTE: this is ACCEPTED, so the dbConnector.Add should have a go routine)
		return things.NewWeaviateThingsCreateAccepted().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsDeleteHandler = things.WeaviateThingsDeleteHandlerFunc(func(params things.WeaviateThingsDeleteParams, principal interface{}) middleware.Responder {
		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		errGet := dbConnector.GetThing(params.ThingID, &thingGetResponse)

		// Save the old-thing in a variable
		oldThing := thingGetResponse

		// Not found
		if errGet != nil {
			return things.NewWeaviateThingsDeleteNotFound()
		}

		// This is a delete function, validate if allowed to delete?
		if allowed, _ := ActionsAllowed([]string{"delete"}, principal, dbConnector, thingGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsDeleteForbidden()
		}

		// Delete the Actions
		actionsExist := true
		lastActionsCount := int64(0)
		for actionsExist {
			actions := models.ActionsListResponse{}
			actions.Actions = []*models.ActionGetResponse{}
			dbConnector.ListActions(params.ThingID, 50, 0, []*connutils.WhereQuery{}, &actions)
			for _, v := range actions.Actions {
				go dbConnector.DeleteAction(&v.Action, v.ActionID)
			}

			// Exit if total results are 0 or the total results are not lowering, then there is some kind of error
			actionsExist = (actions.TotalResults > 0 && actions.TotalResults != lastActionsCount)
			lastActionsCount = actions.TotalResults
		}

		thingGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Move the current properties to the history
		go dbConnector.MoveToHistoryThing(&oldThing.Thing, params.ThingID, true)

		// Add new row as GO-routine
		go dbConnector.DeleteThing(&thingGetResponse.Thing, params.ThingID)

		// Return 'No Content'
		return things.NewWeaviateThingsDeleteNoContent()
	})
	api.ThingsWeaviateThingsGetHandler = things.WeaviateThingsGetHandlerFunc(func(params things.WeaviateThingsGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		responseObject := models.ThingGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Get item from database
		err := dbConnector.GetThing(strfmt.UUID(params.ThingID), &responseObject)

		// Object is not found
		if err != nil {
			messaging.ErrorMessage(err)
			return things.NewWeaviateThingsGetNotFound()
		}

		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, responseObject.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsGetForbidden()
		}

		// Get is successful
		return things.NewWeaviateThingsGetOK().WithPayload(&responseObject)
	})

	api.ThingsWeaviateThingHistoryGetHandler = things.WeaviateThingHistoryGetHandlerFunc(func(params things.WeaviateThingHistoryGetParams, principal interface{}) middleware.Responder {
		// Initialize response
		responseObject := models.ThingGetResponse{}
		responseObject.Schema = map[string]models.JSONObject{}

		// Set UUID var for easy usage
		UUID := strfmt.UUID(params.ThingID)

		// Get item from database
		errGet := dbConnector.GetThing(UUID, &responseObject)

		// Init the response variables
		historyResponse := &models.ThingGetHistoryResponse{}
		historyResponse.PropertyHistory = []*models.ThingHistoryObject{}
		historyResponse.ThingID = UUID

		// Fill the history for these objects
		errHist := dbConnector.HistoryThing(UUID, &historyResponse.ThingHistory)

		// Check whether dont exist (both give an error) to return a not found
		if errGet != nil && (errHist != nil || len(historyResponse.PropertyHistory) == 0) {
			messaging.ErrorMessage(errGet)
			messaging.ErrorMessage(errHist)
			return things.NewWeaviateThingHistoryGetNotFound()
		}

		// This is a read function, validate if allowed to read?
		if errHist == nil {
			if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, historyResponse.Key.NrDollarCref); !allowed {
				return things.NewWeaviateThingHistoryGetForbidden()
			}
		} else if errGet == nil {
			if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, responseObject.Key.NrDollarCref); !allowed {
				return things.NewWeaviateThingHistoryGetForbidden()
			}
		}

		// Thing is deleted when we have an get error and no history error
		historyResponse.Deleted = errGet != nil && errHist == nil && len(historyResponse.PropertyHistory) != 0

		return things.NewWeaviateThingHistoryGetOK().WithPayload(historyResponse)
	})

	api.ThingsWeaviateThingsListHandler = things.WeaviateThingsListHandlerFunc(func(params things.WeaviateThingsListParams, principal interface{}) middleware.Responder {
		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// Get user out of principal
		keyID := principal.(*models.KeyGetResponse).KeyID

		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, keyID); !allowed {
			return things.NewWeaviateThingsListForbidden()
		}

		// Initialize response
		thingsResponse := models.ThingsListResponse{}
		thingsResponse.Things = []*models.ThingGetResponse{}

		// List all results
		err := dbConnector.ListThings(limit, (page-1)*limit, keyID, []*connutils.WhereQuery{}, &thingsResponse)

		if err != nil {
			messaging.ErrorMessage(err)
		}

		return things.NewWeaviateThingsListOK().WithPayload(&thingsResponse)
	})
	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal interface{}) middleware.Responder {
		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get and transform object
		UUID := strfmt.UUID(params.ThingID)
		errGet := dbConnector.GetThing(UUID, &thingGetResponse)

		// Save the old-thing in a variable
		oldThing := thingGetResponse

		// Add update time
		thingGetResponse.LastUpdateTimeUnix = connutils.NowUnix()

		// Return error if UUID is not found.
		if errGet != nil {
			return things.NewWeaviateThingsPatchNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, dbConnector, thingGetResponse.Key.NrDollarCref); !allowed {
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
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(applyErr.Error()))
		}

		// Turn it into a Thing object
		thing := &models.Thing{}
		json.Unmarshal([]byte(updatedJSON), &thing)

		// Validate schema made after patching with the weaviate schema
		validatedErr := validation.ValidateThingBody(&thing.ThingCreate, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		go dbConnector.MoveToHistoryThing(&oldThing.Thing, UUID, false)

		// Update the database
		go dbConnector.UpdateThing(thing, UUID)

		// Create return Object
		thingGetResponse.Thing = *thing

		// Returns accepted so a Go routine can process in the background
		return things.NewWeaviateThingsPatchAccepted().WithPayload(&thingGetResponse)
	})
	api.ThingsWeaviateThingsUpdateHandler = things.WeaviateThingsUpdateHandlerFunc(func(params things.WeaviateThingsUpdateParams, principal interface{}) middleware.Responder {
		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}

		// Get item from database
		UUID := params.ThingID
		errGet := dbConnector.GetThing(UUID, &thingGetResponse)

		// Save the old-thing in a variable
		oldThing := thingGetResponse

		// If there are no results, there is an error
		if errGet != nil {
			// Object not found response.
			return things.NewWeaviateThingsUpdateNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, dbConnector, thingGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsUpdateForbidden()
		}

		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateThingBody(&params.Body.ThingCreate, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsUpdateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		// Move the current properties to the history
		go dbConnector.MoveToHistoryThing(&oldThing.Thing, UUID, false)

		// Update the database
		params.Body.LastUpdateTimeUnix = connutils.NowUnix()
		params.Body.CreationTimeUnix = thingGetResponse.CreationTimeUnix
		params.Body.Key = thingGetResponse.Key
		go dbConnector.UpdateThing(&params.Body.Thing, UUID)

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
		// Validate schema given in body with the weaviate schema
		validatedErr := validation.ValidateThingBody(params.Body, databaseSchema, dbConnector, serverConfig)
		if validatedErr != nil {
			return things.NewWeaviateThingsValidateUnprocessableEntity().WithPayload(createErrorResponseObject(validatedErr.Error()))
		}

		return things.NewWeaviateThingsValidateOK()
	})
	api.MetaWeaviateMetaGetHandler = meta.WeaviateMetaGetHandlerFunc(func(params meta.WeaviateMetaGetParams, principal interface{}) middleware.Responder {
		// Create response object
		metaResponse := &models.Meta{}

		// Set the response object's values
		metaResponse.Hostname = serverConfig.GetHostAddress()
		metaResponse.ActionsSchema = databaseSchema.ActionSchema.Schema
		metaResponse.ThingsSchema = databaseSchema.ThingSchema.Schema

		return meta.NewWeaviateMetaGetOK().WithPayload(metaResponse)
	})
	api.ThingsWeaviateThingsActionsListHandler = things.WeaviateThingsActionsListHandlerFunc(func(params things.WeaviateThingsActionsListParams, principal interface{}) middleware.Responder {
		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// Get key-object
		keyObject := principal.(*models.KeyGetResponse)

		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, dbConnector, keyObject.KeyID); !allowed {
			return things.NewWeaviateThingsActionsListForbidden()
		}

		// Initialize response
		thingGetResponse := models.ThingGetResponse{}
		thingGetResponse.Schema = map[string]models.JSONObject{}
		errGet := dbConnector.GetThing(params.ThingID, &thingGetResponse)

		// If there are no results, there is an error
		if errGet != nil {
			// Object not found response.
			return things.NewWeaviateThingsActionsListNotFound()
		}

		// Initialize response
		actionsResponse := models.ActionsListResponse{}
		actionsResponse.Actions = []*models.ActionGetResponse{}

		// List all results
		err := dbConnector.ListActions(params.ThingID, limit, (page-1)*limit, []*connutils.WhereQuery{}, &actionsResponse)

		if err != nil {
			messaging.ErrorMessage(err)
		}

		return things.NewWeaviateThingsActionsListOK().WithPayload(&actionsResponse)
	})
	api.GraphqlWeaviateGraphqlPostHandler = graphql.WeaviateGraphqlPostHandlerFunc(func(params graphql.WeaviateGraphqlPostParams, principal interface{}) middleware.Responder {
		defer messaging.TimeTrack(time.Now())
		messaging.DebugMessage("Starting GraphQL resolving")

		// Get all input from the body of the request, as it is a POST.
		query := params.Body.Query
		operationName := params.Body.OperationName

		// If query is empty, the request is unprocessable
		if query == "" {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity()
		}

		// Only set variables if exists in request
		var variables map[string]interface{}
		if params.Body.Variables != nil {
			variables = params.Body.Variables.(map[string]interface{})
		}

		// Get the results by doing a request with the given parameters and the initialized schema.
		graphQLSchema.SetKey(principal.(*models.KeyGetResponse))
		gqlSchema, _ := graphQLSchema.GetGraphQLSchema()

		// Do the request
		result := gographql.Do(gographql.Params{
			Schema:         gqlSchema,
			RequestString:  query,
			OperationName:  operationName,
			VariableValues: variables,
		})

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)

		// If json gave error, return nothing.
		if jsonErr != nil {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity()
		}

		// Put the data in a response ready object
		graphQLResponse := &models.GraphQLResponse{}
		err := json.Unmarshal(resultJSON, graphQLResponse)

		// If json gave error, return nothing.
		if err != nil {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity()
		}

		messaging.DebugMessage("Ending GraphQL resolving")

		// Return the response
		return graphql.NewWeaviateGraphqlPostOK().WithPayload(graphQLResponse)
	})

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *graceful.Server, scheme, addr string) {

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

	// Load the schema using the config
	databaseSchema = schema.WeaviateSchema{}
	err = databaseSchema.LoadSchema(&serverConfig.Environment, messaging)

	// Fatal error loading schema file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	// Connect to MQTT via Broker
	weaviateBroker.ConnectToMqtt(serverConfig.Environment.Broker.Host, serverConfig.Environment.Broker.Port)

	// Create the database connector usint the config
	dbConnector = CreateDatabaseConnector(&serverConfig.Environment)

	// Error the system when the database connector returns no connector
	if dbConnector == nil {
		messaging.ExitError(78, "database with the name '"+serverConfig.Environment.Database.Name+"' couldn't be loaded from the config")
	}

	// Set connector vars
	err = dbConnector.SetConfig(&serverConfig.Environment)
	// Fatal error loading config file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	err = dbConnector.SetSchema(&databaseSchema)
	// Fatal error loading schema file
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	err = dbConnector.SetMessaging(messaging)
	// Fatal error setting messaging
	if err != nil {
		messaging.ExitError(78, err.Error())
	}

	dbConnector.SetServerAddress(serverConfig.GetHostAddress())

	// connect the database
	errConnect := dbConnector.Connect()
	if errConnect != nil {
		messaging.ExitError(1, "database with the name '"+serverConfig.Environment.Database.Name+"' gave an error when connecting: "+errConnect.Error())
	}

	// init the database
	errInit := dbConnector.Init()
	if errInit != nil {
		messaging.ExitError(1, "database with the name '"+serverConfig.Environment.Database.Name+"' gave an error when initializing: "+errInit.Error())
	}

	// Init the GraphQL schema
	graphQLSchema = graphqlapi.NewGraphQLSchema(dbConnector, serverConfig, &databaseSchema, messaging)

	// Error init
	errInitGQL := graphQLSchema.InitSchema()
	if errInitGQL != nil {
		messaging.ExitError(1, "GraphQL schema initialization gave an error when initializing: "+errInitGQL.Error())
	}

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
		handler.ServeHTTP(w, r)
	})
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	return handler
}
