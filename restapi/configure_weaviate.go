/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
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
	"reflect"
	"strings"
	"unicode"

	jsonpatch "github.com/evanphx/json-patch"
	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/runtime/yamlpc"
	"github.com/go-openapi/strfmt"
	graceful "github.com/tylerb/graceful"

	"github.com/go-openapi/swag"
	"github.com/weaviate/weaviate/config"
	"github.com/weaviate/weaviate/connectors"
	"github.com/weaviate/weaviate/connectors/utils"
	weaviate_error "github.com/weaviate/weaviate/error"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/mqtt"
	"github.com/weaviate/weaviate/restapi/operations"
	"github.com/weaviate/weaviate/restapi/operations/actions"
	"github.com/weaviate/weaviate/restapi/operations/keys"
	"github.com/weaviate/weaviate/restapi/operations/things"
	"github.com/weaviate/weaviate/schema"
	"google.golang.org/grpc/grpclog"
)

const maxResultsOverride int64 = 100
const pageOverride int64 = 1

var connectorOptionGroup *swag.CommandLineOptionsGroup
var databaseSchema schema.WeaviateSchema
var databaseConfig config.WeaviateConfig

func init() {
	discard := ioutil.Discard
	myGRPCLogger := log.New(discard, "", log.LstdFlags)
	grpclog.SetLogger(myGRPCLogger)

	// Create temp folder if it does not exist
	tempFolder := "temp"
	if _, err := os.Stat(tempFolder); os.IsNotExist(err) {
		log.Println("Temp folder created...")
		os.Mkdir(tempFolder, 0766)
	}
}

// getLimit returns the maximized limit
func getLimit(paramMaxResults *int64) int {
	maxResults := maxResultsOverride
	// Get the max results from params, if exists
	if paramMaxResults != nil {
		maxResults = *paramMaxResults
	}

	// Max results form URL, otherwise max = maxResultsOverride.
	return int(math.Min(float64(maxResults), float64(maxResultsOverride)))
}

// getPage returns the page if set
func getPage(paramPage *int64) int {
	page := pageOverride
	// Get the page from params, if exists
	if paramPage != nil {
		page = *paramPage
	}

	// Page form URL, otherwise max = maxResultsOverride.
	return int(page)
}

// getKind generates a kind out of an object
func getKind(object interface{}) *string {
	kinds := strings.Split(reflect.TypeOf(object).String(), ".")
	kind := kinds[len(kinds)-1]
	for i, v := range kind {
		kind = string(unicode.ToLower(v)) + kind[i+1:]
		break
	}
	kind = "weaviate#" + kind

	return &kind
}

// isOwnKeyOrLowerInTree returns whether a key is his own or in his children
func isOwnKeyOrLowerInTree(currentUsersObject connector_utils.Key, userKeyID strfmt.UUID, databaseConnector dbconnector.DatabaseConnector) bool {
	// // If is own key, return true
	// if strings.EqualFold(userKeyID, currentUsersObject.UUID) {
	// 	return true
	// }

	// // Get all child id's
	// var childIDs []string
	// childIDs = GetKeyChildren(databaseConnector, currentUsersObject.UUID, true, childIDs, 0, 0)

	// // Check ID is in childIds
	// isChildID := false
	// for _, childID := range childIDs {
	// 	if childID == userKeyID {
	// 		isChildID = true
	// 	}
	// }

	// // This is a delete function, validate if allowed to do action with own/parent.
	// if isChildID {
	// 	return true
	// }

	return false
}

// GetKeyChildren returns children recursivly based on its parameters.
func GetKeyChildren(databaseConnector dbconnector.DatabaseConnector, parentUUID string, filterOutDeleted bool, allIDs []string, maxDepth int, depth int) []string {
	// if depth > 0 {
	// 	allIDs = append(allIDs, parentUUID)
	// }

	// childUserObjects, _ := databaseConnector.GetChildObjects(parentUUID, filterOutDeleted)

	// if maxDepth == 0 || depth < maxDepth {
	// 	for _, childUserObject := range childUserObjects {
	// 		allIDs = GetKeyChildren(databaseConnector, childUserObject.Uuid, filterOutDeleted, allIDs, maxDepth, depth+1)
	// 	}
	// }

	return allIDs
}

func deleteKey(databaseConnector dbconnector.DatabaseConnector, parentUUID strfmt.UUID) {
	// // Find its children
	// var allIDs []string
	// allIDs = GetKeyChildren(databaseConnector, parentUUID, false, allIDs, 0, 0)

	// allIDs = append(allIDs, parentUUID)

	// // Delete for every child
	// for _, keyID := range allIDs {
	// 	go databaseConnector.DeleteKey(keyID)
	// }
}

func validateSchemaInBody(weaviateSchema *schema.Schema, bodySchema *models.Schema, className string) error {
	// TODO: Implementation required
	// log.Println(weaviateSchema)
	// log.Println(*bodySchema)
	// log.Println(className)

	// Validated error-message
	// return errors_.New("no valid schema used")
	return nil
}

// ActionsAllowed returns information whether an action is allowed based on given several input vars.
func ActionsAllowed(actions []string, validateObject interface{}, databaseConnector dbconnector.DatabaseConnector, objectOwnerUUID interface{}) (bool, error) {
	// Get the user by the given principal
	keyObject := connector_utils.PrincipalMarshalling(validateObject)

	// // Check whether the given owner of the object is in the children, if the ownerID is given
	// correctChild := false
	// if objectOwnerUUID != nil {
	// 	correctChild = isOwnKeyOrLowerInTree(keyObject, objectOwnerUUID.(strfmt.UUID), databaseConnector)
	// } else {
	// 	correctChild = true
	// }

	// // Return false if the object's owner is not the logged in user or one of its childs.
	// if !correctChild {
	// 	return false, errors_.New("the object does not belong to the given token or to one of the token's children")
	// }

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

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	// Load the config using the flags
	databaseConfig := config.WeaviateConfig{}
	err := databaseConfig.LoadConfig(connectorOptionGroup)

	// Fatal error loading config file
	if err != nil {
		weaviate_error.ExitError(78, err.Error())
	}

	// Load the schema using the config
	databaseSchema = schema.WeaviateSchema{}
	err = databaseSchema.LoadSchema(&databaseConfig.Environment)

	// Fatal error loading schema file
	if err != nil {
		weaviate_error.ExitError(78, err.Error())
	}

	// Create the database connector usint the config
	databaseConnector := dbconnector.CreateDatabaseConnector(&databaseConfig.Environment)

	// Error the system when the database connector returns no connector
	if databaseConnector == nil {
		weaviate_error.ExitError(78, "database with the name '"+databaseConfig.Environment.Database.Name+"' couldn't be loaded from the config")
	}

	// Set connector vars
	err = databaseConnector.SetConfig(&databaseConfig.Environment)
	// Fatal error loading config file
	if err != nil {
		weaviate_error.ExitError(78, err.Error())
	}

	err = databaseConnector.SetSchema(&databaseSchema)
	// Fatal error loading schema file
	if err != nil {
		weaviate_error.ExitError(78, err.Error())
	}

	// connect the database
	errConnect := databaseConnector.Connect()
	if errConnect != nil {
		weaviate_error.ExitError(1, "database with the name '"+databaseConfig.Environment.Database.Name+"' gave an error when connecting: "+errConnect.Error())
	}

	// init the database
	errInit := databaseConnector.Init()
	if errInit != nil {
		weaviate_error.ExitError(1, "database with the name '"+databaseConfig.Environment.Database.Name+"' gave an error when initializing: "+errInit.Error())
	}

	// connect to mqtt
	mqtt_client.Connect()

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
		// Check if the user has access, true if yes
		validatedKey, err := databaseConnector.ValidateToken(strfmt.UUID(token))

		// Error printing
		if err != nil {
			return nil, errors.New(401, err.Error())
		}

		// Validate the key on expiry time
		currentUnix := connector_utils.NowUnix()
		if validatedKey.KeyExpiresUnix != -1 && validatedKey.KeyExpiresUnix < currentUnix {
			return nil, errors.New(401, "Provided key has expired.")
		}

		// key is valid, next step is allowing per Handler handling
		return validatedKey, nil
	}

	/*
	 * HANDLE EVENTS
	 */
	api.ActionsWeaviateActionsGetHandler = actions.WeaviateActionsGetHandlerFunc(func(params actions.WeaviateActionsGetParams, principal interface{}) middleware.Responder {
		// Get item from database
		actionGetResponse, err := databaseConnector.GetAction(params.ActionID)

		// Object is deleted eleted
		if err != nil {
			return actions.NewWeaviateActionsGetNotFound()
		}

		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, databaseConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return actions.NewWeaviateActionsGetForbidden()
		}

		// Get is successful
		return actions.NewWeaviateActionsGetOK().WithPayload(&actionGetResponse)
	})
	api.ActionsWeaviateActionsPatchHandler = actions.WeaviateActionsPatchHandlerFunc(func(params actions.WeaviateActionsPatchParams, principal interface{}) middleware.Responder {
		// Get and transform object
		UUID := strfmt.UUID(params.ActionID)
		actionGetResponse, errGet := databaseConnector.GetAction(UUID)
		actionGetResponse.LastUpdateTimeUnix = connector_utils.NowUnix()

		// Return error if UUID is not found.
		if errGet != nil {
			return actions.NewWeaviateActionsPatchNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, databaseConnector, actionGetResponse.Key.NrDollarCref); !allowed {
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
			return actions.NewWeaviateActionsPatchUnprocessableEntity()
		}

		// Turn it into a Action object
		action := &models.Action{}
		json.Unmarshal([]byte(updatedJSON), &action)

		// Update the database
		insertErr := databaseConnector.UpdateAction(action, UUID) // TODO: go-routine?
		if insertErr != nil {
			log.Println("InsertErr:", insertErr)
		}

		// Create return Object
		responseObject := &models.ActionGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &responseObject)
		responseObject.ActionID = UUID
		url := "http://localhost/"
		responseObject.Key = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(connector_utils.Key).UUID,
			Type:         "Key",
		}

		return actions.NewWeaviateActionsPatchOK().WithPayload(responseObject)
	})
	api.ActionsWeaviateActionsValidateHandler = actions.WeaviateActionsValidateHandlerFunc(func(params actions.WeaviateActionsValidateParams, principal interface{}) middleware.Responder {
		// Validate Schema given in body with the weaviate schema
		validatedErr := validateSchemaInBody(&databaseSchema.ThingSchema.Schema, &params.Body.Schema, params.Body.AtClass)
		if validatedErr != nil {
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte(fmt.Sprintf("{ \"ERROR\": \"%s\" }", validatedErr.Error())))
			})
		}

		return actions.NewWeaviateActionsValidateOK()
	})
	api.ActionsWeaviateActionsCreateHandler = actions.WeaviateActionsCreateHandlerFunc(func(params actions.WeaviateActionsCreateParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, databaseConnector, nil); !allowed {
			return actions.NewWeaviateActionsCreateForbidden()
		}

		// Generate UUID for the new object
		UUID := connector_utils.GenerateUUID()

		// Validate Schema given in body with the weaviate schema
		validatedErr := validateSchemaInBody(&databaseSchema.ThingSchema.Schema, &params.Body.Schema, params.Body.AtClass)
		if validatedErr != nil {
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte(fmt.Sprintf("{ \"ERROR\": \"%s\" }", validatedErr.Error())))
			})
		}

		// Make Action-Object
		actionCreateJSON, _ := json.Marshal(params.Body)
		action := &models.Action{}
		json.Unmarshal([]byte(actionCreateJSON), action)

		action.CreationTimeUnix = connector_utils.NowUnix()
		action.LastUpdateTimeUnix = 0

		// Save to DB, this needs to be a Go routine because we will return an accepted
		insertErr := databaseConnector.AddAction(action, UUID, principal.(connector_utils.Key).UUID) // TODO: go-routine?
		if insertErr != nil {
			log.Println("InsertErr:", insertErr)
		}

		// Initialize a response object
		responseObject := &models.ActionGetResponse{}
		responseObject.Action = *action
		responseObject.ActionID = UUID
		url := "http://localhost/"
		responseObject.Key = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(connector_utils.Key).UUID,
			Type:         "Key",
		}

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return actions.NewWeaviateActionsCreateAccepted().WithPayload(responseObject)
	})
	api.ActionsWeaviateActionsDeleteHandler = actions.WeaviateActionsDeleteHandlerFunc(func(params actions.WeaviateActionsDeleteParams, principal interface{}) middleware.Responder {
		// Get item from database
		actionGetResponse, errGet := databaseConnector.GetAction(params.ActionID)

		// Not found
		if errGet != nil {
			return actions.NewWeaviateActionsDeleteNotFound()
		}

		// This is a delete function, validate if allowed to delete? TODO
		if allowed, _ := ActionsAllowed([]string{"delete"}, principal, databaseConnector, actionGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsDeleteForbidden()
		}

		// Add new row as GO-routine
		go databaseConnector.DeleteAction(params.ActionID)

		// Return 'No Content'
		return actions.NewWeaviateActionsDeleteNoContent()
	})

	/*
	 * HANDLE KEYS
	 */
	api.KeysWeaviateKeyCreateHandler = keys.WeaviateKeyCreateHandlerFunc(func(params keys.WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
		// Create current User object from principal
		key := connector_utils.PrincipalMarshalling(principal)

		// Fill the new User object
		newKey := &connector_utils.Key{}
		newKey.Root = false
		newKey.UUID = connector_utils.GenerateUUID()
		newKey.KeyToken = connector_utils.GenerateUUID()
		newKey.Parent = string(principal.(connector_utils.Key).UUID)
		newKey.KeyCreate = *params.Body

		// Key expiry time is in the past
		currentUnix := connector_utils.NowUnix()
		if newKey.KeyExpiresUnix != -1 && newKey.KeyExpiresUnix < currentUnix {
			// return keys.NewWeaviateKeyCreateUnprocessableEntity()
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte(fmt.Sprintf("{ \"ERROR\": \"%s\" }", "Key expiry time is in the past.")))
			})
		}

		// Key expiry time is later than the expiry time of parent
		if key.KeyExpiresUnix != -1 && key.KeyExpiresUnix < newKey.KeyExpiresUnix {
			// return keys.NewWeaviateKeyCreateUnprocessableEntity()
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte(fmt.Sprintf("{ \"ERROR\": \"%s\" }", "Key expiry time is later than the expiry time of parent.")))
			})
		}

		// Save to DB, this needs to be a Go routine because we will return an accepted
		insertErr := databaseConnector.AddKey(newKey, newKey.UUID) // TODO: go-routine?
		if insertErr != nil {
			log.Println("InsertErr:", insertErr)
		}

		// Create response Object from create object.
		responseObject := &models.KeyTokenGetResponse{}
		responseObject.KeyCreate = newKey.KeyCreate
		responseObject.KeyID = newKey.UUID
		url := "http://localhost/"
		responseObject.Parent = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(connector_utils.Key).UUID,
			Type:         "Key",
		}

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return keys.NewWeaviateKeyCreateAccepted().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysChildrenGetHandler = keys.WeaviateKeysChildrenGetHandlerFunc(func(params keys.WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		// userObject, errGet := databaseConnector.GetKey(string(params.KeyID))

		// // Not found
		// if userObject.Deleted || errGet != nil {
		// 	return keys.NewWeaviateKeysChildrenGetNotFound()
		// }

		// // Check on permissions
		// currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)
		// if !isOwnKeyOrLowerInTree(currentUsersObject, string(params.KeyID), databaseConnector) {
		// 	return keys.NewWeaviateKeysChildrenGetForbidden()
		// }

		// // Get the children
		// var childIDs []string
		// childIDs = GetKeyChildren(databaseConnector, string(params.KeyID), true, childIDs, 1, 0)

		// // Format the IDs for the response
		// childUUIDs := make([]strfmt.UUID, len(childIDs))
		// for i, v := range childIDs {
		// 	childUUIDs[i] = strfmt.UUID(v)
		// }

		// // Initiate response object
		// responseObject := &models.KeyChildrenGetResponse{}
		// responseObject.Children = childUUIDs

		// // Return children with 'OK'
		// return keys.NewWeaviateKeysChildrenGetOK().WithPayload(responseObject)
		return keys.NewWeaviateKeysChildrenGetNotImplemented()
	})
	api.KeysWeaviateKeysDeleteHandler = keys.WeaviateKeysDeleteHandlerFunc(func(params keys.WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		// userObject, errGet := databaseConnector.GetKey(string(params.KeyID))

		// // Not found
		// if userObject.Deleted || errGet != nil {
		// 	return keys.NewWeaviateKeysDeleteNotFound()
		// }

		// // Check on permissions
		// currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)
		// if !isOwnKeyOrLowerInTree(currentUsersObject, string(params.KeyID), databaseConnector) {
		// 	return keys.NewWeaviateKeysDeleteForbidden()
		// }

		// // Remove key from database if found
		// deleteKey(databaseConnector, userObject.Uuid)

		// // Return 'No Content'
		// return keys.NewWeaviateKeysDeleteNoContent()
		return keys.NewWeaviateKeysDeleteNotImplemented()
	})
	api.KeysWeaviateKeysGetHandler = keys.WeaviateKeysGetHandlerFunc(func(params keys.WeaviateKeysGetParams, principal interface{}) middleware.Responder {
		// // Get item from database
		// userObject, err := databaseConnector.GetKey(string(params.KeyID))

		// // Object is deleted or not-existing
		// if userObject.Deleted || err != nil {
		// 	return keys.NewWeaviateKeysGetNotFound()
		// }

		// // Check on permissions
		// currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)
		// if !isOwnKeyOrLowerInTree(currentUsersObject, string(params.KeyID), databaseConnector) {
		// 	return keys.NewWeaviateKeysDeleteForbidden()
		// }

		// // Create response Object from create object.
		// responseObject := &models.KeyGetResponse{}
		// json.Unmarshal([]byte(userObject.Object), responseObject)
		// responseObject.KeyID = strfmt.UUID(userObject.Uuid)
		// responseObject.Parent = userObject.Parent
		// responseObject.KeyExpiresUnix = userObject.KeyExpiresUnix

		// // Get is successful
		// return keys.NewWeaviateKeysGetOK().WithPayload(responseObject)
		return keys.NewWeaviateKeysGetNotImplemented()
	})
	api.KeysWeaviateKeysMeChildrenGetHandler = keys.WeaviateKeysMeChildrenGetHandlerFunc(func(params keys.WeaviateKeysMeChildrenGetParams, principal interface{}) middleware.Responder {
		// // Create current User object from principal
		// currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// // Object is deleted or not-existing
		// if currentUsersObject.Deleted {
		// 	return keys.NewWeaviateKeysMeChildrenGetNotFound()
		// }

		// // Get the children
		// var childIDs []string
		// childIDs = GetKeyChildren(databaseConnector, currentUsersObject.Uuid, true, childIDs, 1, 0)

		// // Format the IDs for the response
		// childUUIDs := make([]strfmt.UUID, len(childIDs))
		// for i, v := range childIDs {
		// 	childUUIDs[i] = strfmt.UUID(v)
		// }

		// // Initiate response object
		// responseObject := &models.KeyChildrenGetResponse{}
		// responseObject.Children = childUUIDs

		// // Return children with 'OK'
		// return keys.NewWeaviateKeysMeChildrenGetOK().WithPayload(responseObject)
		return keys.NewWeaviateKeysMeChildrenGetNotImplemented()
	})
	api.KeysWeaviateKeysMeDeleteHandler = keys.WeaviateKeysMeDeleteHandlerFunc(func(params keys.WeaviateKeysMeDeleteParams, principal interface{}) middleware.Responder {
		// // Create current User object from principal
		// currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// // Object is deleted or not-existing
		// if currentUsersObject.Deleted {
		// 	return keys.NewWeaviateKeysMeDeleteNotFound()
		// }

		// // Change to Deleted
		// currentUsersObject.Deleted = true

		// // Remove key from database if found
		// deleteKey(databaseConnector, currentUsersObject.Uuid)

		// // Return 'No Content'
		// return keys.NewWeaviateKeysMeDeleteNoContent()
		return keys.NewWeaviateKeysMeDeleteNotImplemented()
	})
	api.KeysWeaviateKeysMeGetHandler = keys.WeaviateKeysMeGetHandlerFunc(func(params keys.WeaviateKeysMeGetParams, principal interface{}) middleware.Responder {
		// // Create current User object from principal
		// currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// // Init object
		// responseObject := &models.KeyTokenGetResponse{}

		// // Object is deleted or not-existing
		// if currentUsersObject.Deleted {
		// 	return keys.NewWeaviateKeysMeGetNotFound()
		// }

		// // Create response Object from create object.
		// json.Unmarshal([]byte(currentUsersObject.Object), responseObject)
		// responseObject.KeyID = strfmt.UUID(currentUsersObject.Uuid)
		// responseObject.Parent = currentUsersObject.Parent
		// responseObject.Key = currentUsersObject.KeyToken
		// responseObject.KeyExpiresUnix = currentUsersObject.KeyExpiresUnix

		// // Get is successful
		// return keys.NewWeaviateKeysMeGetOK().WithPayload(responseObject)
		return keys.NewWeaviateKeysMeGetNotImplemented()
	})

	/*
	 * HANDLE THINGS
	 */
	api.ThingsWeaviateThingsCreateHandler = things.WeaviateThingsCreateHandlerFunc(func(params things.WeaviateThingsCreateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, databaseConnector, nil); !allowed {
			return things.NewWeaviateThingsCreateForbidden()
		}

		// Generate UUID for the new object
		UUID := connector_utils.GenerateUUID()

		// Validate Schema given in body with the weaviate schema
		validatedErr := validateSchemaInBody(&databaseSchema.ThingSchema.Schema, &params.Body.Schema, params.Body.AtClass)
		if validatedErr != nil {
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte(fmt.Sprintf("{ \"ERROR\": \"%s\" }", validatedErr.Error())))
			})
		}

		// Make Thing-Object
		thingCreateJSON, _ := json.Marshal(params.Body)
		thing := &models.Thing{}
		json.Unmarshal([]byte(thingCreateJSON), thing)
		thing.CreationTimeUnix = connector_utils.NowUnix()
		thing.LastUpdateTimeUnix = 0

		// Save to DB, this needs to be a Go routine because we will return an accepted
		insertErr := databaseConnector.AddThing(thing, UUID, principal.(connector_utils.Key).UUID) // TODO: go-routine?
		if insertErr != nil {
			log.Println("InsertErr:", insertErr)
		}

		// Create response Object from create object.
		responseObject := &models.ThingGetResponse{}
		responseObject.Thing = *thing
		responseObject.ThingID = UUID
		url := "http://localhost/"
		responseObject.Key = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(connector_utils.Key).UUID,
			Type:         "Key",
		}

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return things.NewWeaviateThingsCreateAccepted().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsDeleteHandler = things.WeaviateThingsDeleteHandlerFunc(func(params things.WeaviateThingsDeleteParams, principal interface{}) middleware.Responder {
		// Get item from database
		thingGetResponse, errGet := databaseConnector.GetThing(params.ThingID)

		// Not found
		if errGet != nil {
			return things.NewWeaviateThingsDeleteNotFound()
		}

		// This is a delete function, validate if allowed to delete?
		if allowed, _ := ActionsAllowed([]string{"delete"}, principal, databaseConnector, thingGetResponse.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsDeleteForbidden()
		}

		// Add new row as GO-routine
		go databaseConnector.DeleteThing(params.ThingID)

		// Return 'No Content'
		return things.NewWeaviateThingsDeleteNoContent()
	})
	api.ThingsWeaviateThingsGetHandler = things.WeaviateThingsGetHandlerFunc(func(params things.WeaviateThingsGetParams, principal interface{}) middleware.Responder {
		// Get item from database
		responseObject, err := databaseConnector.GetThing(strfmt.UUID(params.ThingID))

		// Object is not found
		if err != nil {
			return things.NewWeaviateThingsGetNotFound()
		}

		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, databaseConnector, responseObject.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsGetForbidden()
		}

		// Get is successful
		return things.NewWeaviateThingsGetOK().WithPayload(&responseObject)
	})
	api.ThingsWeaviateThingsListHandler = things.WeaviateThingsListHandlerFunc(func(params things.WeaviateThingsListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, databaseConnector, nil); !allowed {
			return things.NewWeaviateThingsListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// Get user out of principal TODO
		// usersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// List all results
		thingsResponse, err := databaseConnector.ListThings(limit, page)

		if err != nil {
			log.Println("ERROR", err)
		}

		return things.NewWeaviateThingsListOK().WithPayload(&thingsResponse)
	})
	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal interface{}) middleware.Responder {
		// Get and transform object
		UUID := strfmt.UUID(params.ThingID)
		thingGetResponse, errGet := databaseConnector.GetThing(UUID)
		thingGetResponse.LastUpdateTimeUnix = connector_utils.NowUnix()

		// Return error if UUID is not found.
		if errGet != nil {
			return things.NewWeaviateThingsPatchNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, databaseConnector, thingGetResponse.Key.NrDollarCref); !allowed {
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
			return things.NewWeaviateThingsPatchUnprocessableEntity()
		}

		// Turn it into a Thing object
		thing := &models.Thing{}
		json.Unmarshal([]byte(updatedJSON), &thing)

		// Update the database
		insertErr := databaseConnector.UpdateThing(thing, UUID) // TODO: go-routine?
		if insertErr != nil {
			log.Println("InsertErr:", insertErr)
		}

		// Create return Object
		responseObject := &models.ThingGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &responseObject)
		responseObject.ThingID = UUID
		url := "http://localhost/"
		responseObject.Key = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(connector_utils.Key).UUID,
			Type:         "Key",
		}

		return things.NewWeaviateThingsPatchOK().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsUpdateHandler = things.WeaviateThingsUpdateHandlerFunc(func(params things.WeaviateThingsUpdateParams, principal interface{}) middleware.Responder {
		// Get item from database
		UUID := strfmt.UUID(params.ThingID)
		databaseResponseObject, errGet := databaseConnector.GetThing(UUID)

		// If there are no results, there is an error
		if errGet != nil {
			// Object not found response.
			return things.NewWeaviateThingsUpdateNotFound()
		}

		// This is a write function, validate if allowed to write?
		if allowed, _ := ActionsAllowed([]string{"write"}, principal, databaseConnector, databaseResponseObject.Key.NrDollarCref); !allowed {
			return things.NewWeaviateThingsUpdateForbidden()
		}

		// Validate Schema given in body with the weaviate schema
		validatedErr := validateSchemaInBody(&databaseSchema.ThingSchema.Schema, &params.Body.Schema, params.Body.AtClass)
		if validatedErr != nil {
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte(fmt.Sprintf("{ \"ERROR\": \"%s\" }", validatedErr.Error())))
			})
		}

		// Update the database
		params.Body.LastUpdateTimeUnix = connector_utils.NowUnix()
		params.Body.CreationTimeUnix = databaseResponseObject.CreationTimeUnix
		insertErr := databaseConnector.UpdateThing(&params.Body.Thing, UUID) // TODO: go-routine?
		if insertErr != nil {
			log.Println("InsertErr:", insertErr)
		}

		// Create object to return
		responseObject := &models.ThingGetResponse{}
		responseObject.Thing = params.Body.Thing
		responseObject.ThingID = UUID
		url := "http://localhost/"
		responseObject.Key = &models.SingleRef{
			LocationURL:  &url,
			NrDollarCref: principal.(connector_utils.Key).UUID,
			Type:         "Key",
		}

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return things.NewWeaviateThingsUpdateOK().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsValidateHandler = things.WeaviateThingsValidateHandlerFunc(func(params things.WeaviateThingsValidateParams, principal interface{}) middleware.Responder {
		// Validate Schema given in body with the weaviate schema
		validatedErr := validateSchemaInBody(&databaseSchema.ThingSchema.Schema, &params.Body.Schema, params.Body.AtClass)
		if validatedErr != nil {
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte(fmt.Sprintf("{ \"ERROR\": \"%s\" }", validatedErr.Error())))
			})
		}

		return things.NewWeaviateThingsValidateOK()
	})
	api.ThingsWeaviateThingsActionsListHandler = things.WeaviateThingsActionsListHandlerFunc(func(params things.WeaviateThingsActionsListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if allowed, _ := ActionsAllowed([]string{"read"}, principal, databaseConnector, nil); !allowed {
			return things.NewWeaviateThingsActionsListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// // Get user out of principal TODO
		// usersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// List all results
		actionsResponse, err := databaseConnector.ListActions(params.ThingID, limit, page)

		if err != nil {
			log.Println("ERROR", err)
		}

		return things.NewWeaviateThingsActionsListOK().WithPayload(&actionsResponse)
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
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
func setupMiddlewares(handler http.Handler) http.Handler {
	return handler
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	return handler
}
