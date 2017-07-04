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
	"io/ioutil"
	"log"
	"math"
	"net/http"

	"google.golang.org/grpc/grpclog"

	jsonpatch "github.com/evanphx/json-patch"
	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/runtime/yamlpc"
	"github.com/go-openapi/strfmt"
	graceful "github.com/tylerb/graceful"

	"github.com/weaviate/weaviate/mqtt"

	"github.com/weaviate/weaviate/connectors"
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/models"

	"reflect"
	"strings"
	"unicode"

	"fmt"

	"github.com/go-openapi/swag"
	gouuid "github.com/satori/go.uuid"
	"github.com/weaviate/weaviate/restapi/operations"
	"github.com/weaviate/weaviate/restapi/operations/commands"
	"github.com/weaviate/weaviate/restapi/operations/events"
	"github.com/weaviate/weaviate/restapi/operations/groups"
	"github.com/weaviate/weaviate/restapi/operations/keys"
	"github.com/weaviate/weaviate/restapi/operations/locations"
	"github.com/weaviate/weaviate/restapi/operations/thing_templates"
	"github.com/weaviate/weaviate/restapi/operations/things"
)

const refTypeCommand string = "#/paths/commands"
const refTypeEvent string = "#/paths/events"
const refTypeGroup string = "#/paths/groups"
const refTypeLocation string = "#/paths/locations"
const refTypeThing string = "#/paths/things"
const refTypeThingTemplate string = "#/paths/thingTemplates"
const maxResultsOverride int64 = 100
const pageOverride int64 = 1

var connectorOptionGroup *swag.CommandLineOptionsGroup

func init() {
	discard := ioutil.Discard
	myGRPCLogger := log.New(discard, "", log.LstdFlags)
	grpclog.SetLogger(myGRPCLogger)
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
func isOwnKeyOrLowerInTree(currentUsersObject connector_utils.DatabaseUsersObject, userKeyID string, databaseConnector dbconnector.DatabaseConnector) bool {
	// If is own key, return true
	if strings.EqualFold(userKeyID, currentUsersObject.Uuid) {
		return true
	}

	// Get all child id's
	var childIDs []string
	childIDs = databaseConnector.GetChildKeys(currentUsersObject.Uuid, childIDs, 0, 0)

	// Check ID is in childIds
	isChildID := false
	for _, childID := range childIDs {
		if childID == userKeyID {
			isChildID = true
		}
	}

	// This is a delete function, validate if allowed to do action with own/parent.
	if isChildID {
		return true
	}

	return false
}

func configureFlags(api *operations.WeaviateAPI) {
	connectorOptionGroup = dbconnector.GetConfigOptionGroup()

	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		*connectorOptionGroup,
	}
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	// configure database connection
	var databaseConnector dbconnector.DatabaseConnector

	// Determine the database name and use that name to create a connection.
	databaseConnector = dbconnector.CreateDatabaseConnector(connectorOptionGroup)

	// connect the database
	errConnect := databaseConnector.Connect()
	if errConnect != nil {
		panic(errConnect)
	}

	// init the database
	errInit := databaseConnector.Init()
	if errInit != nil {
		panic(errInit)
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
		validatedKey, _ := databaseConnector.ValidateKey(token)

		if len(validatedKey) == 0 {
			return nil, errors.New(401, "Provided key is not valid")
		}

		// key is valid, next step is allowing per Handler handling
		return validatedKey[0], nil

	}

	/*
	 * HANDLE COMMANDS
	 */
	api.CommandsWeaviateCommandsCreateHandler = commands.WeaviateCommandsCreateHandlerFunc(func(params commands.WeaviateCommandsCreateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return commands.NewWeaviateCommandsCreateForbidden()
		}

		// Create basic DataBase object
		dbObject := *connector_utils.NewDatabaseObjectFromPrincipal(principal, refTypeCommand)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		responseObject := &models.CommandGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return commands.NewWeaviateCommandsCreateAccepted().WithPayload(responseObject)
	})
	api.CommandsWeaviateCommandsDeleteHandler = commands.WeaviateCommandsDeleteHandlerFunc(func(params commands.WeaviateCommandsDeleteParams, principal interface{}) middleware.Responder {
		// This is a delete function, validate if allowed to read?
		if connector_utils.DeleteAllowed(principal) == false {
			return commands.NewWeaviateCommandsDeleteForbidden()
		}

		// Get item from database
		databaseObject, errGet := databaseConnector.Get(string(params.CommandID))

		// Not found
		if databaseObject.Deleted || errGet != nil {
			return commands.NewWeaviateCommandsDeleteNotFound()
		}

		// Set deleted values
		databaseObject.MakeObjectDeleted()

		// Add new row as GO-routine
		go databaseConnector.Add(databaseObject)

		// Return 'No Content'
		return commands.NewWeaviateCommandsDeleteNoContent()
	})
	api.CommandsWeaviateCommandsGetHandler = commands.WeaviateCommandsGetHandlerFunc(func(params commands.WeaviateCommandsGetParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return commands.NewWeaviateCommandsGetForbidden()
		}

		// Get item from database
		dbObject, err := databaseConnector.Get(params.CommandID)

		// Object is deleted eleted
		if dbObject.Deleted || err != nil {
			return commands.NewWeaviateCommandsGetNotFound()
		}

		// Create object to return
		responseObject := &models.CommandGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Get is successful
		return commands.NewWeaviateCommandsGetOK().WithPayload(responseObject)
	})
	api.CommandsWeaviateCommandsListHandler = commands.WeaviateCommandsListHandlerFunc(func(params commands.WeaviateCommandsListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return commands.NewWeaviateCommandsListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// List all results
		commandsDatabaseObjects, totalResults, _ := databaseConnector.List(refTypeCommand, limit, page, nil)

		// Convert to an response object
		responseObject := &models.CommandsListResponse{}
		responseObject.Commands = make([]*models.CommandGetResponse, len(commandsDatabaseObjects))

		// Loop to fill response project
		for i, commandDatabaseObject := range commandsDatabaseObjects {
			commandObject := &models.CommandGetResponse{}
			json.Unmarshal([]byte(commandDatabaseObject.Object), commandObject)
			commandObject.ID = strfmt.UUID(commandDatabaseObject.Uuid)
			responseObject.Commands[i] = commandObject
		}

		// Add totalResults to response object.
		responseObject.TotalResults = totalResults
		responseObject.Kind = getKind(responseObject)

		return commands.NewWeaviateCommandsListOK().WithPayload(responseObject)
	})
	api.CommandsWeaviateCommandsPatchHandler = commands.WeaviateCommandsPatchHandlerFunc(func(params commands.WeaviateCommandsPatchParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return commands.NewWeaviateCommandsPatchForbidden()
		}

		// Get and transform object
		UUID := params.CommandID
		dbObject, errGet := databaseConnector.Get(UUID)

		// Return error if UUID is not found.
		if dbObject.Deleted || errGet != nil {
			return commands.NewWeaviateCommandsPatchNotFound()
		}

		// Get PATCH params in format RFC 6902
		jsonBody, marshalErr := json.Marshal(params.Body)
		patchObject, decodeErr := jsonpatch.DecodePatch([]byte(jsonBody))

		if marshalErr != nil || decodeErr != nil {
			return commands.NewWeaviateCommandsPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply([]byte(dbObject.Object))

		if applyErr != nil {
			return commands.NewWeaviateCommandsPatchUnprocessableEntity()
		}

		// Set patched JSON back in dbObject
		dbObject.Object = string(updatedJSON)

		dbObject.SetCreateTimeMsToNow()
		go databaseConnector.Add(dbObject)

		// Create return Object
		responseObject := &models.CommandGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		return commands.NewWeaviateCommandsPatchOK().WithPayload(responseObject)
	})
	api.CommandsWeaviateCommandsUpdateHandler = commands.WeaviateCommandsUpdateHandlerFunc(func(params commands.WeaviateCommandsUpdateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return commands.NewWeaviateCommandsUpdateForbidden()
		}

		// Get item from database
		UUID := params.CommandID
		dbObject, errGet := databaseConnector.Get(UUID)

		// If there are no results, there is an error
		if dbObject.Deleted || errGet != nil {
			// Object not found response.
			return commands.NewWeaviateCommandsUpdateNotFound()
		}

		// Set the body-id and generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)
		dbObject.SetCreateTimeMsToNow()

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create object to return
		responseObject := &models.CommandGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return commands.NewWeaviateCommandsUpdateOK().WithPayload(responseObject)
	})

	/*
	 * HANDLE EVENTS
	 */
	api.EventsWeaviateEventsGetHandler = events.WeaviateEventsGetHandlerFunc(func(params events.WeaviateEventsGetParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return events.NewWeaviateEventsGetForbidden()
		}

		// Get item from database
		dbObject, err := databaseConnector.Get(string(params.EventID))

		// Object is deleted eleted
		if dbObject.Deleted || err != nil {
			return events.NewWeaviateEventsGetNotFound()
		}

		// Create object to return
		responseObject := &models.EventGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Get is successful
		return events.NewWeaviateEventsGetOK().WithPayload(responseObject)
	})
	api.EventsWeaviateEventsValidateHandler = events.WeaviateEventsValidateHandlerFunc(func(params events.WeaviateEventsValidateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateEventsValidate has not yet been implemented")
	})
	api.EventsWeaviateGroupsEventsCreateHandler = events.WeaviateGroupsEventsCreateHandlerFunc(func(params events.WeaviateGroupsEventsCreateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateGroupsEventsCreate has not yet been implemented")
	})
	api.EventsWeaviateGroupsEventsListHandler = events.WeaviateGroupsEventsListHandlerFunc(func(params events.WeaviateGroupsEventsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateGroupsEventsList has not yet been implemented")
	})
	api.EventsWeaviateThingsEventsCreateHandler = events.WeaviateThingsEventsCreateHandlerFunc(func(params events.WeaviateThingsEventsCreateParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return events.NewWeaviateThingsEventsCreateForbidden()
		}

		// Get ThingID from URL
		thingID := strfmt.UUID(params.ThingID)

		// Create basic DataBase object
		dbObject := *connector_utils.NewDatabaseObjectFromPrincipal(principal, refTypeEvent)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)
		dbObject.RelatedObjects.ThingID = thingID

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		responseObject := &models.EventGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)
		responseObject.ThingID = thingID

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return events.NewWeaviateThingsEventsCreateAccepted().WithPayload(responseObject)
	})
	api.EventsWeaviateThingsEventsListHandler = events.WeaviateThingsEventsListHandlerFunc(func(params events.WeaviateThingsEventsListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return events.NewWeaviateThingsEventsListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)
		referenceFilter := &connector_utils.ObjectReferences{ThingID: params.ThingID}

		// List all results
		eventsDatabaseObjects, totalResults, _ := databaseConnector.List(refTypeEvent, limit, page, referenceFilter)

		// Convert to an response object
		responseObject := &models.EventsListResponse{}
		responseObject.Events = make([]*models.EventGetResponse, len(eventsDatabaseObjects))

		// Loop to fill response project
		for i, eventsDatabaseObject := range eventsDatabaseObjects {
			eventObject := &models.EventGetResponse{}
			json.Unmarshal([]byte(eventsDatabaseObject.Object), eventObject)
			eventObject.ID = strfmt.UUID(eventsDatabaseObject.Uuid)
			responseObject.Events[i] = eventObject
		}

		// Add totalResults to response object.
		responseObject.TotalResults = totalResults
		responseObject.Kind = getKind(responseObject)

		return events.NewWeaviateThingsEventsListOK().WithPayload(responseObject)
	})

	/*
	 * HANDLE GROUPS
	 */
	api.GroupsWeaviateGroupsCreateHandler = groups.WeaviateGroupsCreateHandlerFunc(func(params groups.WeaviateGroupsCreateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return groups.NewWeaviateGroupsCreateForbidden()
		}

		// Create basic DataBase object
		dbObject := *connector_utils.NewDatabaseObjectFromPrincipal(principal, refTypeGroup)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		responseObject := &models.GroupGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return groups.NewWeaviateGroupsCreateAccepted().WithPayload(responseObject)
	})
	api.GroupsWeaviateGroupsDeleteHandler = groups.WeaviateGroupsDeleteHandlerFunc(func(params groups.WeaviateGroupsDeleteParams, principal interface{}) middleware.Responder {
		// This is a delete function, validate if allowed to read?
		if connector_utils.DeleteAllowed(principal) == false {
			return groups.NewWeaviateGroupsDeleteForbidden()
		}

		// Get item from database
		databaseObject, errGet := databaseConnector.Get(string(params.GroupID))

		// Not found
		if databaseObject.Deleted || errGet != nil {
			return groups.NewWeaviateGroupsDeleteNotFound()
		}

		// Set deleted values
		databaseObject.MakeObjectDeleted()

		// Add new row as GO-routine
		go databaseConnector.Add(databaseObject)

		// Return 'No Content'
		return groups.NewWeaviateGroupsDeleteNoContent()
	})
	api.GroupsWeaviateGroupsGetHandler = groups.WeaviateGroupsGetHandlerFunc(func(params groups.WeaviateGroupsGetParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return groups.NewWeaviateGroupsGetForbidden()
		}

		// Get item from database
		dbObject, err := databaseConnector.Get(params.GroupID)

		// Object is deleted eleted
		if dbObject.Deleted || err != nil {
			return groups.NewWeaviateGroupsGetNotFound()
		}

		// Create object to return
		responseObject := &models.GroupGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Get is successful
		return groups.NewWeaviateGroupsGetOK().WithPayload(responseObject)
	})
	api.GroupsWeaviateGroupsListHandler = groups.WeaviateGroupsListHandlerFunc(func(params groups.WeaviateGroupsListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return groups.NewWeaviateGroupsListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// List all results
		groupsDatabaseObjects, totalResults, _ := databaseConnector.List(refTypeGroup, limit, page, nil)

		// Convert to an response object
		responseObject := &models.GroupsListResponse{}
		responseObject.Groups = make([]*models.GroupGetResponse, len(groupsDatabaseObjects))

		// Loop to fill response project
		for i, groupsDatabaseObject := range groupsDatabaseObjects {
			groupObject := &models.GroupGetResponse{}
			json.Unmarshal([]byte(groupsDatabaseObject.Object), groupObject)
			groupObject.ID = strfmt.UUID(groupsDatabaseObject.Uuid)
			responseObject.Groups[i] = groupObject
		}

		// Add totalResults to response object.
		responseObject.TotalResults = totalResults
		responseObject.Kind = getKind(responseObject)

		return groups.NewWeaviateGroupsListOK().WithPayload(responseObject)
	})
	api.GroupsWeaviateGroupsPatchHandler = groups.WeaviateGroupsPatchHandlerFunc(func(params groups.WeaviateGroupsPatchParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return groups.NewWeaviateGroupsPatchForbidden()
		}

		// Get and transform object
		UUID := params.GroupID
		dbObject, errGet := databaseConnector.Get(UUID)

		// Return error if UUID is not found.
		if dbObject.Deleted || errGet != nil {
			return groups.NewWeaviateGroupsPatchNotFound()
		}

		// Get PATCH params in format RFC 6902
		jsonBody, marshalErr := json.Marshal(params.Body)
		patchObject, decodeErr := jsonpatch.DecodePatch([]byte(jsonBody))

		if marshalErr != nil || decodeErr != nil {
			return groups.NewWeaviateGroupsPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply([]byte(dbObject.Object))

		if applyErr != nil {
			return groups.NewWeaviateGroupsPatchUnprocessableEntity()
		}

		// Set patched JSON back in dbObject
		dbObject.Object = string(updatedJSON)

		dbObject.SetCreateTimeMsToNow()
		go databaseConnector.Add(dbObject)

		// Create return Object
		responseObject := &models.GroupGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		return groups.NewWeaviateGroupsPatchOK().WithPayload(responseObject)
	})
	api.GroupsWeaviateGroupsUpdateHandler = groups.WeaviateGroupsUpdateHandlerFunc(func(params groups.WeaviateGroupsUpdateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return groups.NewWeaviateGroupsUpdateForbidden()
		}

		// Get item from database
		UUID := params.GroupID
		dbObject, errGet := databaseConnector.Get(UUID)

		// If there are no results, there is an error
		if dbObject.Deleted || errGet != nil {
			// Object not found response.
			return groups.NewWeaviateGroupsUpdateNotFound()
		}

		// Set the body-id and generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)
		dbObject.SetCreateTimeMsToNow()

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create object to return
		responseObject := &models.GroupGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return groups.NewWeaviateGroupsUpdateOK().WithPayload(responseObject)
	})

	/*
	 * HANDLE KEYS
	 */
	api.KeysWeaviateKeyCreateHandler = keys.WeaviateKeyCreateHandlerFunc(func(params keys.WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
		// Create current User object from principal
		currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// Fill the new User object
		newUsersObject := &connector_utils.DatabaseUsersObject{}
		newUsersObject.Deleted = false
		newUsersObject.KeyExpiresUnix = int64(params.Body.KeyExpiresUnix)
		newUsersObject.Uuid = fmt.Sprintf("%v", gouuid.NewV4())
		newUsersObject.KeyToken = fmt.Sprintf("%v", gouuid.NewV4())
		newUsersObject.Parent = currentUsersObject.Uuid

		// Fill in the string-Object of the User
		objectsBody, _ := json.Marshal(params.Body)
		newUsersObjectsObject := &connector_utils.DatabaseUsersObjectsObject{}
		json.Unmarshal(objectsBody, newUsersObjectsObject)
		databaseBody, _ := json.Marshal(newUsersObjectsObject)
		newUsersObject.Object = string(databaseBody)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.AddKey(currentUsersObject.Uuid, *newUsersObject)

		// Create response Object from create object.
		responseObject := &models.KeyTokenGetResponse{}
		json.Unmarshal([]byte(newUsersObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(newUsersObject.Uuid)
		responseObject.Kind = getKind(responseObject)
		responseObject.Key = newUsersObject.KeyToken
		responseObject.Parent = newUsersObject.Parent
		responseObject.KeyExpiresUnix = newUsersObject.KeyExpiresUnix

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return keys.NewWeaviateKeyCreateAccepted().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysChildrenGetHandler = keys.WeaviateKeysChildrenGetHandlerFunc(func(params keys.WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		userObject, errGet := databaseConnector.GetKey(string(params.KeyID))

		// Not found
		if userObject.Deleted || errGet != nil {
			return keys.NewWeaviateKeysChildrenGetNotFound()
		}

		// Check on permissions
		currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)
		if !isOwnKeyOrLowerInTree(currentUsersObject, string(params.KeyID), databaseConnector) {
			return keys.NewWeaviateKeysChildrenGetForbidden()
		}

		// Get the children
		var childIDs []string
		childIDs = databaseConnector.GetChildKeys(string(params.KeyID), childIDs, 1, 0)

		// Format the IDs for the response
		childUUIDs := make([]strfmt.UUID, len(childIDs))
		for i, v := range childIDs {
			childUUIDs[i] = strfmt.UUID(v)
		}

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = childUUIDs

		// Return children with 'OK'
		return keys.NewWeaviateKeysChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysDeleteHandler = keys.WeaviateKeysDeleteHandlerFunc(func(params keys.WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
		// First check on 'not found', otherwise it will say 'forbidden' in stead of 'not found'
		userObject, errGet := databaseConnector.GetKey(string(params.KeyID))

		// Not found
		if userObject.Deleted || errGet != nil {
			return keys.NewWeaviateKeysDeleteNotFound()
		}

		// Check on permissions
		currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)
		if !isOwnKeyOrLowerInTree(currentUsersObject, string(params.KeyID), databaseConnector) {
			return keys.NewWeaviateKeysDeleteForbidden()
		}

		// Remove key from database if found
		userObject.Deleted = true
		errDel := databaseConnector.DeleteKey(userObject)
		if errDel != nil {
			return keys.NewWeaviateKeysDeleteNotFound()
		}

		// Return 'No Content'
		return keys.NewWeaviateKeysDeleteNoContent()
	})
	api.KeysWeaviateKeysGetHandler = keys.WeaviateKeysGetHandlerFunc(func(params keys.WeaviateKeysGetParams, principal interface{}) middleware.Responder {
		// Get item from database
		userObject, err := databaseConnector.GetKey(string(params.KeyID))

		// Object is deleted or not-existing
		if userObject.Deleted || err != nil {
			return keys.NewWeaviateKeysGetNotFound()
		}

		// Check on permissions
		currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)
		if !isOwnKeyOrLowerInTree(currentUsersObject, string(params.KeyID), databaseConnector) {
			return locations.NewWeaviateLocationsDeleteForbidden()
		}

		// Create response Object from create object.
		responseObject := &models.KeyGetResponse{}
		json.Unmarshal([]byte(userObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(userObject.Uuid)
		responseObject.Kind = getKind(responseObject)
		responseObject.Parent = userObject.Parent
		responseObject.KeyExpiresUnix = userObject.KeyExpiresUnix

		// Get is successful
		return keys.NewWeaviateKeysGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysMeChildrenGetHandler = keys.WeaviateKeysMeChildrenGetHandlerFunc(func(params keys.WeaviateKeysMeChildrenGetParams, principal interface{}) middleware.Responder {
		// Create current User object from principal
		currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// Object is deleted or not-existing
		if currentUsersObject.Deleted {
			return keys.NewWeaviateKeysMeChildrenGetNotFound()
		}

		// Get the children
		var childIDs []string
		childIDs = databaseConnector.GetChildKeys(currentUsersObject.Uuid, childIDs, 1, 0)

		// Format the IDs for the response
		childUUIDs := make([]strfmt.UUID, len(childIDs))
		for i, v := range childIDs {
			childUUIDs[i] = strfmt.UUID(v)
		}

		// Initiate response object
		responseObject := &models.KeyChildrenGetResponse{}
		responseObject.Children = childUUIDs

		// Return children with 'OK'
		return keys.NewWeaviateKeysMeChildrenGetOK().WithPayload(responseObject)
	})
	api.KeysWeaviateKeysMeDeleteHandler = keys.WeaviateKeysMeDeleteHandlerFunc(func(params keys.WeaviateKeysMeDeleteParams, principal interface{}) middleware.Responder {
		// Create current User object from principal
		currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// Object is deleted or not-existing
		if currentUsersObject.Deleted {
			return keys.NewWeaviateKeysMeDeleteNotFound()
		}

		// Change to Deleted
		currentUsersObject.Deleted = true

		// Remove key from database if found
		errDel := databaseConnector.DeleteKey(currentUsersObject)
		if errDel != nil {
			return keys.NewWeaviateKeysMeDeleteNotFound()
		}

		// Return 'No Content'
		return keys.NewWeaviateKeysMeDeleteNoContent()

	})
	api.KeysWeaviateKeysMeGetHandler = keys.WeaviateKeysMeGetHandlerFunc(func(params keys.WeaviateKeysMeGetParams, principal interface{}) middleware.Responder {
		// Create current User object from principal
		currentUsersObject, _ := connector_utils.PrincipalMarshalling(principal)

		// Init object
		responseObject := &models.KeyTokenGetResponse{}

		// Object is deleted or not-existing
		if currentUsersObject.Deleted {
			return keys.NewWeaviateKeysMeGetNotFound()
		}

		// Create response Object from create object.
		json.Unmarshal([]byte(currentUsersObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(currentUsersObject.Uuid)
		responseObject.Kind = getKind(responseObject)
		responseObject.Parent = currentUsersObject.Parent
		responseObject.Key = currentUsersObject.KeyToken
		responseObject.KeyExpiresUnix = currentUsersObject.KeyExpiresUnix

		// Get is successful
		return keys.NewWeaviateKeysMeGetOK().WithPayload(responseObject)
	})

	/*
	 * HANDLE LOCATIONS
	 */
	api.LocationsWeaviateLocationsDeleteHandler = locations.WeaviateLocationsDeleteHandlerFunc(func(params locations.WeaviateLocationsDeleteParams, principal interface{}) middleware.Responder {

		// This is a delete function, validate if allowed to read?
		if connector_utils.DeleteAllowed(principal) == false {
			return locations.NewWeaviateLocationsDeleteForbidden()
		}

		// Get item from database
		databaseObject, errGet := databaseConnector.Get(string(params.LocationID))

		// Not found
		if databaseObject.Deleted || errGet != nil {
			return locations.NewWeaviateLocationsDeleteNotFound()
		}

		// Set deleted values
		databaseObject.MakeObjectDeleted()

		// Add new row as GO-routine
		go databaseConnector.Add(databaseObject)

		// Return 'No Content'
		return locations.NewWeaviateLocationsDeleteNoContent()
	})
	api.LocationsWeaviateLocationsGetHandler = locations.WeaviateLocationsGetHandlerFunc(func(params locations.WeaviateLocationsGetParams, principal interface{}) middleware.Responder {

		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return locations.NewWeaviateLocationsGetForbidden()
		}

		// Get item from database
		dbObject, err := databaseConnector.Get(params.LocationID)

		// Object is deleted eleted
		if dbObject.Deleted || err != nil {
			return locations.NewWeaviateLocationsGetNotFound()
		}

		// Create object to return
		responseObject := &models.LocationGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Get is successful
		return locations.NewWeaviateLocationsGetOK().WithPayload(responseObject)
	})
	api.LocationsWeaviateLocationsCreateHandler = locations.WeaviateLocationsCreateHandlerFunc(func(params locations.WeaviateLocationsCreateParams, principal interface{}) middleware.Responder {

		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return locations.NewWeaviateLocationsCreateForbidden()
		}

		// Create basic DataBase object
		dbObject := *connector_utils.NewDatabaseObjectFromPrincipal(principal, refTypeLocation)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		responseObject := &models.LocationGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return locations.NewWeaviateLocationsCreateAccepted().WithPayload(responseObject)
	})
	api.LocationsWeaviateLocationsListHandler = locations.WeaviateLocationsListHandlerFunc(func(params locations.WeaviateLocationsListParams, principal interface{}) middleware.Responder {

		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return locations.NewWeaviateLocationsListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// List all results
		locationDatabaseObjects, totalResults, _ := databaseConnector.List(refTypeLocation, limit, page, nil)

		// Convert to an response object
		responseObject := &models.LocationsListResponse{}
		responseObject.Locations = make([]*models.LocationGetResponse, len(locationDatabaseObjects))

		// Loop to fill response project
		for i, locationDatabaseObject := range locationDatabaseObjects {
			locationObject := &models.LocationGetResponse{}
			json.Unmarshal([]byte(locationDatabaseObject.Object), locationObject)
			locationObject.ID = strfmt.UUID(locationDatabaseObject.Uuid)
			locationObject.Kind = getKind(locationObject)
			responseObject.Locations[i] = locationObject
		}

		// Add totalResults to response object.
		responseObject.TotalResults = totalResults
		responseObject.Kind = getKind(responseObject)

		return locations.NewWeaviateLocationsListOK().WithPayload(responseObject)
	})
	api.LocationsWeaviateLocationsPatchHandler = locations.WeaviateLocationsPatchHandlerFunc(func(params locations.WeaviateLocationsPatchParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return locations.NewWeaviateLocationsPatchForbidden()
		}

		// Get and transform object
		UUID := params.LocationID
		dbObject, errGet := databaseConnector.Get(UUID)

		// Return error if UUID is not found.
		if dbObject.Deleted || errGet != nil {
			return locations.NewWeaviateLocationsPatchNotFound()
		}

		// Get PATCH params in format RFC 6902
		jsonBody, marshalErr := json.Marshal(params.Body)
		patchObject, decodeErr := jsonpatch.DecodePatch([]byte(jsonBody))

		if marshalErr != nil || decodeErr != nil {
			return locations.NewWeaviateLocationsPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply([]byte(dbObject.Object))

		if applyErr != nil {
			return locations.NewWeaviateLocationsPatchUnprocessableEntity()
		}

		// Set patched JSON back in dbObject
		dbObject.Object = string(updatedJSON)

		dbObject.SetCreateTimeMsToNow()
		go databaseConnector.Add(dbObject)

		// Create return Object
		responseObject := &models.LocationGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		return locations.NewWeaviateLocationsPatchOK().WithPayload(responseObject)
	})
	api.LocationsWeaviateLocationsUpdateHandler = locations.WeaviateLocationsUpdateHandlerFunc(func(params locations.WeaviateLocationsUpdateParams, principal interface{}) middleware.Responder {

		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return locations.NewWeaviateLocationsUpdateForbidden()
		}

		// Get item from database
		UUID := params.LocationID
		dbObject, errGet := databaseConnector.Get(UUID)

		// If there are no results, there is an error
		if dbObject.Deleted || errGet != nil {
			// Object not found response.
			return locations.NewWeaviateLocationsUpdateNotFound()
		}

		// Set the body-id and generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)
		dbObject.SetCreateTimeMsToNow()

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create object to return
		responseObject := &models.LocationGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return locations.NewWeaviateLocationsUpdateOK().WithPayload(responseObject)
	})

	/*
	 * HANDLE THING TEMPLATES
	 */
	api.ThingTemplatesWeaviateThingTemplatesCreateHandler = thing_templates.WeaviateThingTemplatesCreateHandlerFunc(func(params thing_templates.WeaviateThingTemplatesCreateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesCreateForbidden()
		}

		// Create basic DataBase object
		dbObject := *connector_utils.NewDatabaseObjectFromPrincipal(principal, refTypeThingTemplate)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		responseObject := &models.ThingTemplateGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return thing_templates.NewWeaviateThingTemplatesCreateAccepted().WithPayload(responseObject)
	})
	api.ThingTemplatesWeaviateThingTemplatesDeleteHandler = thing_templates.WeaviateThingTemplatesDeleteHandlerFunc(func(params thing_templates.WeaviateThingTemplatesDeleteParams, principal interface{}) middleware.Responder {
		// This is a delete function, validate if allowed to read?
		if connector_utils.DeleteAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesDeleteForbidden()
		}

		// Get item from database
		databaseObject, errGet := databaseConnector.Get(string(params.ThingTemplateID))

		// Not found
		if databaseObject.Deleted || errGet != nil {
			return thing_templates.NewWeaviateThingTemplatesDeleteNotFound()
		}

		// Set deleted values
		databaseObject.MakeObjectDeleted()

		// Add new row as GO-routine
		go databaseConnector.Add(databaseObject)

		// Return 'No Content'
		return thing_templates.NewWeaviateThingTemplatesDeleteNoContent()
	})
	api.ThingTemplatesWeaviateThingTemplatesGetHandler = thing_templates.WeaviateThingTemplatesGetHandlerFunc(func(params thing_templates.WeaviateThingTemplatesGetParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesGetForbidden()
		}

		// Get item from database
		dbObject, err := databaseConnector.Get(params.ThingTemplateID)

		// Object is deleted eleted
		if dbObject.Deleted || err != nil {
			return thing_templates.NewWeaviateThingTemplatesGetNotFound()
		}

		// Create object to return
		responseObject := &models.ThingTemplateGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Get is successful
		return thing_templates.NewWeaviateThingTemplatesGetOK().WithPayload(responseObject)
	})
	api.ThingTemplatesWeaviateThingTemplatesListHandler = thing_templates.WeaviateThingTemplatesListHandlerFunc(func(params thing_templates.WeaviateThingTemplatesListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// List all results
		thingTemplatesDatabaseObjects, totalResults, _ := databaseConnector.List(refTypeThingTemplate, limit, page, nil)

		// Convert to an response object
		responseObject := &models.ThingTemplatesListResponse{}
		responseObject.ThingTemplates = make([]*models.ThingTemplateGetResponse, len(thingTemplatesDatabaseObjects))

		// Loop to fill response project
		for i, thingTemplatesDatabaseObject := range thingTemplatesDatabaseObjects {
			thingTemplateObject := &models.ThingTemplateGetResponse{}
			json.Unmarshal([]byte(thingTemplatesDatabaseObject.Object), thingTemplateObject)
			thingTemplateObject.ID = strfmt.UUID(thingTemplatesDatabaseObject.Uuid)
			thingTemplateObject.Kind = getKind(thingTemplateObject)
			responseObject.ThingTemplates[i] = thingTemplateObject
		}

		// Add totalResults to response object.
		responseObject.TotalResults = totalResults
		responseObject.Kind = getKind(responseObject)

		return thing_templates.NewWeaviateThingTemplatesListOK().WithPayload(responseObject)
	})
	api.ThingTemplatesWeaviateThingTemplatesPatchHandler = thing_templates.WeaviateThingTemplatesPatchHandlerFunc(func(params thing_templates.WeaviateThingTemplatesPatchParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesPatchForbidden()
		}

		// Get and transform object
		UUID := params.ThingTemplateID
		dbObject, errGet := databaseConnector.Get(UUID)

		// Return error if UUID is not found.
		if dbObject.Deleted || errGet != nil {
			return thing_templates.NewWeaviateThingTemplatesPatchNotFound()
		}

		// Get PATCH params in format RFC 6902
		jsonBody, marshalErr := json.Marshal(params.Body)
		patchObject, decodeErr := jsonpatch.DecodePatch([]byte(jsonBody))

		if marshalErr != nil || decodeErr != nil {
			return thing_templates.NewWeaviateThingTemplatesPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply([]byte(dbObject.Object))

		if applyErr != nil {
			return thing_templates.NewWeaviateThingTemplatesPatchUnprocessableEntity()
		}

		// Set patched JSON back in dbObject
		dbObject.Object = string(updatedJSON)

		dbObject.SetCreateTimeMsToNow()
		go databaseConnector.Add(dbObject)

		// Create return Object
		responseObject := &models.ThingTemplateGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		return thing_templates.NewWeaviateThingTemplatesPatchOK().WithPayload(responseObject)
	})
	api.ThingTemplatesWeaviateThingTemplatesUpdateHandler = thing_templates.WeaviateThingTemplatesUpdateHandlerFunc(func(params thing_templates.WeaviateThingTemplatesUpdateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesUpdateForbidden()
		}

		// Get item from database
		UUID := params.ThingTemplateID
		dbObject, errGet := databaseConnector.Get(UUID)

		// If there are no results, there is an error
		if dbObject.Deleted || errGet != nil {
			// Object not found response.
			return thing_templates.NewWeaviateThingTemplatesUpdateNotFound()
		}

		// Set the body-id and generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)
		dbObject.SetCreateTimeMsToNow()

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create object to return
		responseObject := &models.ThingTemplateGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return thing_templates.NewWeaviateThingTemplatesUpdateOK().WithPayload(responseObject)
	})

	/*
	 * HANDLE THINGS
	 */
	api.ThingsWeaviateThingsCreateHandler = things.WeaviateThingsCreateHandlerFunc(func(params things.WeaviateThingsCreateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return things.NewWeaviateThingsCreateForbidden()
		}

		// Create basic DataBase object
		dbObject := *connector_utils.NewDatabaseObjectFromPrincipal(principal, refTypeThing)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		responseObject := &models.ThingGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return things.NewWeaviateThingsCreateAccepted().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsDeleteHandler = things.WeaviateThingsDeleteHandlerFunc(func(params things.WeaviateThingsDeleteParams, principal interface{}) middleware.Responder {
		// This is a delete function, validate if allowed to read?
		if connector_utils.DeleteAllowed(principal) == false {
			return things.NewWeaviateThingsDeleteForbidden()
		}

		// Get item from database
		databaseObject, errGet := databaseConnector.Get(string(params.ThingID))

		// Not found
		if databaseObject.Deleted || errGet != nil {
			return things.NewWeaviateThingsDeleteNotFound()
		}

		// Set deleted values
		databaseObject.MakeObjectDeleted()

		// Add new row as GO-routine
		go databaseConnector.Add(databaseObject)

		// Return 'No Content'
		return things.NewWeaviateThingsDeleteNoContent()
	})
	api.ThingsWeaviateThingsGetHandler = things.WeaviateThingsGetHandlerFunc(func(params things.WeaviateThingsGetParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return things.NewWeaviateThingsGetForbidden()
		}

		// Get item from database
		dbObject, err := databaseConnector.Get(params.ThingID)

		// Object is deleted eleted
		if dbObject.Deleted || err != nil {
			return things.NewWeaviateThingsGetNotFound()
		}

		// Create object to return
		responseObject := &models.ThingGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Get is successful
		return things.NewWeaviateThingsGetOK().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsListHandler = things.WeaviateThingsListHandlerFunc(func(params things.WeaviateThingsListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if connector_utils.ReadAllowed(principal) == false {
			return things.NewWeaviateThingsListForbidden()
		}

		// Get limit and page
		limit := getLimit(params.MaxResults)
		page := getPage(params.Page)

		// List all results
		thingDatabaseObjects, totalResults, _ := databaseConnector.List(refTypeThing, limit, page, nil)

		// Convert to an response object
		responseObject := &models.ThingsListResponse{}
		responseObject.Things = make([]*models.ThingGetResponse, len(thingDatabaseObjects))

		// Loop to fill response project
		for i, thingDatabaseObject := range thingDatabaseObjects {
			thingObject := &models.ThingGetResponse{}
			json.Unmarshal([]byte(thingDatabaseObject.Object), thingObject)
			thingObject.ID = strfmt.UUID(thingDatabaseObject.Uuid)
			thingObject.Kind = getKind(thingObject)
			responseObject.Things[i] = thingObject
		}

		// Add totalResults to response object.
		responseObject.TotalResults = int64(totalResults)
		responseObject.Kind = getKind(responseObject)

		return things.NewWeaviateThingsListOK().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return things.NewWeaviateThingsPatchForbidden()
		}

		// Get and transform object
		UUID := params.ThingID
		dbObject, errGet := databaseConnector.Get(UUID)

		// Return error if UUID is not found.
		if dbObject.Deleted || errGet != nil {
			return things.NewWeaviateThingsPatchNotFound()
		}

		// Get PATCH params in format RFC 6902
		jsonBody, marshalErr := json.Marshal(params.Body)
		patchObject, decodeErr := jsonpatch.DecodePatch([]byte(jsonBody))

		if marshalErr != nil || decodeErr != nil {
			return things.NewWeaviateThingsPatchBadRequest()
		}

		// Apply the patch
		updatedJSON, applyErr := patchObject.Apply([]byte(dbObject.Object))

		if applyErr != nil {
			return things.NewWeaviateThingsPatchUnprocessableEntity()
		}

		// Set patched JSON back in dbObject
		dbObject.Object = string(updatedJSON)

		dbObject.SetCreateTimeMsToNow()
		go databaseConnector.Add(dbObject)

		// Create return Object
		responseObject := &models.ThingGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		return things.NewWeaviateThingsPatchOK().WithPayload(responseObject)
	})
	api.ThingsWeaviateThingsUpdateHandler = things.WeaviateThingsUpdateHandlerFunc(func(params things.WeaviateThingsUpdateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if connector_utils.WriteAllowed(principal) == false {
			return things.NewWeaviateThingsUpdateForbidden()
		}

		// Get item from database
		UUID := params.ThingID
		dbObject, errGet := databaseConnector.Get(UUID)

		// If there are no results, there is an error
		if dbObject.Deleted || errGet != nil {
			// Object not found response.
			return things.NewWeaviateThingsUpdateNotFound()
		}

		// Set the body-id and generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)
		dbObject.SetCreateTimeMsToNow()

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create object to return
		responseObject := &models.ThingGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &responseObject)
		responseObject.ID = strfmt.UUID(dbObject.Uuid)
		responseObject.Kind = getKind(responseObject)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return things.NewWeaviateThingsUpdateOK().WithPayload(responseObject)
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
