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
package restapi

import (
	"crypto/tls"
	"encoding/json"
	"io"
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
	"github.com/weaviate/weaviate/connectors/datastore"
	"github.com/weaviate/weaviate/connectors/mysql"
	"github.com/weaviate/weaviate/models"

	"github.com/weaviate/weaviate/restapi/operations"
	"github.com/weaviate/weaviate/restapi/operations/commands"
	"github.com/weaviate/weaviate/restapi/operations/events"
	"github.com/weaviate/weaviate/restapi/operations/groups"
	"github.com/weaviate/weaviate/restapi/operations/keys"
	"github.com/weaviate/weaviate/restapi/operations/locations"
	"github.com/weaviate/weaviate/restapi/operations/thing_templates"
	"github.com/weaviate/weaviate/restapi/operations/things"
	"reflect"
	"strings"
	"unicode"
)

const refTypeLocation string = "#/paths/locations"
const refTypeThingTemplate string = "#/paths/thingTemplates"
const maxResultsOverride int64 = 100

func init() {
	var discard io.Writer = ioutil.Discard
	var myGRPCLogger grpclog.Logger = log.New(discard, "", log.LstdFlags)
	grpclog.SetLogger(myGRPCLogger)
}

func getLimit(paramMaxResults *int64) int {
	// Get the max results from params, if exists
	maxResults := maxResultsOverride
	if paramMaxResults != nil {
		maxResults = *paramMaxResults
	}

	// Max results form URL, otherwise max = maxResultsOverride.
	return int(math.Min(float64(maxResults), float64(maxResultsOverride)))
}

func getKind(object interface{}) *string {
	kinds := strings.Split(reflect.TypeOf(object).String(), ".")
	kind := kinds[len(kinds)-1]
	for i, v := range kind {
		kind = string(unicode.ToLower(v)) + kind[i+1:]
		break
	}
	kind = "#weaviate/" + kind

	return &kind
}

func configureFlags(api *operations.WeaviateAPI) {
	// api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{ ... }
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {

	// configure database connection
	var databaseConnector dbconnector.DatabaseConnector

	commandLineInput := "datastore"

	if commandLineInput == "datastore" {
		databaseConnector = &datastore.Datastore{}
	} else {
		databaseConnector = &mysql.Mysql{}
	}

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

	api.CommandsWeaviateCommandsDeleteHandler = commands.WeaviateCommandsDeleteHandlerFunc(func(params commands.WeaviateCommandsDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsDelete has not yet been implemented")
	})
	api.CommandsWeaviateCommandsGetHandler = commands.WeaviateCommandsGetHandlerFunc(func(params commands.WeaviateCommandsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsGet has not yet been implemented")
	})
	api.CommandsWeaviateCommandsInsertHandler = commands.WeaviateCommandsInsertHandlerFunc(func(params commands.WeaviateCommandsInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsInsert has not yet been implemented")
	})
	api.CommandsWeaviateCommandsListHandler = commands.WeaviateCommandsListHandlerFunc(func(params commands.WeaviateCommandsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsList has not yet been implemented")
	})
	api.CommandsWeaviateCommandsPatchHandler = commands.WeaviateCommandsPatchHandlerFunc(func(params commands.WeaviateCommandsPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsPatch has not yet been implemented")
	})
	api.CommandsWeaviateCommandsUpdateHandler = commands.WeaviateCommandsUpdateHandlerFunc(func(params commands.WeaviateCommandsUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsUpdate has not yet been implemented")
	})
	api.CommandsWeaviateCommandsValidateHandler = commands.WeaviateCommandsValidateHandlerFunc(func(params commands.WeaviateCommandsValidateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsValidate has not yet been implemented")
	})
	api.GroupsWeaviateGroupsDeleteHandler = groups.WeaviateGroupsDeleteHandlerFunc(func(params groups.WeaviateGroupsDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation groups.WeaviateGroupsDelete has not yet been implemented")
	})
	api.EventsWeaviateGroupsEventsCreateHandler = events.WeaviateGroupsEventsCreateHandlerFunc(func(params events.WeaviateGroupsEventsCreateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateGroupsEventsCreate has not yet been implemented")
	})
	api.EventsWeaviateGroupsEventsGetHandler = events.WeaviateGroupsEventsGetHandlerFunc(func(params events.WeaviateGroupsEventsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateGroupsEventsGet has not yet been implemented")
	})
	api.EventsWeaviateGroupsEventsListHandler = events.WeaviateGroupsEventsListHandlerFunc(func(params events.WeaviateGroupsEventsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateGroupsEventsList has not yet been implemented")
	})
	api.GroupsWeaviateGroupsGetHandler = groups.WeaviateGroupsGetHandlerFunc(func(params groups.WeaviateGroupsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation groups.WeaviateGroupsGet has not yet been implemented")
	})
	api.GroupsWeaviateGroupsInsertHandler = groups.WeaviateGroupsInsertHandlerFunc(func(params groups.WeaviateGroupsInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation groups.WeaviateGroupsInsert has not yet been implemented")
	})
	api.GroupsWeaviateGroupsListHandler = groups.WeaviateGroupsListHandlerFunc(func(params groups.WeaviateGroupsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation groups.WeaviateGroupsList has not yet been implemented")
	})
	api.GroupsWeaviateGroupsPatchHandler = groups.WeaviateGroupsPatchHandlerFunc(func(params groups.WeaviateGroupsPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation groups.WeaviateGroupsPatch has not yet been implemented")
	})
	api.GroupsWeaviateGroupsUpdateHandler = groups.WeaviateGroupsUpdateHandlerFunc(func(params groups.WeaviateGroupsUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation groups.WeaviateGroupsUpdate has not yet been implemented")
	})
	api.KeysWeaviateKeyCreateHandler = keys.WeaviateKeyCreateHandlerFunc(func(params keys.WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation keys.WeaviateKeyCreate has not yet been implemented")
	})
	api.KeysWeaviateKeysChildrenGetHandler = keys.WeaviateKeysChildrenGetHandlerFunc(func(params keys.WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation keys.WeaviateKeysChildrenGet has not yet been implemented")
	})
	api.KeysWeaviateKeysDeleteHandler = keys.WeaviateKeysDeleteHandlerFunc(func(params keys.WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation keys.WeaviateKeysDelete has not yet been implemented")
	})
	api.KeysWeaviateKeysGetHandler = keys.WeaviateKeysGetHandlerFunc(func(params keys.WeaviateKeysGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation keys.WeaviateKeysGet has not yet been implemented")
	})

	/*
	 * HANDLE LOCATIONS
	 */
	api.LocationsWeaviateLocationsDeleteHandler = locations.WeaviateLocationsDeleteHandlerFunc(func(params locations.WeaviateLocationsDeleteParams, principal interface{}) middleware.Responder {

		// This is a delete function, validate if allowed to read?
		if dbconnector.DeleteAllowed(principal) == false {
			return locations.NewWeaviateLocationsDeleteForbidden()
		}

		// Get item from database
		databaseObject, errGet := databaseConnector.Get(params.LocationID)

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
		if dbconnector.ReadAllowed(principal) == false {
			return locations.NewWeaviateLocationsGetForbidden()
		}

		// Get item from database
		dbObject, err := databaseConnector.Get(params.LocationID)

		// Object is deleted eleted
		if dbObject.Deleted || err != nil {
			return locations.NewWeaviateLocationsGetNotFound()
		}

		// Create object to return
		object := &models.LocationGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &object)
		object.ID = strfmt.UUID(dbObject.Uuid)

		// Get is successful
		return locations.NewWeaviateLocationsGetOK().WithPayload(object)
	})
	api.LocationsWeaviateLocationsInsertHandler = locations.WeaviateLocationsInsertHandlerFunc(func(params locations.WeaviateLocationsInsertParams, principal interface{}) middleware.Responder {

		// This is a write function, validate if allowed to read?
		if dbconnector.WriteAllowed(principal) == false {
			return locations.NewWeaviateLocationsInsertForbidden()
		}

		// Create basic DataBase object
		dbObject := *dbconnector.NewDatabaseObjectFromPrincipal(principal, refTypeLocation)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		locationResponseObject := &models.LocationGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), locationResponseObject)
		locationResponseObject.ID = strfmt.UUID(dbObject.Uuid)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return locations.NewWeaviateLocationsInsertAccepted().WithPayload(locationResponseObject)
	})
	api.LocationsWeaviateLocationsListHandler = locations.WeaviateLocationsListHandlerFunc(func(params locations.WeaviateLocationsListParams, principal interface{}) middleware.Responder {

		// This is a read function, validate if allowed to read?
		if dbconnector.ReadAllowed(principal) == false {
			return locations.NewWeaviateLocationsListForbidden()
		}

		// Get limit
		//limit := getLimit(params.maxResults)
		limit := int(maxResultsOverride)

		// List all results
		locationDatabaseObjects, _, _ := databaseConnector.List(refTypeLocation, limit)

		// Convert to an response object
		locationsListResponse := &models.LocationsListResponse{}
		locationsListResponse.Locations = make([]*models.LocationGetResponse, len(locationDatabaseObjects))

		// Loop to fill response project
		for i, locationDatabaseObject := range locationDatabaseObjects {
			locationObject := &models.LocationGetResponse{}
			json.Unmarshal([]byte(locationDatabaseObject.Object), locationObject)
			locationObject.ID = strfmt.UUID(locationDatabaseObject.Uuid)
			locationsListResponse.Locations[i] = locationObject
		}

		// Add totalResults to response object.
		//locationsListResponse.TotalResults = int32(totalResults)
		//locationsListResponse.Kind = getKind(locationsListResponse)

		return locations.NewWeaviateLocationsListOK().WithPayload(locationsListResponse)
	})
	api.LocationsWeaviateLocationsPatchHandler = locations.WeaviateLocationsPatchHandlerFunc(func(params locations.WeaviateLocationsPatchParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if dbconnector.WriteAllowed(principal) == false {
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
		returnObject := &models.LocationGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &returnObject)
		returnObject.ID = strfmt.UUID(dbObject.Uuid)

		return locations.NewWeaviateLocationsPatchOK().WithPayload(returnObject)
	})
	api.LocationsWeaviateLocationsUpdateHandler = locations.WeaviateLocationsUpdateHandlerFunc(func(params locations.WeaviateLocationsUpdateParams, principal interface{}) middleware.Responder {

		// This is a write function, validate if allowed to read?
		if dbconnector.WriteAllowed(principal) == false {
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
		object := &models.LocationGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), &object)
		object.ID = strfmt.UUID(dbObject.Uuid)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return locations.NewWeaviateLocationsUpdateOK().WithPayload(object)
	})

	api.ThingTemplatesWeaviateThingTemplatesCreateHandler = thing_templates.WeaviateThingTemplatesCreateHandlerFunc(func(params thing_templates.WeaviateThingTemplatesCreateParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if dbconnector.WriteAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesCreateForbidden()
		}

		// Create basic DataBase object
		dbObject := *dbconnector.NewDatabaseObjectFromPrincipal(principal, refTypeThingTemplate)

		// Set the generate JSON to save to the database
		dbObject.MergeRequestBodyIntoObject(params.Body)

		// Save to DB, this needs to be a Go routine because we will return an accepted
		go databaseConnector.Add(dbObject)

		// Create response Object from create object.
		thingTemplateResponseObject := &models.ThingTemplateGetResponse{}
		json.Unmarshal([]byte(dbObject.Object), thingTemplateResponseObject)
		thingTemplateResponseObject.ID = strfmt.UUID(dbObject.Uuid)

		// Return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
		return thing_templates.NewWeaviateThingTemplatesCreateAccepted().WithPayload(thingTemplateResponseObject)
	})
	api.ThingTemplatesWeaviateThingTemplatesDeleteHandler = thing_templates.WeaviateThingTemplatesDeleteHandlerFunc(func(params thing_templates.WeaviateThingTemplatesDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation thing_templates.WeaviateThingTemplatesDelete has not yet been implemented")
	})
	api.ThingTemplatesWeaviateThingTemplatesGetHandler = thing_templates.WeaviateThingTemplatesGetHandlerFunc(func(params thing_templates.WeaviateThingTemplatesGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation thing_templates.WeaviateThingTemplatesGet has not yet been implemented")
	})
	api.ThingTemplatesWeaviateThingTemplatesListHandler = thing_templates.WeaviateThingTemplatesListHandlerFunc(func(params thing_templates.WeaviateThingTemplatesListParams, principal interface{}) middleware.Responder {
		// This is a read function, validate if allowed to read?
		if dbconnector.ReadAllowed(principal) == false {
			return thing_templates.NewWeaviateThingTemplatesListForbidden()
		}

		// Get limit
		//limit := getLimit(params.maxResults)
		limit := int(maxResultsOverride)

		// List all results
		thingTemplatesDatabaseObjects, totalResults, _ := databaseConnector.List(refTypeThingTemplate, limit)

		// Convert to an response object
		thingTemplatesListResponse := &models.ThingTemplatesListResponse{}
		thingTemplatesListResponse.ThingTemplates = make([]*models.ThingTemplateGetResponse, len(thingTemplatesDatabaseObjects))

		// Loop to fill response project
		for i, thingTemplatesDatabaseObject := range thingTemplatesDatabaseObjects {
			thingTemplateObject := &models.ThingTemplateGetResponse{}
			json.Unmarshal([]byte(thingTemplatesDatabaseObject.Object), thingTemplateObject)
			thingTemplateObject.ID = strfmt.UUID(thingTemplatesDatabaseObject.Uuid)
			thingTemplatesListResponse.ThingTemplates[i] = thingTemplateObject
		}

		// Add totalResults to response object.
		thingTemplatesListResponse.TotalResults = int32(totalResults)
		thingTemplatesListResponse.Kind = getKind(thingTemplatesListResponse)

		return thing_templates.NewWeaviateThingTemplatesListOK().WithPayload(thingTemplatesListResponse)
	})
	api.ThingTemplatesWeaviateThingTemplatesPatchHandler = thing_templates.WeaviateThingTemplatesPatchHandlerFunc(func(params thing_templates.WeaviateThingTemplatesPatchParams, principal interface{}) middleware.Responder {
		// This is a write function, validate if allowed to read?
		if dbconnector.WriteAllowed(principal) == false {
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
		returnObject := &models.ThingTemplateGetResponse{}
		json.Unmarshal([]byte(updatedJSON), &returnObject)
		returnObject.ID = strfmt.UUID(dbObject.Uuid)

		return thing_templates.NewWeaviateThingTemplatesPatchOK().WithPayload(returnObject)
	})
	api.ThingTemplatesWeaviateThingTemplatesUpdateHandler = thing_templates.WeaviateThingTemplatesUpdateHandlerFunc(func(params thing_templates.WeaviateThingTemplatesUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation thing_templates.WeaviateThingTemplatesUpdate has not yet been implemented")
	})
	api.ThingTemplatesWeaviateThingTemplatesValidateHandler = thing_templates.WeaviateThingTemplatesValidateHandlerFunc(func(params thing_templates.WeaviateThingTemplatesValidateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation thing_templates.WeaviateThingTemplatesValidate has not yet been implemented")
	})
	api.ThingsWeaviateThingsDeleteHandler = things.WeaviateThingsDeleteHandlerFunc(func(params things.WeaviateThingsDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation things.WeaviateThingsDelete has not yet been implemented")
	})
	api.EventsWeaviateThingsEventsCreateHandler = events.WeaviateThingsEventsCreateHandlerFunc(func(params events.WeaviateThingsEventsCreateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateThingsEventsCreate has not yet been implemented")
	})
	api.EventsWeaviateThingsEventsGetHandler = events.WeaviateThingsEventsGetHandlerFunc(func(params events.WeaviateThingsEventsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateThingsEventsGet has not yet been implemented")
	})
	api.EventsWeaviateThingsEventsListHandler = events.WeaviateThingsEventsListHandlerFunc(func(params events.WeaviateThingsEventsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateThingsEventsList has not yet been implemented")
	})
	api.ThingsWeaviateThingsGetHandler = things.WeaviateThingsGetHandlerFunc(func(params things.WeaviateThingsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation things.WeaviateThingsGet has not yet been implemented")
	})
	api.ThingsWeaviateThingsInsertHandler = things.WeaviateThingsInsertHandlerFunc(func(params things.WeaviateThingsInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation things.WeaviateThingsInsert has not yet been implemented")
	})
	api.ThingsWeaviateThingsListHandler = things.WeaviateThingsListHandlerFunc(func(params things.WeaviateThingsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation things.WeaviateThingsList has not yet been implemented")
	})
	api.ThingsWeaviateThingsPatchHandler = things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation things.WeaviateThingsPatch has not yet been implemented")
	})
	api.ThingsWeaviateThingsUpdateHandler = things.WeaviateThingsUpdateHandlerFunc(func(params things.WeaviateThingsUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation things.WeaviateThingsUpdate has not yet been implemented")
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
