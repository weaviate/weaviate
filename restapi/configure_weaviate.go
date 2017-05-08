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
	"fmt"
	"net/http"
	"strings"

	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/runtime/yamlpc"
	gouuid "github.com/satori/go.uuid"
	graceful "github.com/tylerb/graceful"

	"github.com/weaviate/weaviate/connectors"
	"github.com/weaviate/weaviate/connectors/datastore"
	"github.com/weaviate/weaviate/connectors/mysql"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/restapi/operations"
	"github.com/weaviate/weaviate/restapi/operations/acl_entries"
	"github.com/weaviate/weaviate/restapi/operations/adapters"
	"github.com/weaviate/weaviate/restapi/operations/commands"
	"github.com/weaviate/weaviate/restapi/operations/devices"
	"github.com/weaviate/weaviate/restapi/operations/events"
	"github.com/weaviate/weaviate/restapi/operations/locations"
	"github.com/weaviate/weaviate/restapi/operations/model_manifests"
	"time"
)

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

	err := databaseConnector.Connect()
	if err != nil {
		panic(err)
	}

	// configure the api here
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
	 * HANDLE ACL
	 */
	api.ACLEntriesWeaviateACLEntriesDeleteHandler = acl_entries.WeaviateACLEntriesDeleteHandlerFunc(func(params acl_entries.WeaviateACLEntriesDeleteParams) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaviateACLEntriesDelete has not yet been implemented")
	})
	api.ACLEntriesWeaviateACLEntriesGetHandler = acl_entries.WeaviateACLEntriesGetHandlerFunc(func(params acl_entries.WeaviateACLEntriesGetParams) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaviateACLEntriesGet has not yet been implemented")
	})
	api.ACLEntriesWeaviateACLEntriesInsertHandler = acl_entries.WeaviateACLEntriesInsertHandlerFunc(func(params acl_entries.WeaviateACLEntriesInsertParams) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaviateACLEntriesInsert has not yet been implemented")
	})
	api.ACLEntriesWeaviateACLEntriesListHandler = acl_entries.WeaviateACLEntriesListHandlerFunc(func(params acl_entries.WeaviateACLEntriesListParams) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaviateACLEntriesList has not yet been implemented")
	})
	api.ACLEntriesWeaviateACLEntriesPatchHandler = acl_entries.WeaviateACLEntriesPatchHandlerFunc(func(params acl_entries.WeaviateACLEntriesPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaviateACLEntriesPatch has not yet been implemented")
	})
	api.ACLEntriesWeaviateACLEntriesUpdateHandler = acl_entries.WeaviateACLEntriesUpdateHandlerFunc(func(params acl_entries.WeaviateACLEntriesUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaviateACLEntriesUpdate has not yet been implemented")
	})

	/*
	 * HANDLE ADAPTERS
	 */
	api.AdaptersWeaviateAdaptersDeleteHandler = adapters.WeaviateAdaptersDeleteHandlerFunc(func(params adapters.WeaviateAdaptersDeleteParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaviateAdaptersDelete has not yet been implemented")
	})
	api.AdaptersWeaviateAdaptersGetHandler = adapters.WeaviateAdaptersGetHandlerFunc(func(params adapters.WeaviateAdaptersGetParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaviateAdaptersGet has not yet been implemented")
	})
	api.AdaptersWeaviateAdaptersInsertHandler = adapters.WeaviateAdaptersInsertHandlerFunc(func(params adapters.WeaviateAdaptersInsertParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaviateAdaptersInsert has not yet been implemented")
	})
	api.AdaptersWeaviateAdaptersListHandler = adapters.WeaviateAdaptersListHandlerFunc(func(params adapters.WeaviateAdaptersListParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaviateAdaptersList has not yet been implemented")
	})
	api.AdaptersWeaviateAdaptersPatchHandler = adapters.WeaviateAdaptersPatchHandlerFunc(func(params adapters.WeaviateAdaptersPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaviateAdaptersPatch has not yet been implemented")
	})
	api.AdaptersWeaviateAdaptersUpdateHandler = adapters.WeaviateAdaptersUpdateHandlerFunc(func(params adapters.WeaviateAdaptersUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaviateAdaptersUpdate has not yet been implemented")
	})

	/*
	 * HANDLE COMMANDS
	 */
	api.CommandsWeaviateCommandsDeleteHandler = commands.WeaviateCommandsDeleteHandlerFunc(func(params commands.WeaviateCommandsDeleteParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsDelete has not yet been implemented")
	})
	api.CommandsWeaviateCommandsGetHandler = commands.WeaviateCommandsGetHandlerFunc(func(params commands.WeaviateCommandsGetParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsGet has not yet been implemented")
	})
	api.CommandsWeaviateCommandsGetQueueHandler = commands.WeaviateCommandsGetQueueHandlerFunc(func(params commands.WeaviateCommandsGetQueueParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsGetQueue has not yet been implemented")
	})
	api.CommandsWeaviateCommandsInsertHandler = commands.WeaviateCommandsInsertHandlerFunc(func(params commands.WeaviateCommandsInsertParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsInsert has not yet been implemented")
	})
	api.CommandsWeaviateCommandsListHandler = commands.WeaviateCommandsListHandlerFunc(func(params commands.WeaviateCommandsListParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsList has not yet been implemented")
	})
	api.CommandsWeaviateCommandsPatchHandler = commands.WeaviateCommandsPatchHandlerFunc(func(params commands.WeaviateCommandsPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsPatch has not yet been implemented")
	})
	api.CommandsWeaviateCommandsUpdateHandler = commands.WeaviateCommandsUpdateHandlerFunc(func(params commands.WeaviateCommandsUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaviateCommandsUpdate has not yet been implemented")
	})

	/*
	 * HANDLE DEVICES
	 */
	api.DevicesWeaviateDevicesDeleteHandler = devices.WeaviateDevicesDeleteHandlerFunc(func(params devices.WeaviateDevicesDeleteParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaviateDevicesDelete has not yet been implemented")
	})
	api.DevicesWeaviateDevicesGetHandler = devices.WeaviateDevicesGetHandlerFunc(func(params devices.WeaviateDevicesGetParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaviateDevicesGet has not yet been implemented")
	})
	api.DevicesWeaviateDevicesInsertHandler = devices.WeaviateDevicesInsertHandlerFunc(func(params devices.WeaviateDevicesInsertParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaviateDevicesInsert has not yet been implemented")
	})
	api.DevicesWeaviateDevicesListHandler = devices.WeaviateDevicesListHandlerFunc(func(params devices.WeaviateDevicesListParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaviateDevicesList has not yet been implemented")
	})
	api.DevicesWeaviateDevicesPatchHandler = devices.WeaviateDevicesPatchHandlerFunc(func(params devices.WeaviateDevicesPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaviateDevicesPatch has not yet been implemented")
	})
	api.DevicesWeaviateDevicesUpdateHandler = devices.WeaviateDevicesUpdateHandlerFunc(func(params devices.WeaviateDevicesUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaviateDevicesUpdate has not yet been implemented")
	})

	/*
	 * HANDLE EVENTS
	 */
	api.EventsWeaviateEventsGetHandler = events.WeaviateEventsGetHandlerFunc(func(params events.WeaviateEventsGetParams) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateEventsGet has not yet been implemented")
	})
	api.EventsWeaviateEventsListHandler = events.WeaviateEventsListHandlerFunc(func(params events.WeaviateEventsListParams) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateEventsList has not yet been implemented")
	})
	api.EventsWeaviateEventsRecordDeviceEventsHandler = events.WeaviateEventsRecordDeviceEventsHandlerFunc(func(params events.WeaviateEventsRecordDeviceEventsParams) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaviateEventsRecordDeviceEvents has not yet been implemented")
	})

	/*
	 * HANDLE LOCATIONS
	 */
	api.LocationsWeaviateLocationsDeleteHandler = locations.WeaviateLocationsDeleteHandlerFunc(func(params locations.WeaviateLocationsDeleteParams) middleware.Responder {
		// Delete item from database
		err := databaseConnector.Delete(params.LocationID)

		// TODO: Not found response
		// TODO: Deleted response

		if err != nil {
			panic(err)
		}

		return locations.NewWeaviateLocationsDeleteNoContent()
	})
	api.LocationsWeaviateLocationsGetHandler = locations.WeaviateLocationsGetHandlerFunc(func(params locations.WeaviateLocationsGetParams) middleware.Responder {

		// Get item from database
		result, err := databaseConnector.Get(params.LocationID)

		// Create object to return
		object := &models.Location{}
		json.Unmarshal([]byte(result.Object), &object)

		objectID := strings.TrimSpace(object.ID)

		println("'", objectID, "'")

		// If there are no results, the Object ID = 0
		if len(objectID) == 0 && err == nil {
			// return SUCCESS of query but no content.
			return locations.NewWeaviateLocationsGetNotFound()
		}

		// return SUCCESS
		return locations.NewWeaviateLocationsGetOK().WithPayload(object)

	})
	api.LocationsWeaviateLocationsInsertHandler = locations.WeaviateLocationsInsertHandlerFunc(func(params locations.WeaviateLocationsInsertParams) middleware.Responder {

		/*
		 * TODO VALIDATE IF THE OBJECT IS OKAY
		 */
		validated := true

		// Create UUID
		uuid := fmt.Sprintf("%v", gouuid.NewV4())
		params.Body.ID = uuid

		// Set the body to save to the database
		databaseBody, _ := json.Marshal(params.Body)

		// return error
		if validated == false {
			return middleware.ResponderFunc(func(rw http.ResponseWriter, p runtime.Producer) {
				rw.WriteHeader(422)
				rw.Write([]byte("{ \"ERROR\": \"There is something wrong with your original POSTed body\" }"))
			})
		} else {
			// TODO: Make object with default fill createtime and delete
			dbObject := dbconnector.DatabaseObject{
				Uuid:         uuid,
				Owner:        "FOOBAR USER UUID",
				RefType:      "#/paths/locations",
				CreateTimeMs: time.Now().UnixNano() / int64(time.Millisecond),
				Object:       string(databaseBody),
				Deleted:      false,
			}

			// save to DB, this needs to be a Go routine because we will return an accepted
			go databaseConnector.Add(dbObject)

			// return SUCCESS (NOTE: this is ACCEPTED, so the databaseConnector.Add should have a go routine)
			return locations.NewWeaviateLocationsInsertAccepted().WithPayload(params.Body)
		}

	})
	api.LocationsWeaviateLocationsListHandler = locations.WeaviateLocationsListHandlerFunc(func(params locations.WeaviateLocationsListParams) middleware.Responder {
		// Show all locations with List function, get max results in URL, otherwise max = 10.

		limit := 10

		locationDatabaseObjects, err := databaseConnector.List("#/paths/locations", limit)

		// TODO: Limit max here
		// TODO: None found
		if err != nil {
			panic(err)
		}

		locationsListResponse := &models.LocationsListResponse{}
		locationsListResponse.Locations = make([]*models.Location, limit)

		for i, locationDatabaseObject := range locationDatabaseObjects {
			locationObject := &models.Location{}
			json.Unmarshal([]byte(locationDatabaseObject.Object), locationObject)
			locationsListResponse.Locations[i] = locationObject
		}

		return locations.NewWeaviateLocationsListOK().WithPayload(locationsListResponse)
	})
	api.LocationsWeaviateLocationsPatchHandler = locations.WeaviateLocationsPatchHandlerFunc(func(params locations.WeaviateLocationsPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation locations.WeaviateLocationsPatch has not yet been implemented")
	})
	api.LocationsWeaviateLocationsUpdateHandler = locations.WeaviateLocationsUpdateHandlerFunc(func(params locations.WeaviateLocationsUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation locations.WeaviateLocationsUpdate has not yet been implemented")
	})

	/*
	 * HANDLE MODEL MANIFESTS
	 */
	api.ModelManifestsWeaviateModelManifestsCreateHandler = model_manifests.WeaviateModelManifestsCreateHandlerFunc(func(params model_manifests.WeaviateModelManifestsCreateParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsCreate has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsDeleteHandler = model_manifests.WeaviateModelManifestsDeleteHandlerFunc(func(params model_manifests.WeaviateModelManifestsDeleteParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsDelete has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsGetHandler = model_manifests.WeaviateModelManifestsGetHandlerFunc(func(params model_manifests.WeaviateModelManifestsGetParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsGet has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsListHandler = model_manifests.WeaviateModelManifestsListHandlerFunc(func(params model_manifests.WeaviateModelManifestsListParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsList has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsPatchHandler = model_manifests.WeaviateModelManifestsPatchHandlerFunc(func(params model_manifests.WeaviateModelManifestsPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsPatch has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsUpdateHandler = model_manifests.WeaviateModelManifestsUpdateHandlerFunc(func(params model_manifests.WeaviateModelManifestsUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsUpdate has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsValidateCommandDefsHandler = model_manifests.WeaviateModelManifestsValidateCommandDefsHandlerFunc(func(params model_manifests.WeaviateModelManifestsValidateCommandDefsParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsValidateCommandDefs has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsValidateComponentsHandler = model_manifests.WeaviateModelManifestsValidateComponentsHandlerFunc(func(params model_manifests.WeaviateModelManifestsValidateComponentsParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsValidateComponents has not yet been implemented")
	})
	api.ModelManifestsWeaviateModelManifestsValidateDeviceStateHandler = model_manifests.WeaviateModelManifestsValidateDeviceStateHandlerFunc(func(params model_manifests.WeaviateModelManifestsValidateDeviceStateParams) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaviateModelManifestsValidateDeviceState has not yet been implemented")
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
