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
 package operations




import (
	"fmt"
	"net/http"
	"strings"

	errors "github.com/go-openapi/errors"
	loads "github.com/go-openapi/loads"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/runtime/yamlpc"
	spec "github.com/go-openapi/spec"
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/weaviate/weaviate/restapi/operations/acl_entries"
	"github.com/weaviate/weaviate/restapi/operations/adapters"
	"github.com/weaviate/weaviate/restapi/operations/commands"
	"github.com/weaviate/weaviate/restapi/operations/devices"
	"github.com/weaviate/weaviate/restapi/operations/events"
	"github.com/weaviate/weaviate/restapi/operations/locations"
	"github.com/weaviate/weaviate/restapi/operations/model_manifests"
)

// NewWeaviateAPI creates a new Weaviate instance
func NewWeaviateAPI(spec *loads.Document) *WeaviateAPI {
	return &WeaviateAPI{
		handlers:              make(map[string]map[string]http.Handler),
		formats:               strfmt.Default,
		defaultConsumes:       "application/json",
		defaultProduces:       "application/json",
		ServerShutdown:        func() {},
		spec:                  spec,
		ServeError:            errors.ServeError,
		JSONConsumer:          runtime.JSONConsumer(),
		BinConsumer:           runtime.ByteStreamConsumer(),
		UrlformConsumer:       runtime.DiscardConsumer,
		YamlConsumer:          yamlpc.YAMLConsumer(),
		XMLConsumer:           runtime.XMLConsumer(),
		MultipartformConsumer: runtime.DiscardConsumer,
		TxtConsumer:           runtime.TextConsumer(),
		JSONProducer:          runtime.JSONProducer(),
		BinProducer:           runtime.ByteStreamProducer(),
		UrlformProducer:       runtime.DiscardProducer,
		YamlProducer:          yamlpc.YAMLProducer(),
		XMLProducer:           runtime.XMLProducer(),
		MultipartformProducer: runtime.DiscardProducer,
		TxtProducer:           runtime.TextProducer(),
		ACLEntriesWeaviateACLEntriesDeleteHandler: acl_entries.WeaviateACLEntriesDeleteHandlerFunc(func(params acl_entries.WeaviateACLEntriesDeleteParams) middleware.Responder {
			return middleware.NotImplemented("operation ACLEntriesWeaviateACLEntriesDelete has not yet been implemented")
		}),
		ACLEntriesWeaviateACLEntriesGetHandler: acl_entries.WeaviateACLEntriesGetHandlerFunc(func(params acl_entries.WeaviateACLEntriesGetParams) middleware.Responder {
			return middleware.NotImplemented("operation ACLEntriesWeaviateACLEntriesGet has not yet been implemented")
		}),
		ACLEntriesWeaviateACLEntriesInsertHandler: acl_entries.WeaviateACLEntriesInsertHandlerFunc(func(params acl_entries.WeaviateACLEntriesInsertParams) middleware.Responder {
			return middleware.NotImplemented("operation ACLEntriesWeaviateACLEntriesInsert has not yet been implemented")
		}),
		ACLEntriesWeaviateACLEntriesListHandler: acl_entries.WeaviateACLEntriesListHandlerFunc(func(params acl_entries.WeaviateACLEntriesListParams) middleware.Responder {
			return middleware.NotImplemented("operation ACLEntriesWeaviateACLEntriesList has not yet been implemented")
		}),
		ACLEntriesWeaviateACLEntriesPatchHandler: acl_entries.WeaviateACLEntriesPatchHandlerFunc(func(params acl_entries.WeaviateACLEntriesPatchParams) middleware.Responder {
			return middleware.NotImplemented("operation ACLEntriesWeaviateACLEntriesPatch has not yet been implemented")
		}),
		ACLEntriesWeaviateACLEntriesUpdateHandler: acl_entries.WeaviateACLEntriesUpdateHandlerFunc(func(params acl_entries.WeaviateACLEntriesUpdateParams) middleware.Responder {
			return middleware.NotImplemented("operation ACLEntriesWeaviateACLEntriesUpdate has not yet been implemented")
		}),
		AdaptersWeaviateAdaptersDeleteHandler: adapters.WeaviateAdaptersDeleteHandlerFunc(func(params adapters.WeaviateAdaptersDeleteParams) middleware.Responder {
			return middleware.NotImplemented("operation AdaptersWeaviateAdaptersDelete has not yet been implemented")
		}),
		AdaptersWeaviateAdaptersGetHandler: adapters.WeaviateAdaptersGetHandlerFunc(func(params adapters.WeaviateAdaptersGetParams) middleware.Responder {
			return middleware.NotImplemented("operation AdaptersWeaviateAdaptersGet has not yet been implemented")
		}),
		AdaptersWeaviateAdaptersInsertHandler: adapters.WeaviateAdaptersInsertHandlerFunc(func(params adapters.WeaviateAdaptersInsertParams) middleware.Responder {
			return middleware.NotImplemented("operation AdaptersWeaviateAdaptersInsert has not yet been implemented")
		}),
		AdaptersWeaviateAdaptersListHandler: adapters.WeaviateAdaptersListHandlerFunc(func(params adapters.WeaviateAdaptersListParams) middleware.Responder {
			return middleware.NotImplemented("operation AdaptersWeaviateAdaptersList has not yet been implemented")
		}),
		AdaptersWeaviateAdaptersPatchHandler: adapters.WeaviateAdaptersPatchHandlerFunc(func(params adapters.WeaviateAdaptersPatchParams) middleware.Responder {
			return middleware.NotImplemented("operation AdaptersWeaviateAdaptersPatch has not yet been implemented")
		}),
		AdaptersWeaviateAdaptersUpdateHandler: adapters.WeaviateAdaptersUpdateHandlerFunc(func(params adapters.WeaviateAdaptersUpdateParams) middleware.Responder {
			return middleware.NotImplemented("operation AdaptersWeaviateAdaptersUpdate has not yet been implemented")
		}),
		CommandsWeaviateCommandsDeleteHandler: commands.WeaviateCommandsDeleteHandlerFunc(func(params commands.WeaviateCommandsDeleteParams) middleware.Responder {
			return middleware.NotImplemented("operation CommandsWeaviateCommandsDelete has not yet been implemented")
		}),
		CommandsWeaviateCommandsGetHandler: commands.WeaviateCommandsGetHandlerFunc(func(params commands.WeaviateCommandsGetParams) middleware.Responder {
			return middleware.NotImplemented("operation CommandsWeaviateCommandsGet has not yet been implemented")
		}),
		CommandsWeaviateCommandsGetQueueHandler: commands.WeaviateCommandsGetQueueHandlerFunc(func(params commands.WeaviateCommandsGetQueueParams) middleware.Responder {
			return middleware.NotImplemented("operation CommandsWeaviateCommandsGetQueue has not yet been implemented")
		}),
		CommandsWeaviateCommandsInsertHandler: commands.WeaviateCommandsInsertHandlerFunc(func(params commands.WeaviateCommandsInsertParams) middleware.Responder {
			return middleware.NotImplemented("operation CommandsWeaviateCommandsInsert has not yet been implemented")
		}),
		CommandsWeaviateCommandsListHandler: commands.WeaviateCommandsListHandlerFunc(func(params commands.WeaviateCommandsListParams) middleware.Responder {
			return middleware.NotImplemented("operation CommandsWeaviateCommandsList has not yet been implemented")
		}),
		CommandsWeaviateCommandsPatchHandler: commands.WeaviateCommandsPatchHandlerFunc(func(params commands.WeaviateCommandsPatchParams) middleware.Responder {
			return middleware.NotImplemented("operation CommandsWeaviateCommandsPatch has not yet been implemented")
		}),
		CommandsWeaviateCommandsUpdateHandler: commands.WeaviateCommandsUpdateHandlerFunc(func(params commands.WeaviateCommandsUpdateParams) middleware.Responder {
			return middleware.NotImplemented("operation CommandsWeaviateCommandsUpdate has not yet been implemented")
		}),
		DevicesWeaviateDevicesDeleteHandler: devices.WeaviateDevicesDeleteHandlerFunc(func(params devices.WeaviateDevicesDeleteParams) middleware.Responder {
			return middleware.NotImplemented("operation DevicesWeaviateDevicesDelete has not yet been implemented")
		}),
		DevicesWeaviateDevicesGetHandler: devices.WeaviateDevicesGetHandlerFunc(func(params devices.WeaviateDevicesGetParams) middleware.Responder {
			return middleware.NotImplemented("operation DevicesWeaviateDevicesGet has not yet been implemented")
		}),
		DevicesWeaviateDevicesInsertHandler: devices.WeaviateDevicesInsertHandlerFunc(func(params devices.WeaviateDevicesInsertParams) middleware.Responder {
			return middleware.NotImplemented("operation DevicesWeaviateDevicesInsert has not yet been implemented")
		}),
		DevicesWeaviateDevicesListHandler: devices.WeaviateDevicesListHandlerFunc(func(params devices.WeaviateDevicesListParams) middleware.Responder {
			return middleware.NotImplemented("operation DevicesWeaviateDevicesList has not yet been implemented")
		}),
		DevicesWeaviateDevicesPatchHandler: devices.WeaviateDevicesPatchHandlerFunc(func(params devices.WeaviateDevicesPatchParams) middleware.Responder {
			return middleware.NotImplemented("operation DevicesWeaviateDevicesPatch has not yet been implemented")
		}),
		DevicesWeaviateDevicesUpdateHandler: devices.WeaviateDevicesUpdateHandlerFunc(func(params devices.WeaviateDevicesUpdateParams) middleware.Responder {
			return middleware.NotImplemented("operation DevicesWeaviateDevicesUpdate has not yet been implemented")
		}),
		EventsWeaviateEventsGetHandler: events.WeaviateEventsGetHandlerFunc(func(params events.WeaviateEventsGetParams) middleware.Responder {
			return middleware.NotImplemented("operation EventsWeaviateEventsGet has not yet been implemented")
		}),
		EventsWeaviateEventsListHandler: events.WeaviateEventsListHandlerFunc(func(params events.WeaviateEventsListParams) middleware.Responder {
			return middleware.NotImplemented("operation EventsWeaviateEventsList has not yet been implemented")
		}),
		EventsWeaviateEventsRecordDeviceEventsHandler: events.WeaviateEventsRecordDeviceEventsHandlerFunc(func(params events.WeaviateEventsRecordDeviceEventsParams) middleware.Responder {
			return middleware.NotImplemented("operation EventsWeaviateEventsRecordDeviceEvents has not yet been implemented")
		}),
		LocationsWeaviateLocationsDeleteHandler: locations.WeaviateLocationsDeleteHandlerFunc(func(params locations.WeaviateLocationsDeleteParams) middleware.Responder {
			return middleware.NotImplemented("operation LocationsWeaviateLocationsDelete has not yet been implemented")
		}),
		LocationsWeaviateLocationsGetHandler: locations.WeaviateLocationsGetHandlerFunc(func(params locations.WeaviateLocationsGetParams) middleware.Responder {
			return middleware.NotImplemented("operation LocationsWeaviateLocationsGet has not yet been implemented")
		}),
		LocationsWeaviateLocationsInsertHandler: locations.WeaviateLocationsInsertHandlerFunc(func(params locations.WeaviateLocationsInsertParams) middleware.Responder {
			return middleware.NotImplemented("operation LocationsWeaviateLocationsInsert has not yet been implemented")
		}),
		LocationsWeaviateLocationsListHandler: locations.WeaviateLocationsListHandlerFunc(func(params locations.WeaviateLocationsListParams) middleware.Responder {
			return middleware.NotImplemented("operation LocationsWeaviateLocationsList has not yet been implemented")
		}),
		LocationsWeaviateLocationsPatchHandler: locations.WeaviateLocationsPatchHandlerFunc(func(params locations.WeaviateLocationsPatchParams) middleware.Responder {
			return middleware.NotImplemented("operation LocationsWeaviateLocationsPatch has not yet been implemented")
		}),
		LocationsWeaviateLocationsUpdateHandler: locations.WeaviateLocationsUpdateHandlerFunc(func(params locations.WeaviateLocationsUpdateParams) middleware.Responder {
			return middleware.NotImplemented("operation LocationsWeaviateLocationsUpdate has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsCreateHandler: model_manifests.WeaviateModelManifestsCreateHandlerFunc(func(params model_manifests.WeaviateModelManifestsCreateParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsCreate has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsDeleteHandler: model_manifests.WeaviateModelManifestsDeleteHandlerFunc(func(params model_manifests.WeaviateModelManifestsDeleteParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsDelete has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsGetHandler: model_manifests.WeaviateModelManifestsGetHandlerFunc(func(params model_manifests.WeaviateModelManifestsGetParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsGet has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsListHandler: model_manifests.WeaviateModelManifestsListHandlerFunc(func(params model_manifests.WeaviateModelManifestsListParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsList has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsPatchHandler: model_manifests.WeaviateModelManifestsPatchHandlerFunc(func(params model_manifests.WeaviateModelManifestsPatchParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsPatch has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsUpdateHandler: model_manifests.WeaviateModelManifestsUpdateHandlerFunc(func(params model_manifests.WeaviateModelManifestsUpdateParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsUpdate has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsValidateCommandDefsHandler: model_manifests.WeaviateModelManifestsValidateCommandDefsHandlerFunc(func(params model_manifests.WeaviateModelManifestsValidateCommandDefsParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsValidateCommandDefs has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsValidateComponentsHandler: model_manifests.WeaviateModelManifestsValidateComponentsHandlerFunc(func(params model_manifests.WeaviateModelManifestsValidateComponentsParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsValidateComponents has not yet been implemented")
		}),
		ModelManifestsWeaviateModelManifestsValidateDeviceStateHandler: model_manifests.WeaviateModelManifestsValidateDeviceStateHandlerFunc(func(params model_manifests.WeaviateModelManifestsValidateDeviceStateParams) middleware.Responder {
			return middleware.NotImplemented("operation ModelManifestsWeaviateModelManifestsValidateDeviceState has not yet been implemented")
		}),
	}
}

/*WeaviateAPI Lets you register, view and manage cloud ready devices. */
type WeaviateAPI struct {
	spec            *loads.Document
	context         *middleware.Context
	handlers        map[string]map[string]http.Handler
	formats         strfmt.Registry
	defaultConsumes string
	defaultProduces string
	Middleware      func(middleware.Builder) http.Handler
	// JSONConsumer registers a consumer for a "application/json-patch+json" mime type
	JSONConsumer runtime.Consumer
	// BinConsumer registers a consumer for a "application/octet-stream" mime type
	BinConsumer runtime.Consumer
	// UrlformConsumer registers a consumer for a "application/x-www-form-urlencoded" mime type
	UrlformConsumer runtime.Consumer
	// YamlConsumer registers a consumer for a "application/x-yaml" mime type
	YamlConsumer runtime.Consumer
	// XMLConsumer registers a consumer for a "application/xml" mime type
	XMLConsumer runtime.Consumer
	// MultipartformConsumer registers a consumer for a "multipart/form-data" mime type
	MultipartformConsumer runtime.Consumer
	// TxtConsumer registers a consumer for a "text/plain" mime type
	TxtConsumer runtime.Consumer

	// JSONProducer registers a producer for a "application/json" mime type
	JSONProducer runtime.Producer
	// BinProducer registers a producer for a "application/octet-stream" mime type
	BinProducer runtime.Producer
	// UrlformProducer registers a producer for a "application/x-www-form-urlencoded" mime type
	UrlformProducer runtime.Producer
	// YamlProducer registers a producer for a "application/x-yaml" mime type
	YamlProducer runtime.Producer
	// XMLProducer registers a producer for a "application/xml" mime type
	XMLProducer runtime.Producer
	// MultipartformProducer registers a producer for a "multipart/form-data" mime type
	MultipartformProducer runtime.Producer
	// TxtProducer registers a producer for a "text/plain" mime type
	TxtProducer runtime.Producer

	// ACLEntriesWeaviateACLEntriesDeleteHandler sets the operation handler for the weaviate acl entries delete operation
	ACLEntriesWeaviateACLEntriesDeleteHandler acl_entries.WeaviateACLEntriesDeleteHandler
	// ACLEntriesWeaviateACLEntriesGetHandler sets the operation handler for the weaviate acl entries get operation
	ACLEntriesWeaviateACLEntriesGetHandler acl_entries.WeaviateACLEntriesGetHandler
	// ACLEntriesWeaviateACLEntriesInsertHandler sets the operation handler for the weaviate acl entries insert operation
	ACLEntriesWeaviateACLEntriesInsertHandler acl_entries.WeaviateACLEntriesInsertHandler
	// ACLEntriesWeaviateACLEntriesListHandler sets the operation handler for the weaviate acl entries list operation
	ACLEntriesWeaviateACLEntriesListHandler acl_entries.WeaviateACLEntriesListHandler
	// ACLEntriesWeaviateACLEntriesPatchHandler sets the operation handler for the weaviate acl entries patch operation
	ACLEntriesWeaviateACLEntriesPatchHandler acl_entries.WeaviateACLEntriesPatchHandler
	// ACLEntriesWeaviateACLEntriesUpdateHandler sets the operation handler for the weaviate acl entries update operation
	ACLEntriesWeaviateACLEntriesUpdateHandler acl_entries.WeaviateACLEntriesUpdateHandler
	// AdaptersWeaviateAdaptersDeleteHandler sets the operation handler for the weaviate adapters delete operation
	AdaptersWeaviateAdaptersDeleteHandler adapters.WeaviateAdaptersDeleteHandler
	// AdaptersWeaviateAdaptersGetHandler sets the operation handler for the weaviate adapters get operation
	AdaptersWeaviateAdaptersGetHandler adapters.WeaviateAdaptersGetHandler
	// AdaptersWeaviateAdaptersInsertHandler sets the operation handler for the weaviate adapters insert operation
	AdaptersWeaviateAdaptersInsertHandler adapters.WeaviateAdaptersInsertHandler
	// AdaptersWeaviateAdaptersListHandler sets the operation handler for the weaviate adapters list operation
	AdaptersWeaviateAdaptersListHandler adapters.WeaviateAdaptersListHandler
	// AdaptersWeaviateAdaptersPatchHandler sets the operation handler for the weaviate adapters patch operation
	AdaptersWeaviateAdaptersPatchHandler adapters.WeaviateAdaptersPatchHandler
	// AdaptersWeaviateAdaptersUpdateHandler sets the operation handler for the weaviate adapters update operation
	AdaptersWeaviateAdaptersUpdateHandler adapters.WeaviateAdaptersUpdateHandler
	// CommandsWeaviateCommandsDeleteHandler sets the operation handler for the weaviate commands delete operation
	CommandsWeaviateCommandsDeleteHandler commands.WeaviateCommandsDeleteHandler
	// CommandsWeaviateCommandsGetHandler sets the operation handler for the weaviate commands get operation
	CommandsWeaviateCommandsGetHandler commands.WeaviateCommandsGetHandler
	// CommandsWeaviateCommandsGetQueueHandler sets the operation handler for the weaviate commands get queue operation
	CommandsWeaviateCommandsGetQueueHandler commands.WeaviateCommandsGetQueueHandler
	// CommandsWeaviateCommandsInsertHandler sets the operation handler for the weaviate commands insert operation
	CommandsWeaviateCommandsInsertHandler commands.WeaviateCommandsInsertHandler
	// CommandsWeaviateCommandsListHandler sets the operation handler for the weaviate commands list operation
	CommandsWeaviateCommandsListHandler commands.WeaviateCommandsListHandler
	// CommandsWeaviateCommandsPatchHandler sets the operation handler for the weaviate commands patch operation
	CommandsWeaviateCommandsPatchHandler commands.WeaviateCommandsPatchHandler
	// CommandsWeaviateCommandsUpdateHandler sets the operation handler for the weaviate commands update operation
	CommandsWeaviateCommandsUpdateHandler commands.WeaviateCommandsUpdateHandler
	// DevicesWeaviateDevicesDeleteHandler sets the operation handler for the weaviate devices delete operation
	DevicesWeaviateDevicesDeleteHandler devices.WeaviateDevicesDeleteHandler
	// DevicesWeaviateDevicesGetHandler sets the operation handler for the weaviate devices get operation
	DevicesWeaviateDevicesGetHandler devices.WeaviateDevicesGetHandler
	// DevicesWeaviateDevicesInsertHandler sets the operation handler for the weaviate devices insert operation
	DevicesWeaviateDevicesInsertHandler devices.WeaviateDevicesInsertHandler
	// DevicesWeaviateDevicesListHandler sets the operation handler for the weaviate devices list operation
	DevicesWeaviateDevicesListHandler devices.WeaviateDevicesListHandler
	// DevicesWeaviateDevicesPatchHandler sets the operation handler for the weaviate devices patch operation
	DevicesWeaviateDevicesPatchHandler devices.WeaviateDevicesPatchHandler
	// DevicesWeaviateDevicesUpdateHandler sets the operation handler for the weaviate devices update operation
	DevicesWeaviateDevicesUpdateHandler devices.WeaviateDevicesUpdateHandler
	// EventsWeaviateEventsGetHandler sets the operation handler for the weaviate events get operation
	EventsWeaviateEventsGetHandler events.WeaviateEventsGetHandler
	// EventsWeaviateEventsListHandler sets the operation handler for the weaviate events list operation
	EventsWeaviateEventsListHandler events.WeaviateEventsListHandler
	// EventsWeaviateEventsRecordDeviceEventsHandler sets the operation handler for the weaviate events record device events operation
	EventsWeaviateEventsRecordDeviceEventsHandler events.WeaviateEventsRecordDeviceEventsHandler
	// LocationsWeaviateLocationsDeleteHandler sets the operation handler for the weaviate locations delete operation
	LocationsWeaviateLocationsDeleteHandler locations.WeaviateLocationsDeleteHandler
	// LocationsWeaviateLocationsGetHandler sets the operation handler for the weaviate locations get operation
	LocationsWeaviateLocationsGetHandler locations.WeaviateLocationsGetHandler
	// LocationsWeaviateLocationsInsertHandler sets the operation handler for the weaviate locations insert operation
	LocationsWeaviateLocationsInsertHandler locations.WeaviateLocationsInsertHandler
	// LocationsWeaviateLocationsListHandler sets the operation handler for the weaviate locations list operation
	LocationsWeaviateLocationsListHandler locations.WeaviateLocationsListHandler
	// LocationsWeaviateLocationsPatchHandler sets the operation handler for the weaviate locations patch operation
	LocationsWeaviateLocationsPatchHandler locations.WeaviateLocationsPatchHandler
	// LocationsWeaviateLocationsUpdateHandler sets the operation handler for the weaviate locations update operation
	LocationsWeaviateLocationsUpdateHandler locations.WeaviateLocationsUpdateHandler
	// ModelManifestsWeaviateModelManifestsCreateHandler sets the operation handler for the weaviate model manifests create operation
	ModelManifestsWeaviateModelManifestsCreateHandler model_manifests.WeaviateModelManifestsCreateHandler
	// ModelManifestsWeaviateModelManifestsDeleteHandler sets the operation handler for the weaviate model manifests delete operation
	ModelManifestsWeaviateModelManifestsDeleteHandler model_manifests.WeaviateModelManifestsDeleteHandler
	// ModelManifestsWeaviateModelManifestsGetHandler sets the operation handler for the weaviate model manifests get operation
	ModelManifestsWeaviateModelManifestsGetHandler model_manifests.WeaviateModelManifestsGetHandler
	// ModelManifestsWeaviateModelManifestsListHandler sets the operation handler for the weaviate model manifests list operation
	ModelManifestsWeaviateModelManifestsListHandler model_manifests.WeaviateModelManifestsListHandler
	// ModelManifestsWeaviateModelManifestsPatchHandler sets the operation handler for the weaviate model manifests patch operation
	ModelManifestsWeaviateModelManifestsPatchHandler model_manifests.WeaviateModelManifestsPatchHandler
	// ModelManifestsWeaviateModelManifestsUpdateHandler sets the operation handler for the weaviate model manifests update operation
	ModelManifestsWeaviateModelManifestsUpdateHandler model_manifests.WeaviateModelManifestsUpdateHandler
	// ModelManifestsWeaviateModelManifestsValidateCommandDefsHandler sets the operation handler for the weaviate model manifests validate command defs operation
	ModelManifestsWeaviateModelManifestsValidateCommandDefsHandler model_manifests.WeaviateModelManifestsValidateCommandDefsHandler
	// ModelManifestsWeaviateModelManifestsValidateComponentsHandler sets the operation handler for the weaviate model manifests validate components operation
	ModelManifestsWeaviateModelManifestsValidateComponentsHandler model_manifests.WeaviateModelManifestsValidateComponentsHandler
	// ModelManifestsWeaviateModelManifestsValidateDeviceStateHandler sets the operation handler for the weaviate model manifests validate device state operation
	ModelManifestsWeaviateModelManifestsValidateDeviceStateHandler model_manifests.WeaviateModelManifestsValidateDeviceStateHandler

	// ServeError is called when an error is received, there is a default handler
	// but you can set your own with this
	ServeError func(http.ResponseWriter, *http.Request, error)

	// ServerShutdown is called when the HTTP(S) server is shut down and done
	// handling all active connections and does not accept connections any more
	ServerShutdown func()

	// Custom command line argument groups with their descriptions
	CommandLineOptionsGroups []swag.CommandLineOptionsGroup

	// User defined logger function.
	Logger func(string, ...interface{})
}

// SetDefaultProduces sets the default produces media type
func (o *WeaviateAPI) SetDefaultProduces(mediaType string) {
	o.defaultProduces = mediaType
}

// SetDefaultConsumes returns the default consumes media type
func (o *WeaviateAPI) SetDefaultConsumes(mediaType string) {
	o.defaultConsumes = mediaType
}

// SetSpec sets a spec that will be served for the clients.
func (o *WeaviateAPI) SetSpec(spec *loads.Document) {
	o.spec = spec
}

// DefaultProduces returns the default produces media type
func (o *WeaviateAPI) DefaultProduces() string {
	return o.defaultProduces
}

// DefaultConsumes returns the default consumes media type
func (o *WeaviateAPI) DefaultConsumes() string {
	return o.defaultConsumes
}

// Formats returns the registered string formats
func (o *WeaviateAPI) Formats() strfmt.Registry {
	return o.formats
}

// RegisterFormat registers a custom format validator
func (o *WeaviateAPI) RegisterFormat(name string, format strfmt.Format, validator strfmt.Validator) {
	o.formats.Add(name, format, validator)
}

// Validate validates the registrations in the WeaviateAPI
func (o *WeaviateAPI) Validate() error {
	var unregistered []string

	if o.JSONConsumer == nil {
		unregistered = append(unregistered, "JSONConsumer")
	}

	if o.BinConsumer == nil {
		unregistered = append(unregistered, "BinConsumer")
	}

	if o.UrlformConsumer == nil {
		unregistered = append(unregistered, "UrlformConsumer")
	}

	if o.YamlConsumer == nil {
		unregistered = append(unregistered, "YamlConsumer")
	}

	if o.XMLConsumer == nil {
		unregistered = append(unregistered, "XMLConsumer")
	}

	if o.MultipartformConsumer == nil {
		unregistered = append(unregistered, "MultipartformConsumer")
	}

	if o.TxtConsumer == nil {
		unregistered = append(unregistered, "TxtConsumer")
	}

	if o.JSONProducer == nil {
		unregistered = append(unregistered, "JSONProducer")
	}

	if o.BinProducer == nil {
		unregistered = append(unregistered, "BinProducer")
	}

	if o.UrlformProducer == nil {
		unregistered = append(unregistered, "UrlformProducer")
	}

	if o.YamlProducer == nil {
		unregistered = append(unregistered, "YamlProducer")
	}

	if o.XMLProducer == nil {
		unregistered = append(unregistered, "XMLProducer")
	}

	if o.MultipartformProducer == nil {
		unregistered = append(unregistered, "MultipartformProducer")
	}

	if o.TxtProducer == nil {
		unregistered = append(unregistered, "TxtProducer")
	}

	if o.ACLEntriesWeaviateACLEntriesDeleteHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaviateACLEntriesDeleteHandler")
	}

	if o.ACLEntriesWeaviateACLEntriesGetHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaviateACLEntriesGetHandler")
	}

	if o.ACLEntriesWeaviateACLEntriesInsertHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaviateACLEntriesInsertHandler")
	}

	if o.ACLEntriesWeaviateACLEntriesListHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaviateACLEntriesListHandler")
	}

	if o.ACLEntriesWeaviateACLEntriesPatchHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaviateACLEntriesPatchHandler")
	}

	if o.ACLEntriesWeaviateACLEntriesUpdateHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaviateACLEntriesUpdateHandler")
	}

	if o.AdaptersWeaviateAdaptersDeleteHandler == nil {
		unregistered = append(unregistered, "adapters.WeaviateAdaptersDeleteHandler")
	}

	if o.AdaptersWeaviateAdaptersGetHandler == nil {
		unregistered = append(unregistered, "adapters.WeaviateAdaptersGetHandler")
	}

	if o.AdaptersWeaviateAdaptersInsertHandler == nil {
		unregistered = append(unregistered, "adapters.WeaviateAdaptersInsertHandler")
	}

	if o.AdaptersWeaviateAdaptersListHandler == nil {
		unregistered = append(unregistered, "adapters.WeaviateAdaptersListHandler")
	}

	if o.AdaptersWeaviateAdaptersPatchHandler == nil {
		unregistered = append(unregistered, "adapters.WeaviateAdaptersPatchHandler")
	}

	if o.AdaptersWeaviateAdaptersUpdateHandler == nil {
		unregistered = append(unregistered, "adapters.WeaviateAdaptersUpdateHandler")
	}

	if o.CommandsWeaviateCommandsDeleteHandler == nil {
		unregistered = append(unregistered, "commands.WeaviateCommandsDeleteHandler")
	}

	if o.CommandsWeaviateCommandsGetHandler == nil {
		unregistered = append(unregistered, "commands.WeaviateCommandsGetHandler")
	}

	if o.CommandsWeaviateCommandsGetQueueHandler == nil {
		unregistered = append(unregistered, "commands.WeaviateCommandsGetQueueHandler")
	}

	if o.CommandsWeaviateCommandsInsertHandler == nil {
		unregistered = append(unregistered, "commands.WeaviateCommandsInsertHandler")
	}

	if o.CommandsWeaviateCommandsListHandler == nil {
		unregistered = append(unregistered, "commands.WeaviateCommandsListHandler")
	}

	if o.CommandsWeaviateCommandsPatchHandler == nil {
		unregistered = append(unregistered, "commands.WeaviateCommandsPatchHandler")
	}

	if o.CommandsWeaviateCommandsUpdateHandler == nil {
		unregistered = append(unregistered, "commands.WeaviateCommandsUpdateHandler")
	}

	if o.DevicesWeaviateDevicesDeleteHandler == nil {
		unregistered = append(unregistered, "devices.WeaviateDevicesDeleteHandler")
	}

	if o.DevicesWeaviateDevicesGetHandler == nil {
		unregistered = append(unregistered, "devices.WeaviateDevicesGetHandler")
	}

	if o.DevicesWeaviateDevicesInsertHandler == nil {
		unregistered = append(unregistered, "devices.WeaviateDevicesInsertHandler")
	}

	if o.DevicesWeaviateDevicesListHandler == nil {
		unregistered = append(unregistered, "devices.WeaviateDevicesListHandler")
	}

	if o.DevicesWeaviateDevicesPatchHandler == nil {
		unregistered = append(unregistered, "devices.WeaviateDevicesPatchHandler")
	}

	if o.DevicesWeaviateDevicesUpdateHandler == nil {
		unregistered = append(unregistered, "devices.WeaviateDevicesUpdateHandler")
	}

	if o.EventsWeaviateEventsGetHandler == nil {
		unregistered = append(unregistered, "events.WeaviateEventsGetHandler")
	}

	if o.EventsWeaviateEventsListHandler == nil {
		unregistered = append(unregistered, "events.WeaviateEventsListHandler")
	}

	if o.EventsWeaviateEventsRecordDeviceEventsHandler == nil {
		unregistered = append(unregistered, "events.WeaviateEventsRecordDeviceEventsHandler")
	}

	if o.LocationsWeaviateLocationsDeleteHandler == nil {
		unregistered = append(unregistered, "locations.WeaviateLocationsDeleteHandler")
	}

	if o.LocationsWeaviateLocationsGetHandler == nil {
		unregistered = append(unregistered, "locations.WeaviateLocationsGetHandler")
	}

	if o.LocationsWeaviateLocationsInsertHandler == nil {
		unregistered = append(unregistered, "locations.WeaviateLocationsInsertHandler")
	}

	if o.LocationsWeaviateLocationsListHandler == nil {
		unregistered = append(unregistered, "locations.WeaviateLocationsListHandler")
	}

	if o.LocationsWeaviateLocationsPatchHandler == nil {
		unregistered = append(unregistered, "locations.WeaviateLocationsPatchHandler")
	}

	if o.LocationsWeaviateLocationsUpdateHandler == nil {
		unregistered = append(unregistered, "locations.WeaviateLocationsUpdateHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsCreateHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsCreateHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsDeleteHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsDeleteHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsGetHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsGetHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsListHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsListHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsPatchHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsPatchHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsUpdateHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsUpdateHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsValidateCommandDefsHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsValidateCommandDefsHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsValidateComponentsHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsValidateComponentsHandler")
	}

	if o.ModelManifestsWeaviateModelManifestsValidateDeviceStateHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaviateModelManifestsValidateDeviceStateHandler")
	}

	if len(unregistered) > 0 {
		return fmt.Errorf("missing registration: %s", strings.Join(unregistered, ", "))
	}

	return nil
}

// ServeErrorFor gets a error handler for a given operation id
func (o *WeaviateAPI) ServeErrorFor(operationID string) func(http.ResponseWriter, *http.Request, error) {
	return o.ServeError
}

// AuthenticatorsFor gets the authenticators for the specified security schemes
func (o *WeaviateAPI) AuthenticatorsFor(schemes map[string]spec.SecurityScheme) map[string]runtime.Authenticator {

	return nil

}

// ConsumersFor gets the consumers for the specified media types
func (o *WeaviateAPI) ConsumersFor(mediaTypes []string) map[string]runtime.Consumer {

	result := make(map[string]runtime.Consumer)
	for _, mt := range mediaTypes {
		switch mt {

		case "application/json":
			result["application/json"] = o.JSONConsumer

		case "application/json-patch+json":
			result["application/json-patch+json"] = o.JSONConsumer

		case "application/octet-stream":
			result["application/octet-stream"] = o.BinConsumer

		case "application/x-www-form-urlencoded":
			result["application/x-www-form-urlencoded"] = o.UrlformConsumer

		case "application/x-yaml":
			result["application/x-yaml"] = o.YamlConsumer

		case "application/xml":
			result["application/xml"] = o.XMLConsumer

		case "multipart/form-data":
			result["multipart/form-data"] = o.MultipartformConsumer

		case "text/plain":
			result["text/plain"] = o.TxtConsumer

		}
	}
	return result

}

// ProducersFor gets the producers for the specified media types
func (o *WeaviateAPI) ProducersFor(mediaTypes []string) map[string]runtime.Producer {

	result := make(map[string]runtime.Producer)
	for _, mt := range mediaTypes {
		switch mt {

		case "application/json":
			result["application/json"] = o.JSONProducer

		case "application/octet-stream":
			result["application/octet-stream"] = o.BinProducer

		case "application/x-www-form-urlencoded":
			result["application/x-www-form-urlencoded"] = o.UrlformProducer

		case "application/x-yaml":
			result["application/x-yaml"] = o.YamlProducer

		case "application/xml":
			result["application/xml"] = o.XMLProducer

		case "multipart/form-data":
			result["multipart/form-data"] = o.MultipartformProducer

		case "text/plain":
			result["text/plain"] = o.TxtProducer

		}
	}
	return result

}

// HandlerFor gets a http.Handler for the provided operation method and path
func (o *WeaviateAPI) HandlerFor(method, path string) (http.Handler, bool) {
	if o.handlers == nil {
		return nil, false
	}
	um := strings.ToUpper(method)
	if _, ok := o.handlers[um]; !ok {
		return nil, false
	}
	if path == "/" {
		path = ""
	}
	h, ok := o.handlers[um][path]
	return h, ok
}

// Context returns the middleware context for the weaviate API
func (o *WeaviateAPI) Context() *middleware.Context {
	if o.context == nil {
		o.context = middleware.NewRoutableContext(o.spec, o, nil)
	}

	return o.context
}

func (o *WeaviateAPI) initHandlerCache() {
	o.Context() // don't care about the result, just that the initialization happened

	if o.handlers == nil {
		o.handlers = make(map[string]map[string]http.Handler)
	}

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaviateACLEntriesDelete(o.context, o.ACLEntriesWeaviateACLEntriesDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaviateACLEntriesGet(o.context, o.ACLEntriesWeaviateACLEntriesGetHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/aclEntries"] = acl_entries.NewWeaviateACLEntriesInsert(o.context, o.ACLEntriesWeaviateACLEntriesInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices/{deviceId}/aclEntries"] = acl_entries.NewWeaviateACLEntriesList(o.context, o.ACLEntriesWeaviateACLEntriesListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaviateACLEntriesPatch(o.context, o.ACLEntriesWeaviateACLEntriesPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaviateACLEntriesUpdate(o.context, o.ACLEntriesWeaviateACLEntriesUpdateHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/adapters/{adapterId}"] = adapters.NewWeaviateAdaptersDelete(o.context, o.AdaptersWeaviateAdaptersDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/adapters/{adapterId}"] = adapters.NewWeaviateAdaptersGet(o.context, o.AdaptersWeaviateAdaptersGetHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/adapters"] = adapters.NewWeaviateAdaptersInsert(o.context, o.AdaptersWeaviateAdaptersInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/adapters"] = adapters.NewWeaviateAdaptersList(o.context, o.AdaptersWeaviateAdaptersListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/adapters/{adapterId}"] = adapters.NewWeaviateAdaptersPatch(o.context, o.AdaptersWeaviateAdaptersPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/adapters/{adapterId}"] = adapters.NewWeaviateAdaptersUpdate(o.context, o.AdaptersWeaviateAdaptersUpdateHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/commands/{commandId}"] = commands.NewWeaviateCommandsDelete(o.context, o.CommandsWeaviateCommandsDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/commands/{commandId}"] = commands.NewWeaviateCommandsGet(o.context, o.CommandsWeaviateCommandsGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/commands/queue"] = commands.NewWeaviateCommandsGetQueue(o.context, o.CommandsWeaviateCommandsGetQueueHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/commands"] = commands.NewWeaviateCommandsInsert(o.context, o.CommandsWeaviateCommandsInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/commands"] = commands.NewWeaviateCommandsList(o.context, o.CommandsWeaviateCommandsListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/commands/{commandId}"] = commands.NewWeaviateCommandsPatch(o.context, o.CommandsWeaviateCommandsPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/commands/{commandId}"] = commands.NewWeaviateCommandsUpdate(o.context, o.CommandsWeaviateCommandsUpdateHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/devices/{deviceId}"] = devices.NewWeaviateDevicesDelete(o.context, o.DevicesWeaviateDevicesDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices/{deviceId}"] = devices.NewWeaviateDevicesGet(o.context, o.DevicesWeaviateDevicesGetHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices"] = devices.NewWeaviateDevicesInsert(o.context, o.DevicesWeaviateDevicesInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices"] = devices.NewWeaviateDevicesList(o.context, o.DevicesWeaviateDevicesListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/devices/{deviceId}"] = devices.NewWeaviateDevicesPatch(o.context, o.DevicesWeaviateDevicesPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/devices/{deviceId}"] = devices.NewWeaviateDevicesUpdate(o.context, o.DevicesWeaviateDevicesUpdateHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/events/{eventId}"] = events.NewWeaviateEventsGet(o.context, o.EventsWeaviateEventsGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/events"] = events.NewWeaviateEventsList(o.context, o.EventsWeaviateEventsListHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/events/recordDeviceEvents"] = events.NewWeaviateEventsRecordDeviceEvents(o.context, o.EventsWeaviateEventsRecordDeviceEventsHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/locations/{locationId}"] = locations.NewWeaviateLocationsDelete(o.context, o.LocationsWeaviateLocationsDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/locations/{locationId}"] = locations.NewWeaviateLocationsGet(o.context, o.LocationsWeaviateLocationsGetHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/locations"] = locations.NewWeaviateLocationsInsert(o.context, o.LocationsWeaviateLocationsInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/locations"] = locations.NewWeaviateLocationsList(o.context, o.LocationsWeaviateLocationsListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/locations/{locationId}"] = locations.NewWeaviateLocationsPatch(o.context, o.LocationsWeaviateLocationsPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/locations/{locationId}"] = locations.NewWeaviateLocationsUpdate(o.context, o.LocationsWeaviateLocationsUpdateHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/modelManifests"] = model_manifests.NewWeaviateModelManifestsCreate(o.context, o.ModelManifestsWeaviateModelManifestsCreateHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/modelManifests/{modelManifestId}"] = model_manifests.NewWeaviateModelManifestsDelete(o.context, o.ModelManifestsWeaviateModelManifestsDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/modelManifests/{modelManifestId}"] = model_manifests.NewWeaviateModelManifestsGet(o.context, o.ModelManifestsWeaviateModelManifestsGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/modelManifests"] = model_manifests.NewWeaviateModelManifestsList(o.context, o.ModelManifestsWeaviateModelManifestsListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/modelManifests/{modelManifestId}"] = model_manifests.NewWeaviateModelManifestsPatch(o.context, o.ModelManifestsWeaviateModelManifestsPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/modelManifests/{modelManifestId}"] = model_manifests.NewWeaviateModelManifestsUpdate(o.context, o.ModelManifestsWeaviateModelManifestsUpdateHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/modelManifests/validateCommandDefs"] = model_manifests.NewWeaviateModelManifestsValidateCommandDefs(o.context, o.ModelManifestsWeaviateModelManifestsValidateCommandDefsHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/modelManifests/validateComponents"] = model_manifests.NewWeaviateModelManifestsValidateComponents(o.context, o.ModelManifestsWeaviateModelManifestsValidateComponentsHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/modelManifests/validateDeviceState"] = model_manifests.NewWeaviateModelManifestsValidateDeviceState(o.context, o.ModelManifestsWeaviateModelManifestsValidateDeviceStateHandler)

}

// Serve creates a http handler to serve the API over HTTP
// can be used directly in http.ListenAndServe(":8000", api.Serve(nil))
func (o *WeaviateAPI) Serve(builder middleware.Builder) http.Handler {
	o.Init()

	if o.Middleware != nil {
		return o.Middleware(builder)
	}
	return o.context.APIHandler(builder)
}

// Init allows you to just initialize the handler cache, you can then recompose the middelware as you see fit
func (o *WeaviateAPI) Init() {
	if len(o.handlers) == 0 {
		o.initHandlerCache()
	}
}
