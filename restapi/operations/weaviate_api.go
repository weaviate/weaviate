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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package operations




import (
	"fmt"
	"net/http"
	"strings"

	loads "github.com/go-openapi/loads"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	security "github.com/go-openapi/runtime/security"
	spec "github.com/go-openapi/spec"
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/weaviate/weaviate/restapi/operations/acl_entries"
	"github.com/weaviate/weaviate/restapi/operations/adapters"
	"github.com/weaviate/weaviate/restapi/operations/commands"
	"github.com/weaviate/weaviate/restapi/operations/devices"
	"github.com/weaviate/weaviate/restapi/operations/events"
	"github.com/weaviate/weaviate/restapi/operations/model_manifests"
)

// NewWeaviateAPI creates a new Weaviate instance
func NewWeaviateAPI(spec *loads.Document) *WeaviateAPI {
	return &WeaviateAPI{
		handlers:        make(map[string]map[string]http.Handler),
		formats:         strfmt.Default,
		defaultConsumes: "application/json",
		defaultProduces: "application/json",
		ServerShutdown:  func() {},
		spec:            spec,
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
	// JSONConsumer registers a consumer for a "application/json" mime type
	JSONConsumer runtime.Consumer
	// ProtobufConsumer registers a consumer for a "application/protobuf" mime type
	ProtobufConsumer runtime.Consumer
	// XMLConsumer registers a consumer for a "application/xml" mime type
	XMLConsumer runtime.Consumer

	// JSONProducer registers a producer for a "application/json" mime type
	JSONProducer runtime.Producer
	// ProtobufProducer registers a producer for a "application/protobuf" mime type
	ProtobufProducer runtime.Producer
	// XMLProducer registers a producer for a "application/xml" mime type
	XMLProducer runtime.Producer

	// APIKeyAuth registers a function that takes a token and returns a principal
	// it performs authentication based on an api key X-API-KEY provided in the header
	APIKeyAuth func(string) (interface{}, error)

	// ACLEntriesWeaveACLEntriesDeleteHandler sets the operation handler for the weave acl entries delete operation
	ACLEntriesWeaveACLEntriesDeleteHandler acl_entries.WeaveACLEntriesDeleteHandler
	// ACLEntriesWeaveACLEntriesGetHandler sets the operation handler for the weave acl entries get operation
	ACLEntriesWeaveACLEntriesGetHandler acl_entries.WeaveACLEntriesGetHandler
	// ACLEntriesWeaveACLEntriesInsertHandler sets the operation handler for the weave acl entries insert operation
	ACLEntriesWeaveACLEntriesInsertHandler acl_entries.WeaveACLEntriesInsertHandler
	// ACLEntriesWeaveACLEntriesListHandler sets the operation handler for the weave acl entries list operation
	ACLEntriesWeaveACLEntriesListHandler acl_entries.WeaveACLEntriesListHandler
	// ACLEntriesWeaveACLEntriesPatchHandler sets the operation handler for the weave acl entries patch operation
	ACLEntriesWeaveACLEntriesPatchHandler acl_entries.WeaveACLEntriesPatchHandler
	// ACLEntriesWeaveACLEntriesUpdateHandler sets the operation handler for the weave acl entries update operation
	ACLEntriesWeaveACLEntriesUpdateHandler acl_entries.WeaveACLEntriesUpdateHandler
	// AdaptersWeaveAdaptersAcceptHandler sets the operation handler for the weave adapters accept operation
	AdaptersWeaveAdaptersAcceptHandler adapters.WeaveAdaptersAcceptHandler
	// AdaptersWeaveAdaptersActivateHandler sets the operation handler for the weave adapters activate operation
	AdaptersWeaveAdaptersActivateHandler adapters.WeaveAdaptersActivateHandler
	// AdaptersWeaveAdaptersDeactivateHandler sets the operation handler for the weave adapters deactivate operation
	AdaptersWeaveAdaptersDeactivateHandler adapters.WeaveAdaptersDeactivateHandler
	// AdaptersWeaveAdaptersGetHandler sets the operation handler for the weave adapters get operation
	AdaptersWeaveAdaptersGetHandler adapters.WeaveAdaptersGetHandler
	// AdaptersWeaveAdaptersListHandler sets the operation handler for the weave adapters list operation
	AdaptersWeaveAdaptersListHandler adapters.WeaveAdaptersListHandler
	// CommandsWeaveCommandsCancelHandler sets the operation handler for the weave commands cancel operation
	CommandsWeaveCommandsCancelHandler commands.WeaveCommandsCancelHandler
	// CommandsWeaveCommandsDeleteHandler sets the operation handler for the weave commands delete operation
	CommandsWeaveCommandsDeleteHandler commands.WeaveCommandsDeleteHandler
	// CommandsWeaveCommandsGetHandler sets the operation handler for the weave commands get operation
	CommandsWeaveCommandsGetHandler commands.WeaveCommandsGetHandler
	// CommandsWeaveCommandsGetQueueHandler sets the operation handler for the weave commands get queue operation
	CommandsWeaveCommandsGetQueueHandler commands.WeaveCommandsGetQueueHandler
	// CommandsWeaveCommandsInsertHandler sets the operation handler for the weave commands insert operation
	CommandsWeaveCommandsInsertHandler commands.WeaveCommandsInsertHandler
	// CommandsWeaveCommandsListHandler sets the operation handler for the weave commands list operation
	CommandsWeaveCommandsListHandler commands.WeaveCommandsListHandler
	// CommandsWeaveCommandsPatchHandler sets the operation handler for the weave commands patch operation
	CommandsWeaveCommandsPatchHandler commands.WeaveCommandsPatchHandler
	// CommandsWeaveCommandsUpdateHandler sets the operation handler for the weave commands update operation
	CommandsWeaveCommandsUpdateHandler commands.WeaveCommandsUpdateHandler
	// DevicesWeaveDevicesAddLabelHandler sets the operation handler for the weave devices add label operation
	DevicesWeaveDevicesAddLabelHandler devices.WeaveDevicesAddLabelHandler
	// DevicesWeaveDevicesAddNicknameHandler sets the operation handler for the weave devices add nickname operation
	DevicesWeaveDevicesAddNicknameHandler devices.WeaveDevicesAddNicknameHandler
	// DevicesWeaveDevicesDeleteHandler sets the operation handler for the weave devices delete operation
	DevicesWeaveDevicesDeleteHandler devices.WeaveDevicesDeleteHandler
	// DevicesWeaveDevicesGetHandler sets the operation handler for the weave devices get operation
	DevicesWeaveDevicesGetHandler devices.WeaveDevicesGetHandler
	// DevicesWeaveDevicesInsertHandler sets the operation handler for the weave devices insert operation
	DevicesWeaveDevicesInsertHandler devices.WeaveDevicesInsertHandler
	// DevicesWeaveDevicesListHandler sets the operation handler for the weave devices list operation
	DevicesWeaveDevicesListHandler devices.WeaveDevicesListHandler
	// DevicesWeaveDevicesPatchHandler sets the operation handler for the weave devices patch operation
	DevicesWeaveDevicesPatchHandler devices.WeaveDevicesPatchHandler
	// DevicesWeaveDevicesPatchStateHandler sets the operation handler for the weave devices patch state operation
	DevicesWeaveDevicesPatchStateHandler devices.WeaveDevicesPatchStateHandler
	// DevicesWeaveDevicesRemoveLabelHandler sets the operation handler for the weave devices remove label operation
	DevicesWeaveDevicesRemoveLabelHandler devices.WeaveDevicesRemoveLabelHandler
	// DevicesWeaveDevicesRemoveNicknameHandler sets the operation handler for the weave devices remove nickname operation
	DevicesWeaveDevicesRemoveNicknameHandler devices.WeaveDevicesRemoveNicknameHandler
	// DevicesWeaveDevicesUpdateHandler sets the operation handler for the weave devices update operation
	DevicesWeaveDevicesUpdateHandler devices.WeaveDevicesUpdateHandler
	// DevicesWeaveDevicesUpdateParentHandler sets the operation handler for the weave devices update parent operation
	DevicesWeaveDevicesUpdateParentHandler devices.WeaveDevicesUpdateParentHandler
	// EventsWeaveEventsDeleteAllHandler sets the operation handler for the weave events delete all operation
	EventsWeaveEventsDeleteAllHandler events.WeaveEventsDeleteAllHandler
	// EventsWeaveEventsListHandler sets the operation handler for the weave events list operation
	EventsWeaveEventsListHandler events.WeaveEventsListHandler
	// EventsWeaveEventsRecordDeviceEventsHandler sets the operation handler for the weave events record device events operation
	EventsWeaveEventsRecordDeviceEventsHandler events.WeaveEventsRecordDeviceEventsHandler
	// ModelManifestsWeaveModelManifestsGetHandler sets the operation handler for the weave model manifests get operation
	ModelManifestsWeaveModelManifestsGetHandler model_manifests.WeaveModelManifestsGetHandler
	// ModelManifestsWeaveModelManifestsListHandler sets the operation handler for the weave model manifests list operation
	ModelManifestsWeaveModelManifestsListHandler model_manifests.WeaveModelManifestsListHandler
	// ModelManifestsWeaveModelManifestsValidateCommandDefsHandler sets the operation handler for the weave model manifests validate command defs operation
	ModelManifestsWeaveModelManifestsValidateCommandDefsHandler model_manifests.WeaveModelManifestsValidateCommandDefsHandler
	// ModelManifestsWeaveModelManifestsValidateComponentsHandler sets the operation handler for the weave model manifests validate components operation
	ModelManifestsWeaveModelManifestsValidateComponentsHandler model_manifests.WeaveModelManifestsValidateComponentsHandler
	// ModelManifestsWeaveModelManifestsValidateDeviceStateHandler sets the operation handler for the weave model manifests validate device state operation
	ModelManifestsWeaveModelManifestsValidateDeviceStateHandler model_manifests.WeaveModelManifestsValidateDeviceStateHandler

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

	if o.ProtobufConsumer == nil {
		unregistered = append(unregistered, "ProtobufConsumer")
	}

	if o.XMLConsumer == nil {
		unregistered = append(unregistered, "XMLConsumer")
	}

	if o.JSONProducer == nil {
		unregistered = append(unregistered, "JSONProducer")
	}

	if o.ProtobufProducer == nil {
		unregistered = append(unregistered, "ProtobufProducer")
	}

	if o.XMLProducer == nil {
		unregistered = append(unregistered, "XMLProducer")
	}

	if o.APIKeyAuth == nil {
		unregistered = append(unregistered, "XAPIKEYAuth")
	}

	if o.ACLEntriesWeaveACLEntriesDeleteHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaveACLEntriesDeleteHandler")
	}

	if o.ACLEntriesWeaveACLEntriesGetHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaveACLEntriesGetHandler")
	}

	if o.ACLEntriesWeaveACLEntriesInsertHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaveACLEntriesInsertHandler")
	}

	if o.ACLEntriesWeaveACLEntriesListHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaveACLEntriesListHandler")
	}

	if o.ACLEntriesWeaveACLEntriesPatchHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaveACLEntriesPatchHandler")
	}

	if o.ACLEntriesWeaveACLEntriesUpdateHandler == nil {
		unregistered = append(unregistered, "acl_entries.WeaveACLEntriesUpdateHandler")
	}

	if o.AdaptersWeaveAdaptersAcceptHandler == nil {
		unregistered = append(unregistered, "adapters.WeaveAdaptersAcceptHandler")
	}

	if o.AdaptersWeaveAdaptersActivateHandler == nil {
		unregistered = append(unregistered, "adapters.WeaveAdaptersActivateHandler")
	}

	if o.AdaptersWeaveAdaptersDeactivateHandler == nil {
		unregistered = append(unregistered, "adapters.WeaveAdaptersDeactivateHandler")
	}

	if o.AdaptersWeaveAdaptersGetHandler == nil {
		unregistered = append(unregistered, "adapters.WeaveAdaptersGetHandler")
	}

	if o.AdaptersWeaveAdaptersListHandler == nil {
		unregistered = append(unregistered, "adapters.WeaveAdaptersListHandler")
	}

	if o.CommandsWeaveCommandsCancelHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsCancelHandler")
	}

	if o.CommandsWeaveCommandsDeleteHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsDeleteHandler")
	}

	if o.CommandsWeaveCommandsGetHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsGetHandler")
	}

	if o.CommandsWeaveCommandsGetQueueHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsGetQueueHandler")
	}

	if o.CommandsWeaveCommandsInsertHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsInsertHandler")
	}

	if o.CommandsWeaveCommandsListHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsListHandler")
	}

	if o.CommandsWeaveCommandsPatchHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsPatchHandler")
	}

	if o.CommandsWeaveCommandsUpdateHandler == nil {
		unregistered = append(unregistered, "commands.WeaveCommandsUpdateHandler")
	}

	if o.DevicesWeaveDevicesAddLabelHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesAddLabelHandler")
	}

	if o.DevicesWeaveDevicesAddNicknameHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesAddNicknameHandler")
	}

	if o.DevicesWeaveDevicesDeleteHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesDeleteHandler")
	}

	if o.DevicesWeaveDevicesGetHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesGetHandler")
	}

	if o.DevicesWeaveDevicesInsertHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesInsertHandler")
	}

	if o.DevicesWeaveDevicesListHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesListHandler")
	}

	if o.DevicesWeaveDevicesPatchHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesPatchHandler")
	}

	if o.DevicesWeaveDevicesPatchStateHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesPatchStateHandler")
	}

	if o.DevicesWeaveDevicesRemoveLabelHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesRemoveLabelHandler")
	}

	if o.DevicesWeaveDevicesRemoveNicknameHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesRemoveNicknameHandler")
	}

	if o.DevicesWeaveDevicesUpdateHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesUpdateHandler")
	}

	if o.DevicesWeaveDevicesUpdateParentHandler == nil {
		unregistered = append(unregistered, "devices.WeaveDevicesUpdateParentHandler")
	}

	if o.EventsWeaveEventsDeleteAllHandler == nil {
		unregistered = append(unregistered, "events.WeaveEventsDeleteAllHandler")
	}

	if o.EventsWeaveEventsListHandler == nil {
		unregistered = append(unregistered, "events.WeaveEventsListHandler")
	}

	if o.EventsWeaveEventsRecordDeviceEventsHandler == nil {
		unregistered = append(unregistered, "events.WeaveEventsRecordDeviceEventsHandler")
	}

	if o.ModelManifestsWeaveModelManifestsGetHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaveModelManifestsGetHandler")
	}

	if o.ModelManifestsWeaveModelManifestsListHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaveModelManifestsListHandler")
	}

	if o.ModelManifestsWeaveModelManifestsValidateCommandDefsHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaveModelManifestsValidateCommandDefsHandler")
	}

	if o.ModelManifestsWeaveModelManifestsValidateComponentsHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaveModelManifestsValidateComponentsHandler")
	}

	if o.ModelManifestsWeaveModelManifestsValidateDeviceStateHandler == nil {
		unregistered = append(unregistered, "model_manifests.WeaveModelManifestsValidateDeviceStateHandler")
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

	result := make(map[string]runtime.Authenticator)
	for name, scheme := range schemes {
		switch name {

		case "apiKey":

			result[name] = security.APIKeyAuth(scheme.Name, scheme.In, o.APIKeyAuth)

		}
	}
	return result

}

// ConsumersFor gets the consumers for the specified media types
func (o *WeaviateAPI) ConsumersFor(mediaTypes []string) map[string]runtime.Consumer {

	result := make(map[string]runtime.Consumer)
	for _, mt := range mediaTypes {
		switch mt {

		case "application/json":
			result["application/json"] = o.JSONConsumer

		case "application/protobuf":
			result["application/protobuf"] = o.ProtobufConsumer

		case "application/xml":
			result["application/xml"] = o.XMLConsumer

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

		case "application/protobuf":
			result["application/protobuf"] = o.ProtobufProducer

		case "application/xml":
			result["application/xml"] = o.XMLProducer

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
		o.handlers[strings.ToUpper("DELETE")] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaveACLEntriesDelete(o.context, o.ACLEntriesWeaveACLEntriesDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaveACLEntriesGet(o.context, o.ACLEntriesWeaveACLEntriesGetHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/aclEntries"] = acl_entries.NewWeaveACLEntriesInsert(o.context, o.ACLEntriesWeaveACLEntriesInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices/{deviceId}/aclEntries"] = acl_entries.NewWeaveACLEntriesList(o.context, o.ACLEntriesWeaveACLEntriesListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers[strings.ToUpper("PATCH")] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaveACLEntriesPatch(o.context, o.ACLEntriesWeaveACLEntriesPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers[strings.ToUpper("PUT")] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/devices/{deviceId}/aclEntries/{aclEntryId}"] = acl_entries.NewWeaveACLEntriesUpdate(o.context, o.ACLEntriesWeaveACLEntriesUpdateHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/adapters/accept"] = adapters.NewWeaveAdaptersAccept(o.context, o.AdaptersWeaveAdaptersAcceptHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/adapters/{adapterId}/activate"] = adapters.NewWeaveAdaptersActivate(o.context, o.AdaptersWeaveAdaptersActivateHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/adapters/{adapterId}/deactivate"] = adapters.NewWeaveAdaptersDeactivate(o.context, o.AdaptersWeaveAdaptersDeactivateHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/adapters/{adapterId}"] = adapters.NewWeaveAdaptersGet(o.context, o.AdaptersWeaveAdaptersGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/adapters"] = adapters.NewWeaveAdaptersList(o.context, o.AdaptersWeaveAdaptersListHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/commands/{commandId}/cancel"] = commands.NewWeaveCommandsCancel(o.context, o.CommandsWeaveCommandsCancelHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers[strings.ToUpper("DELETE")] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/commands/{commandId}"] = commands.NewWeaveCommandsDelete(o.context, o.CommandsWeaveCommandsDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/commands/{commandId}"] = commands.NewWeaveCommandsGet(o.context, o.CommandsWeaveCommandsGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/commands/queue"] = commands.NewWeaveCommandsGetQueue(o.context, o.CommandsWeaveCommandsGetQueueHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/commands"] = commands.NewWeaveCommandsInsert(o.context, o.CommandsWeaveCommandsInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/commands"] = commands.NewWeaveCommandsList(o.context, o.CommandsWeaveCommandsListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers[strings.ToUpper("PATCH")] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/commands/{commandId}"] = commands.NewWeaveCommandsPatch(o.context, o.CommandsWeaveCommandsPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers[strings.ToUpper("PUT")] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/commands/{commandId}"] = commands.NewWeaveCommandsUpdate(o.context, o.CommandsWeaveCommandsUpdateHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/addLabel"] = devices.NewWeaveDevicesAddLabel(o.context, o.DevicesWeaveDevicesAddLabelHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/addNickname"] = devices.NewWeaveDevicesAddNickname(o.context, o.DevicesWeaveDevicesAddNicknameHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers[strings.ToUpper("DELETE")] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/devices/{deviceId}"] = devices.NewWeaveDevicesDelete(o.context, o.DevicesWeaveDevicesDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices/{deviceId}"] = devices.NewWeaveDevicesGet(o.context, o.DevicesWeaveDevicesGetHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices"] = devices.NewWeaveDevicesInsert(o.context, o.DevicesWeaveDevicesInsertHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/devices"] = devices.NewWeaveDevicesList(o.context, o.DevicesWeaveDevicesListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers[strings.ToUpper("PATCH")] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/devices/{deviceId}"] = devices.NewWeaveDevicesPatch(o.context, o.DevicesWeaveDevicesPatchHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/patchState"] = devices.NewWeaveDevicesPatchState(o.context, o.DevicesWeaveDevicesPatchStateHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/removeLabel"] = devices.NewWeaveDevicesRemoveLabel(o.context, o.DevicesWeaveDevicesRemoveLabelHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/removeNickname"] = devices.NewWeaveDevicesRemoveNickname(o.context, o.DevicesWeaveDevicesRemoveNicknameHandler)

	if o.handlers["PUT"] == nil {
		o.handlers[strings.ToUpper("PUT")] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/devices/{deviceId}"] = devices.NewWeaveDevicesUpdate(o.context, o.DevicesWeaveDevicesUpdateHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/devices/{deviceId}/updateParent"] = devices.NewWeaveDevicesUpdateParent(o.context, o.DevicesWeaveDevicesUpdateParentHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/events/deleteAll"] = events.NewWeaveEventsDeleteAll(o.context, o.EventsWeaveEventsDeleteAllHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/events"] = events.NewWeaveEventsList(o.context, o.EventsWeaveEventsListHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/events/recordDeviceEvents"] = events.NewWeaveEventsRecordDeviceEvents(o.context, o.EventsWeaveEventsRecordDeviceEventsHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/modelManifests/{modelManifestId}"] = model_manifests.NewWeaveModelManifestsGet(o.context, o.ModelManifestsWeaveModelManifestsGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers[strings.ToUpper("GET")] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/modelManifests"] = model_manifests.NewWeaveModelManifestsList(o.context, o.ModelManifestsWeaveModelManifestsListHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/modelManifests/validateCommandDefs"] = model_manifests.NewWeaveModelManifestsValidateCommandDefs(o.context, o.ModelManifestsWeaveModelManifestsValidateCommandDefsHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/modelManifests/validateComponents"] = model_manifests.NewWeaveModelManifestsValidateComponents(o.context, o.ModelManifestsWeaveModelManifestsValidateComponentsHandler)

	if o.handlers["POST"] == nil {
		o.handlers[strings.ToUpper("POST")] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/modelManifests/validateDeviceState"] = model_manifests.NewWeaveModelManifestsValidateDeviceState(o.context, o.ModelManifestsWeaveModelManifestsValidateDeviceStateHandler)

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
