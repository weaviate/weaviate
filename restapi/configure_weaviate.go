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
 package restapi

import (
	"crypto/tls"
	"io"
	"net/http"

	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/restapi/operations"
	"github.com/weaviate/weaviate/restapi/operations/acl_entries"
	"github.com/weaviate/weaviate/restapi/operations/adapters"
	"github.com/weaviate/weaviate/restapi/operations/commands"
	"github.com/weaviate/weaviate/restapi/operations/devices"
	"github.com/weaviate/weaviate/restapi/operations/events"
	"github.com/weaviate/weaviate/restapi/operations/model_manifests"
)

// This file is safe to edit. Once it exists it will not be overwritten

//go:generate swagger generate server --target .. --name weaviate --spec ../swagger.json --default-scheme https

func configureFlags(api *operations.WeaviateAPI) {
	// api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{ ... }
}

func configureAPI(api *operations.WeaviateAPI) http.Handler {
	// configure the api here
	api.ServeError = errors.ServeError

	// Set your custom logger if needed. Default one is log.Printf
	// Expected interface func(string, ...interface{})
	//
	// Example:
	// s.api.Logger = log.Printf

	api.JSONConsumer = runtime.JSONConsumer()

	api.ProtobufConsumer = runtime.ConsumerFunc(func(r io.Reader, target interface{}) error {
		return errors.NotImplemented("protobuf consumer has not yet been implemented")
	})
	api.XMLConsumer = runtime.XMLConsumer()

	api.JSONProducer = runtime.JSONProducer()

	api.ProtobufProducer = runtime.ProducerFunc(func(w io.Writer, data interface{}) error {
		return errors.NotImplemented("protobuf producer has not yet been implemented")
	})
	api.XMLProducer = runtime.XMLProducer()

	// Applies when the "X-API-KEY" header is set
	api.APIKeyAuth = func(token string) (interface{}, error) {
		return nil, errors.NotImplemented("api key auth (apiKey) X-API-KEY from header param [X-API-KEY] has not yet been implemented")
	}

	api.ACLEntriesWeaveACLEntriesDeleteHandler = acl_entries.WeaveACLEntriesDeleteHandlerFunc(func(params acl_entries.WeaveACLEntriesDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaveACLEntriesDelete has not yet been implemented")
	})
	api.ACLEntriesWeaveACLEntriesGetHandler = acl_entries.WeaveACLEntriesGetHandlerFunc(func(params acl_entries.WeaveACLEntriesGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaveACLEntriesGet has not yet been implemented")
	})
	api.ACLEntriesWeaveACLEntriesInsertHandler = acl_entries.WeaveACLEntriesInsertHandlerFunc(func(params acl_entries.WeaveACLEntriesInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaveACLEntriesInsert has not yet been implemented")
	})
	api.ACLEntriesWeaveACLEntriesListHandler = acl_entries.WeaveACLEntriesListHandlerFunc(func(params acl_entries.WeaveACLEntriesListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaveACLEntriesList has not yet been implemented")
	})
	api.ACLEntriesWeaveACLEntriesPatchHandler = acl_entries.WeaveACLEntriesPatchHandlerFunc(func(params acl_entries.WeaveACLEntriesPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaveACLEntriesPatch has not yet been implemented")
	})
	api.ACLEntriesWeaveACLEntriesUpdateHandler = acl_entries.WeaveACLEntriesUpdateHandlerFunc(func(params acl_entries.WeaveACLEntriesUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation acl_entries.WeaveACLEntriesUpdate has not yet been implemented")
	})
	api.AdaptersWeaveAdaptersAcceptHandler = adapters.WeaveAdaptersAcceptHandlerFunc(func(params adapters.WeaveAdaptersAcceptParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaveAdaptersAccept has not yet been implemented")
	})
	api.AdaptersWeaveAdaptersActivateHandler = adapters.WeaveAdaptersActivateHandlerFunc(func(params adapters.WeaveAdaptersActivateParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaveAdaptersActivate has not yet been implemented")
	})
	api.AdaptersWeaveAdaptersDeactivateHandler = adapters.WeaveAdaptersDeactivateHandlerFunc(func(params adapters.WeaveAdaptersDeactivateParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaveAdaptersDeactivate has not yet been implemented")
	})
	api.AdaptersWeaveAdaptersGetHandler = adapters.WeaveAdaptersGetHandlerFunc(func(params adapters.WeaveAdaptersGetParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaveAdaptersGet has not yet been implemented")
	})
	api.AdaptersWeaveAdaptersListHandler = adapters.WeaveAdaptersListHandlerFunc(func(params adapters.WeaveAdaptersListParams) middleware.Responder {
		return middleware.NotImplemented("operation adapters.WeaveAdaptersList has not yet been implemented")
	})
	api.CommandsWeaveCommandsCancelHandler = commands.WeaveCommandsCancelHandlerFunc(func(params commands.WeaveCommandsCancelParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsCancel has not yet been implemented")
	})
	api.CommandsWeaveCommandsDeleteHandler = commands.WeaveCommandsDeleteHandlerFunc(func(params commands.WeaveCommandsDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsDelete has not yet been implemented")
	})
	api.CommandsWeaveCommandsGetHandler = commands.WeaveCommandsGetHandlerFunc(func(params commands.WeaveCommandsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsGet has not yet been implemented")
	})
	api.CommandsWeaveCommandsGetQueueHandler = commands.WeaveCommandsGetQueueHandlerFunc(func(params commands.WeaveCommandsGetQueueParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsGetQueue has not yet been implemented")
	})
	api.CommandsWeaveCommandsInsertHandler = commands.WeaveCommandsInsertHandlerFunc(func(params commands.WeaveCommandsInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsInsert has not yet been implemented")
	})
	api.CommandsWeaveCommandsListHandler = commands.WeaveCommandsListHandlerFunc(func(params commands.WeaveCommandsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsList has not yet been implemented")
	})
	api.CommandsWeaveCommandsPatchHandler = commands.WeaveCommandsPatchHandlerFunc(func(params commands.WeaveCommandsPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsPatch has not yet been implemented")
	})
	api.CommandsWeaveCommandsUpdateHandler = commands.WeaveCommandsUpdateHandlerFunc(func(params commands.WeaveCommandsUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsUpdate has not yet been implemented")
	})
	api.DevicesWeaveDevicesAddLabelHandler = devices.WeaveDevicesAddLabelHandlerFunc(func(params devices.WeaveDevicesAddLabelParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesAddLabel has not yet been implemented")
	})
	api.DevicesWeaveDevicesAddNicknameHandler = devices.WeaveDevicesAddNicknameHandlerFunc(func(params devices.WeaveDevicesAddNicknameParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesAddNickname has not yet been implemented")
	})
	api.DevicesWeaveDevicesDeleteHandler = devices.WeaveDevicesDeleteHandlerFunc(func(params devices.WeaveDevicesDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesDelete has not yet been implemented")
	})
	api.DevicesWeaveDevicesGetHandler = devices.WeaveDevicesGetHandlerFunc(func(params devices.WeaveDevicesGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesGet has not yet been implemented")
	})
	api.DevicesWeaveDevicesInsertHandler = devices.WeaveDevicesInsertHandlerFunc(func(params devices.WeaveDevicesInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesInsert has not yet been implemented")
	})
	api.DevicesWeaveDevicesListHandler = devices.WeaveDevicesListHandlerFunc(func(params devices.WeaveDevicesListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesList has not yet been implemented")
	})
	api.DevicesWeaveDevicesPatchHandler = devices.WeaveDevicesPatchHandlerFunc(func(params devices.WeaveDevicesPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesPatch has not yet been implemented")
	})
	api.DevicesWeaveDevicesPatchStateHandler = devices.WeaveDevicesPatchStateHandlerFunc(func(params devices.WeaveDevicesPatchStateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesPatchState has not yet been implemented")
	})
	api.DevicesWeaveDevicesRemoveLabelHandler = devices.WeaveDevicesRemoveLabelHandlerFunc(func(params devices.WeaveDevicesRemoveLabelParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesRemoveLabel has not yet been implemented")
	})
	api.DevicesWeaveDevicesRemoveNicknameHandler = devices.WeaveDevicesRemoveNicknameHandlerFunc(func(params devices.WeaveDevicesRemoveNicknameParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesRemoveNickname has not yet been implemented")
	})
	api.DevicesWeaveDevicesUpdateHandler = devices.WeaveDevicesUpdateHandlerFunc(func(params devices.WeaveDevicesUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesUpdate has not yet been implemented")
	})
	api.DevicesWeaveDevicesUpdateParentHandler = devices.WeaveDevicesUpdateParentHandlerFunc(func(params devices.WeaveDevicesUpdateParentParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesUpdateParent has not yet been implemented")
	})
	api.EventsWeaveEventsDeleteAllHandler = events.WeaveEventsDeleteAllHandlerFunc(func(params events.WeaveEventsDeleteAllParams) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaveEventsDeleteAll has not yet been implemented")
	})
	api.EventsWeaveEventsListHandler = events.WeaveEventsListHandlerFunc(func(params events.WeaveEventsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaveEventsList has not yet been implemented")
	})
	api.EventsWeaveEventsRecordDeviceEventsHandler = events.WeaveEventsRecordDeviceEventsHandlerFunc(func(params events.WeaveEventsRecordDeviceEventsParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation events.WeaveEventsRecordDeviceEvents has not yet been implemented")
	})
	api.ModelManifestsWeaveModelManifestsGetHandler = model_manifests.WeaveModelManifestsGetHandlerFunc(func(params model_manifests.WeaveModelManifestsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaveModelManifestsGet has not yet been implemented")
	})
	api.ModelManifestsWeaveModelManifestsListHandler = model_manifests.WeaveModelManifestsListHandlerFunc(func(params model_manifests.WeaveModelManifestsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaveModelManifestsList has not yet been implemented")
	})
	api.ModelManifestsWeaveModelManifestsValidateCommandDefsHandler = model_manifests.WeaveModelManifestsValidateCommandDefsHandlerFunc(func(params model_manifests.WeaveModelManifestsValidateCommandDefsParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaveModelManifestsValidateCommandDefs has not yet been implemented")
	})
	api.ModelManifestsWeaveModelManifestsValidateComponentsHandler = model_manifests.WeaveModelManifestsValidateComponentsHandlerFunc(func(params model_manifests.WeaveModelManifestsValidateComponentsParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaveModelManifestsValidateComponents has not yet been implemented")
	})
	api.ModelManifestsWeaveModelManifestsValidateDeviceStateHandler = model_manifests.WeaveModelManifestsValidateDeviceStateHandlerFunc(func(params model_manifests.WeaveModelManifestsValidateDeviceStateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation model_manifests.WeaveModelManifestsValidateDeviceState has not yet been implemented")
	})

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
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
