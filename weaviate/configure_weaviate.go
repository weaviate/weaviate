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
	"net/http"

	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/weaviate/operations"
	"github.com/weaviate/weaviate/weaviate/operations/acl_entries"
	"github.com/weaviate/weaviate/weaviate/operations/adapters"
	"github.com/weaviate/weaviate/weaviate/operations/authorized_apps"
	"github.com/weaviate/weaviate/weaviate/operations/commands"
	"github.com/weaviate/weaviate/weaviate/operations/devices"
	"github.com/weaviate/weaviate/weaviate/operations/events"
	"github.com/weaviate/weaviate/weaviate/operations/model_manifests"
	"github.com/weaviate/weaviate/weaviate/operations/personalized_infos"
	"github.com/weaviate/weaviate/weaviate/operations/places"
	"github.com/weaviate/weaviate/weaviate/operations/registration_tickets"
	"github.com/weaviate/weaviate/weaviate/operations/rooms"
	"github.com/weaviate/weaviate/weaviate/operations/subscriptions"
)

func configureFlags(api *operations.WeaviateAPI) {
	// api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{ ... }
}

// API Configuration
func configureAPI(api *operations.WeaviateAPI) http.Handler {

	api.ServeError = errors.ServeError

	api.JSONConsumer = runtime.JSONConsumer()

	api.JSONProducer = runtime.JSONProducer()

	api.Oauth2Auth = func(token string, scopes []string) (interface{}, error) {
		return nil, errors.NotImplemented("oauth2 bearer auth (Oauth2) has not yet been implemented")
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
	api.AdaptersWeaveAdaptersAcceptHandler = adapters.WeaveAdaptersAcceptHandlerFunc(func(params adapters.WeaveAdaptersAcceptParams) middleware.Responder {
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
	api.AuthorizedAppsWeaveAuthorizedAppsCreateAppAuthenticationTokenHandler = authorized_apps.WeaveAuthorizedAppsCreateAppAuthenticationTokenHandlerFunc(func(params authorized_apps.WeaveAuthorizedAppsCreateAppAuthenticationTokenParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation authorized_apps.WeaveAuthorizedAppsCreateAppAuthenticationToken has not yet been implemented")
	})
	api.AuthorizedAppsWeaveAuthorizedAppsListHandler = authorized_apps.WeaveAuthorizedAppsListHandlerFunc(func(params authorized_apps.WeaveAuthorizedAppsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation authorized_apps.WeaveAuthorizedAppsList has not yet been implemented")
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
	api.CommandsWeaveCommandsGetQueueHandler = commands.WeaveCommandsGetQueueHandlerFunc(func(params commands.WeaveCommandsGetQueueParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsGetQueue has not yet been implemented")
	})
	api.CommandsWeaveCommandsInsertHandler = commands.WeaveCommandsInsertHandlerFunc(func(params commands.WeaveCommandsInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsInsert has not yet been implemented")
	})
	api.CommandsWeaveCommandsListHandler = commands.WeaveCommandsListHandlerFunc(func(params commands.WeaveCommandsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsList has not yet been implemented")
	})
	api.CommandsWeaveCommandsPatchHandler = commands.WeaveCommandsPatchHandlerFunc(func(params commands.WeaveCommandsPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsPatch has not yet been implemented")
	})
	api.CommandsWeaveCommandsUpdateHandler = commands.WeaveCommandsUpdateHandlerFunc(func(params commands.WeaveCommandsUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation commands.WeaveCommandsUpdate has not yet been implemented")
	})
	api.DevicesWeaveDevicesAddtoplaceHandler = devices.WeaveDevicesAddtoplaceHandlerFunc(func(params devices.WeaveDevicesAddtoplaceParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesAddtoplace has not yet been implemented")
	})
	api.DevicesWeaveDevicesCreateLocalAuthTokensHandler = devices.WeaveDevicesCreateLocalAuthTokensHandlerFunc(func(params devices.WeaveDevicesCreateLocalAuthTokensParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesCreateLocalAuthTokens has not yet been implemented")
	})
	api.DevicesWeaveDevicesDeleteHandler = devices.WeaveDevicesDeleteHandlerFunc(func(params devices.WeaveDevicesDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesDelete has not yet been implemented")
	})
	api.DevicesWeaveDevicesGetHandler = devices.WeaveDevicesGetHandlerFunc(func(params devices.WeaveDevicesGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesGet has not yet been implemented")
	})
	api.DevicesWeaveDevicesHandleInvitationHandler = devices.WeaveDevicesHandleInvitationHandlerFunc(func(params devices.WeaveDevicesHandleInvitationParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesHandleInvitation has not yet been implemented")
	})
	api.DevicesWeaveDevicesInsertHandler = devices.WeaveDevicesInsertHandlerFunc(func(params devices.WeaveDevicesInsertParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesInsert has not yet been implemented")
	})
	api.DevicesWeaveDevicesListHandler = devices.WeaveDevicesListHandlerFunc(func(params devices.WeaveDevicesListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesList has not yet been implemented")
	})
	api.DevicesWeaveDevicesPatchHandler = devices.WeaveDevicesPatchHandlerFunc(func(params devices.WeaveDevicesPatchParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesPatch has not yet been implemented")
	})
	api.DevicesWeaveDevicesPatchStateHandler = devices.WeaveDevicesPatchStateHandlerFunc(func(params devices.WeaveDevicesPatchStateParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesPatchState has not yet been implemented")
	})
	api.DevicesWeaveDevicesSetroomHandler = devices.WeaveDevicesSetroomHandlerFunc(func(params devices.WeaveDevicesSetroomParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesSetroom has not yet been implemented")
	})
	api.DevicesWeaveDevicesUpdateHandler = devices.WeaveDevicesUpdateHandlerFunc(func(params devices.WeaveDevicesUpdateParams) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesUpdate has not yet been implemented")
	})
	api.DevicesWeaveDevicesUpdateParentHandler = devices.WeaveDevicesUpdateParentHandlerFunc(func(params devices.WeaveDevicesUpdateParentParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesUpdateParent has not yet been implemented")
	})
	api.DevicesWeaveDevicesUpsertLocalAuthInfoHandler = devices.WeaveDevicesUpsertLocalAuthInfoHandlerFunc(func(params devices.WeaveDevicesUpsertLocalAuthInfoParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation devices.WeaveDevicesUpsertLocalAuthInfo has not yet been implemented")
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
	api.PersonalizedInfosWeavePersonalizedInfosGetHandler = personalized_infos.WeavePersonalizedInfosGetHandlerFunc(func(params personalized_infos.WeavePersonalizedInfosGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation personalized_infos.WeavePersonalizedInfosGet has not yet been implemented")
	})
	api.PersonalizedInfosWeavePersonalizedInfosPatchHandler = personalized_infos.WeavePersonalizedInfosPatchHandlerFunc(func(params personalized_infos.WeavePersonalizedInfosPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation personalized_infos.WeavePersonalizedInfosPatch has not yet been implemented")
	})
	api.PersonalizedInfosWeavePersonalizedInfosUpdateHandler = personalized_infos.WeavePersonalizedInfosUpdateHandlerFunc(func(params personalized_infos.WeavePersonalizedInfosUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation personalized_infos.WeavePersonalizedInfosUpdate has not yet been implemented")
	})
	api.PlacesWeavePlacesAddMemberHandler = places.WeavePlacesAddMemberHandlerFunc(func(params places.WeavePlacesAddMemberParams) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesAddMember has not yet been implemented")
	})
	api.PlacesWeavePlacesCreateHandler = places.WeavePlacesCreateHandlerFunc(func(params places.WeavePlacesCreateParams) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesCreate has not yet been implemented")
	})
	api.PlacesWeavePlacesDeleteHandler = places.WeavePlacesDeleteHandlerFunc(func(params places.WeavePlacesDeleteParams) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesDelete has not yet been implemented")
	})
	api.PlacesWeavePlacesGetHandler = places.WeavePlacesGetHandlerFunc(func(params places.WeavePlacesGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesGet has not yet been implemented")
	})
	api.PlacesWeavePlacesHandleInvitationHandler = places.WeavePlacesHandleInvitationHandlerFunc(func(params places.WeavePlacesHandleInvitationParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesHandleInvitation has not yet been implemented")
	})
	api.PlacesWeavePlacesListHandler = places.WeavePlacesListHandlerFunc(func(params places.WeavePlacesListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesList has not yet been implemented")
	})
	api.PlacesWeavePlacesListSuggestionsHandler = places.WeavePlacesListSuggestionsHandlerFunc(func(params places.WeavePlacesListSuggestionsParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesListSuggestions has not yet been implemented")
	})
	api.PlacesWeavePlacesModifyHandler = places.WeavePlacesModifyHandlerFunc(func(params places.WeavePlacesModifyParams) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesModify has not yet been implemented")
	})
	api.PlacesWeavePlacesRemoveMemberHandler = places.WeavePlacesRemoveMemberHandlerFunc(func(params places.WeavePlacesRemoveMemberParams) middleware.Responder {
		return middleware.NotImplemented("operation places.WeavePlacesRemoveMember has not yet been implemented")
	})
	api.RegistrationTicketsWeaveRegistrationTicketsFinalizeHandler = registration_tickets.WeaveRegistrationTicketsFinalizeHandlerFunc(func(params registration_tickets.WeaveRegistrationTicketsFinalizeParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation registration_tickets.WeaveRegistrationTicketsFinalize has not yet been implemented")
	})
	api.RegistrationTicketsWeaveRegistrationTicketsGetHandler = registration_tickets.WeaveRegistrationTicketsGetHandlerFunc(func(params registration_tickets.WeaveRegistrationTicketsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation registration_tickets.WeaveRegistrationTicketsGet has not yet been implemented")
	})
	api.RegistrationTicketsWeaveRegistrationTicketsInsertHandler = registration_tickets.WeaveRegistrationTicketsInsertHandlerFunc(func(params registration_tickets.WeaveRegistrationTicketsInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation registration_tickets.WeaveRegistrationTicketsInsert has not yet been implemented")
	})
	api.RegistrationTicketsWeaveRegistrationTicketsPatchHandler = registration_tickets.WeaveRegistrationTicketsPatchHandlerFunc(func(params registration_tickets.WeaveRegistrationTicketsPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation registration_tickets.WeaveRegistrationTicketsPatch has not yet been implemented")
	})
	api.RegistrationTicketsWeaveRegistrationTicketsUpdateHandler = registration_tickets.WeaveRegistrationTicketsUpdateHandlerFunc(func(params registration_tickets.WeaveRegistrationTicketsUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation registration_tickets.WeaveRegistrationTicketsUpdate has not yet been implemented")
	})
	api.RoomsWeaveRoomsCreateHandler = rooms.WeaveRoomsCreateHandlerFunc(func(params rooms.WeaveRoomsCreateParams) middleware.Responder {
		return middleware.NotImplemented("operation rooms.WeaveRoomsCreate has not yet been implemented")
	})
	api.RoomsWeaveRoomsDeleteHandler = rooms.WeaveRoomsDeleteHandlerFunc(func(params rooms.WeaveRoomsDeleteParams) middleware.Responder {
		return middleware.NotImplemented("operation rooms.WeaveRoomsDelete has not yet been implemented")
	})
	api.RoomsWeaveRoomsGetHandler = rooms.WeaveRoomsGetHandlerFunc(func(params rooms.WeaveRoomsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation rooms.WeaveRoomsGet has not yet been implemented")
	})
	api.RoomsWeaveRoomsListHandler = rooms.WeaveRoomsListHandlerFunc(func(params rooms.WeaveRoomsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation rooms.WeaveRoomsList has not yet been implemented")
	})
	api.RoomsWeaveRoomsModifyHandler = rooms.WeaveRoomsModifyHandlerFunc(func(params rooms.WeaveRoomsModifyParams) middleware.Responder {
		return middleware.NotImplemented("operation rooms.WeaveRoomsModify has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsDeleteHandler = subscriptions.WeaveSubscriptionsDeleteHandlerFunc(func(params subscriptions.WeaveSubscriptionsDeleteParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsDelete has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsGetHandler = subscriptions.WeaveSubscriptionsGetHandlerFunc(func(params subscriptions.WeaveSubscriptionsGetParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsGet has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsGetNotificationsHandler = subscriptions.WeaveSubscriptionsGetNotificationsHandlerFunc(func(params subscriptions.WeaveSubscriptionsGetNotificationsParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsGetNotifications has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsInsertHandler = subscriptions.WeaveSubscriptionsInsertHandlerFunc(func(params subscriptions.WeaveSubscriptionsInsertParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsInsert has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsListHandler = subscriptions.WeaveSubscriptionsListHandlerFunc(func(params subscriptions.WeaveSubscriptionsListParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsList has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsPatchHandler = subscriptions.WeaveSubscriptionsPatchHandlerFunc(func(params subscriptions.WeaveSubscriptionsPatchParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsPatch has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsSubscribeHandler = subscriptions.WeaveSubscriptionsSubscribeHandlerFunc(func(params subscriptions.WeaveSubscriptionsSubscribeParams) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsSubscribe has not yet been implemented")
	})
	api.SubscriptionsWeaveSubscriptionsUpdateHandler = subscriptions.WeaveSubscriptionsUpdateHandlerFunc(func(params subscriptions.WeaveSubscriptionsUpdateParams, principal interface{}) middleware.Responder {
		return middleware.NotImplemented("operation subscriptions.WeaveSubscriptionsUpdate has not yet been implemented")
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
