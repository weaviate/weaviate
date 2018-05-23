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

package operations

import (
	"fmt"
	"net/http"
	"strings"

	errors "github.com/go-openapi/errors"
	loads "github.com/go-openapi/loads"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	security "github.com/go-openapi/runtime/security"
	spec "github.com/go-openapi/spec"
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/creativesoftwarefdn/weaviate/restapi/operations/actions"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/graphql"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/keys"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/meta"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/p2_p"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/things"
)

// NewWeaviateAPI creates a new Weaviate instance
func NewWeaviateAPI(spec *loads.Document) *WeaviateAPI {
	return &WeaviateAPI{
		handlers:            make(map[string]map[string]http.Handler),
		formats:             strfmt.Default,
		defaultConsumes:     "application/json",
		defaultProduces:     "application/json",
		ServerShutdown:      func() {},
		spec:                spec,
		ServeError:          errors.ServeError,
		BasicAuthenticator:  security.BasicAuth,
		APIKeyAuthenticator: security.APIKeyAuth,
		BearerAuthenticator: security.BearerAuth,
		JSONConsumer:        runtime.JSONConsumer(),
		JSONProducer:        runtime.JSONProducer(),
		ActionsWeaviateActionHistoryGetHandler: actions.WeaviateActionHistoryGetHandlerFunc(func(params actions.WeaviateActionHistoryGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ActionsWeaviateActionHistoryGet has not yet been implemented")
		}),
		ActionsWeaviateActionUpdateHandler: actions.WeaviateActionUpdateHandlerFunc(func(params actions.WeaviateActionUpdateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ActionsWeaviateActionUpdate has not yet been implemented")
		}),
		ActionsWeaviateActionsCreateHandler: actions.WeaviateActionsCreateHandlerFunc(func(params actions.WeaviateActionsCreateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ActionsWeaviateActionsCreate has not yet been implemented")
		}),
		ActionsWeaviateActionsDeleteHandler: actions.WeaviateActionsDeleteHandlerFunc(func(params actions.WeaviateActionsDeleteParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ActionsWeaviateActionsDelete has not yet been implemented")
		}),
		ActionsWeaviateActionsGetHandler: actions.WeaviateActionsGetHandlerFunc(func(params actions.WeaviateActionsGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ActionsWeaviateActionsGet has not yet been implemented")
		}),
		ActionsWeaviateActionsPatchHandler: actions.WeaviateActionsPatchHandlerFunc(func(params actions.WeaviateActionsPatchParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ActionsWeaviateActionsPatch has not yet been implemented")
		}),
		ActionsWeaviateActionsValidateHandler: actions.WeaviateActionsValidateHandlerFunc(func(params actions.WeaviateActionsValidateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ActionsWeaviateActionsValidate has not yet been implemented")
		}),
		GraphqlWeaviateGraphqlPostHandler: graphql.WeaviateGraphqlPostHandlerFunc(func(params graphql.WeaviateGraphqlPostParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation GraphqlWeaviateGraphqlPost has not yet been implemented")
		}),
		KeysWeaviateKeyCreateHandler: keys.WeaviateKeyCreateHandlerFunc(func(params keys.WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation KeysWeaviateKeyCreate has not yet been implemented")
		}),
		KeysWeaviateKeysChildrenGetHandler: keys.WeaviateKeysChildrenGetHandlerFunc(func(params keys.WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation KeysWeaviateKeysChildrenGet has not yet been implemented")
		}),
		KeysWeaviateKeysDeleteHandler: keys.WeaviateKeysDeleteHandlerFunc(func(params keys.WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation KeysWeaviateKeysDelete has not yet been implemented")
		}),
		KeysWeaviateKeysGetHandler: keys.WeaviateKeysGetHandlerFunc(func(params keys.WeaviateKeysGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation KeysWeaviateKeysGet has not yet been implemented")
		}),
		KeysWeaviateKeysMeChildrenGetHandler: keys.WeaviateKeysMeChildrenGetHandlerFunc(func(params keys.WeaviateKeysMeChildrenGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation KeysWeaviateKeysMeChildrenGet has not yet been implemented")
		}),
		KeysWeaviateKeysMeGetHandler: keys.WeaviateKeysMeGetHandlerFunc(func(params keys.WeaviateKeysMeGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation KeysWeaviateKeysMeGet has not yet been implemented")
		}),
		KeysWeaviateKeysRenewTokenHandler: keys.WeaviateKeysRenewTokenHandlerFunc(func(params keys.WeaviateKeysRenewTokenParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation KeysWeaviateKeysRenewToken has not yet been implemented")
		}),
		MetaWeaviateMetaGetHandler: meta.WeaviateMetaGetHandlerFunc(func(params meta.WeaviateMetaGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation MetaWeaviateMetaGet has not yet been implemented")
		}),
		P2PWeaviatePeersAnnounceHandler: p2_p.WeaviatePeersAnnounceHandlerFunc(func(params p2_p.WeaviatePeersAnnounceParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation P2PWeaviatePeersAnnounce has not yet been implemented")
		}),
		P2PWeaviatePeersAnswersCreateHandler: p2_p.WeaviatePeersAnswersCreateHandlerFunc(func(params p2_p.WeaviatePeersAnswersCreateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation P2PWeaviatePeersAnswersCreate has not yet been implemented")
		}),
		P2PWeaviatePeersEchoHandler: p2_p.WeaviatePeersEchoHandlerFunc(func(params p2_p.WeaviatePeersEchoParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation P2PWeaviatePeersEcho has not yet been implemented")
		}),
		P2PWeaviatePeersQuestionsCreateHandler: p2_p.WeaviatePeersQuestionsCreateHandlerFunc(func(params p2_p.WeaviatePeersQuestionsCreateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation P2PWeaviatePeersQuestionsCreate has not yet been implemented")
		}),
		ThingsWeaviateThingHistoryGetHandler: things.WeaviateThingHistoryGetHandlerFunc(func(params things.WeaviateThingHistoryGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingHistoryGet has not yet been implemented")
		}),
		ThingsWeaviateThingsActionsListHandler: things.WeaviateThingsActionsListHandlerFunc(func(params things.WeaviateThingsActionsListParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsActionsList has not yet been implemented")
		}),
		ThingsWeaviateThingsCreateHandler: things.WeaviateThingsCreateHandlerFunc(func(params things.WeaviateThingsCreateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsCreate has not yet been implemented")
		}),
		ThingsWeaviateThingsDeleteHandler: things.WeaviateThingsDeleteHandlerFunc(func(params things.WeaviateThingsDeleteParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsDelete has not yet been implemented")
		}),
		ThingsWeaviateThingsGetHandler: things.WeaviateThingsGetHandlerFunc(func(params things.WeaviateThingsGetParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsGet has not yet been implemented")
		}),
		ThingsWeaviateThingsListHandler: things.WeaviateThingsListHandlerFunc(func(params things.WeaviateThingsListParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsList has not yet been implemented")
		}),
		ThingsWeaviateThingsPatchHandler: things.WeaviateThingsPatchHandlerFunc(func(params things.WeaviateThingsPatchParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsPatch has not yet been implemented")
		}),
		ThingsWeaviateThingsUpdateHandler: things.WeaviateThingsUpdateHandlerFunc(func(params things.WeaviateThingsUpdateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsUpdate has not yet been implemented")
		}),
		ThingsWeaviateThingsValidateHandler: things.WeaviateThingsValidateHandlerFunc(func(params things.WeaviateThingsValidateParams, principal interface{}) middleware.Responder {
			return middleware.NotImplemented("operation ThingsWeaviateThingsValidate has not yet been implemented")
		}),

		// Applies when the "X-API-KEY" header is set
		APIKeyAuth: func(token string) (interface{}, error) {
			return nil, errors.NotImplemented("api key auth (apiKey) X-API-KEY from header param [X-API-KEY] has not yet been implemented")
		},
		// Applies when the "X-API-TOKEN" header is set
		APITokenAuth: func(token string) (interface{}, error) {
			return nil, errors.NotImplemented("api key auth (apiToken) X-API-TOKEN from header param [X-API-TOKEN] has not yet been implemented")
		},

		// default authorizer is authorized meaning no requests are blocked
		APIAuthorizer: security.Authorized(),
	}
}

/*WeaviateAPI Weaviate - Semantic Graphql, RESTful Web of Things platform. */
type WeaviateAPI struct {
	spec            *loads.Document
	context         *middleware.Context
	handlers        map[string]map[string]http.Handler
	formats         strfmt.Registry
	defaultConsumes string
	defaultProduces string
	Middleware      func(middleware.Builder) http.Handler

	// BasicAuthenticator generates a runtime.Authenticator from the supplied basic auth function.
	// It has a default implemention in the security package, however you can replace it for your particular usage.
	BasicAuthenticator func(security.UserPassAuthentication) runtime.Authenticator
	// APIKeyAuthenticator generates a runtime.Authenticator from the supplied token auth function.
	// It has a default implemention in the security package, however you can replace it for your particular usage.
	APIKeyAuthenticator func(string, string, security.TokenAuthentication) runtime.Authenticator
	// BearerAuthenticator generates a runtime.Authenticator from the supplied bearer token auth function.
	// It has a default implemention in the security package, however you can replace it for your particular usage.
	BearerAuthenticator func(string, security.ScopedTokenAuthentication) runtime.Authenticator

	// JSONConsumer registers a consumer for a "application/json" mime type
	JSONConsumer runtime.Consumer

	// JSONProducer registers a producer for a "application/json" mime type
	JSONProducer runtime.Producer

	// APIKeyAuth registers a function that takes a token and returns a principal
	// it performs authentication based on an api key X-API-KEY provided in the header
	APIKeyAuth func(string) (interface{}, error)

	// APITokenAuth registers a function that takes a token and returns a principal
	// it performs authentication based on an api key X-API-TOKEN provided in the header
	APITokenAuth func(string) (interface{}, error)

	// APIAuthorizer provides access control (ACL/RBAC/ABAC) by providing access to the request and authenticated principal
	APIAuthorizer runtime.Authorizer

	// ActionsWeaviateActionHistoryGetHandler sets the operation handler for the weaviate action history get operation
	ActionsWeaviateActionHistoryGetHandler actions.WeaviateActionHistoryGetHandler
	// ActionsWeaviateActionUpdateHandler sets the operation handler for the weaviate action update operation
	ActionsWeaviateActionUpdateHandler actions.WeaviateActionUpdateHandler
	// ActionsWeaviateActionsCreateHandler sets the operation handler for the weaviate actions create operation
	ActionsWeaviateActionsCreateHandler actions.WeaviateActionsCreateHandler
	// ActionsWeaviateActionsDeleteHandler sets the operation handler for the weaviate actions delete operation
	ActionsWeaviateActionsDeleteHandler actions.WeaviateActionsDeleteHandler
	// ActionsWeaviateActionsGetHandler sets the operation handler for the weaviate actions get operation
	ActionsWeaviateActionsGetHandler actions.WeaviateActionsGetHandler
	// ActionsWeaviateActionsPatchHandler sets the operation handler for the weaviate actions patch operation
	ActionsWeaviateActionsPatchHandler actions.WeaviateActionsPatchHandler
	// ActionsWeaviateActionsValidateHandler sets the operation handler for the weaviate actions validate operation
	ActionsWeaviateActionsValidateHandler actions.WeaviateActionsValidateHandler
	// GraphqlWeaviateGraphqlPostHandler sets the operation handler for the weaviate graphql post operation
	GraphqlWeaviateGraphqlPostHandler graphql.WeaviateGraphqlPostHandler
	// KeysWeaviateKeyCreateHandler sets the operation handler for the weaviate key create operation
	KeysWeaviateKeyCreateHandler keys.WeaviateKeyCreateHandler
	// KeysWeaviateKeysChildrenGetHandler sets the operation handler for the weaviate keys children get operation
	KeysWeaviateKeysChildrenGetHandler keys.WeaviateKeysChildrenGetHandler
	// KeysWeaviateKeysDeleteHandler sets the operation handler for the weaviate keys delete operation
	KeysWeaviateKeysDeleteHandler keys.WeaviateKeysDeleteHandler
	// KeysWeaviateKeysGetHandler sets the operation handler for the weaviate keys get operation
	KeysWeaviateKeysGetHandler keys.WeaviateKeysGetHandler
	// KeysWeaviateKeysMeChildrenGetHandler sets the operation handler for the weaviate keys me children get operation
	KeysWeaviateKeysMeChildrenGetHandler keys.WeaviateKeysMeChildrenGetHandler
	// KeysWeaviateKeysMeGetHandler sets the operation handler for the weaviate keys me get operation
	KeysWeaviateKeysMeGetHandler keys.WeaviateKeysMeGetHandler
	// KeysWeaviateKeysRenewTokenHandler sets the operation handler for the weaviate keys renew token operation
	KeysWeaviateKeysRenewTokenHandler keys.WeaviateKeysRenewTokenHandler
	// MetaWeaviateMetaGetHandler sets the operation handler for the weaviate meta get operation
	MetaWeaviateMetaGetHandler meta.WeaviateMetaGetHandler
	// P2PWeaviatePeersAnnounceHandler sets the operation handler for the weaviate peers announce operation
	P2PWeaviatePeersAnnounceHandler p2_p.WeaviatePeersAnnounceHandler
	// P2PWeaviatePeersAnswersCreateHandler sets the operation handler for the weaviate peers answers create operation
	P2PWeaviatePeersAnswersCreateHandler p2_p.WeaviatePeersAnswersCreateHandler
	// P2PWeaviatePeersEchoHandler sets the operation handler for the weaviate peers echo operation
	P2PWeaviatePeersEchoHandler p2_p.WeaviatePeersEchoHandler
	// P2PWeaviatePeersQuestionsCreateHandler sets the operation handler for the weaviate peers questions create operation
	P2PWeaviatePeersQuestionsCreateHandler p2_p.WeaviatePeersQuestionsCreateHandler
	// ThingsWeaviateThingHistoryGetHandler sets the operation handler for the weaviate thing history get operation
	ThingsWeaviateThingHistoryGetHandler things.WeaviateThingHistoryGetHandler
	// ThingsWeaviateThingsActionsListHandler sets the operation handler for the weaviate things actions list operation
	ThingsWeaviateThingsActionsListHandler things.WeaviateThingsActionsListHandler
	// ThingsWeaviateThingsCreateHandler sets the operation handler for the weaviate things create operation
	ThingsWeaviateThingsCreateHandler things.WeaviateThingsCreateHandler
	// ThingsWeaviateThingsDeleteHandler sets the operation handler for the weaviate things delete operation
	ThingsWeaviateThingsDeleteHandler things.WeaviateThingsDeleteHandler
	// ThingsWeaviateThingsGetHandler sets the operation handler for the weaviate things get operation
	ThingsWeaviateThingsGetHandler things.WeaviateThingsGetHandler
	// ThingsWeaviateThingsListHandler sets the operation handler for the weaviate things list operation
	ThingsWeaviateThingsListHandler things.WeaviateThingsListHandler
	// ThingsWeaviateThingsPatchHandler sets the operation handler for the weaviate things patch operation
	ThingsWeaviateThingsPatchHandler things.WeaviateThingsPatchHandler
	// ThingsWeaviateThingsUpdateHandler sets the operation handler for the weaviate things update operation
	ThingsWeaviateThingsUpdateHandler things.WeaviateThingsUpdateHandler
	// ThingsWeaviateThingsValidateHandler sets the operation handler for the weaviate things validate operation
	ThingsWeaviateThingsValidateHandler things.WeaviateThingsValidateHandler

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

	if o.JSONProducer == nil {
		unregistered = append(unregistered, "JSONProducer")
	}

	if o.APIKeyAuth == nil {
		unregistered = append(unregistered, "XAPIKEYAuth")
	}

	if o.APITokenAuth == nil {
		unregistered = append(unregistered, "XAPITOKENAuth")
	}

	if o.ActionsWeaviateActionHistoryGetHandler == nil {
		unregistered = append(unregistered, "actions.WeaviateActionHistoryGetHandler")
	}

	if o.ActionsWeaviateActionUpdateHandler == nil {
		unregistered = append(unregistered, "actions.WeaviateActionUpdateHandler")
	}

	if o.ActionsWeaviateActionsCreateHandler == nil {
		unregistered = append(unregistered, "actions.WeaviateActionsCreateHandler")
	}

	if o.ActionsWeaviateActionsDeleteHandler == nil {
		unregistered = append(unregistered, "actions.WeaviateActionsDeleteHandler")
	}

	if o.ActionsWeaviateActionsGetHandler == nil {
		unregistered = append(unregistered, "actions.WeaviateActionsGetHandler")
	}

	if o.ActionsWeaviateActionsPatchHandler == nil {
		unregistered = append(unregistered, "actions.WeaviateActionsPatchHandler")
	}

	if o.ActionsWeaviateActionsValidateHandler == nil {
		unregistered = append(unregistered, "actions.WeaviateActionsValidateHandler")
	}

	if o.GraphqlWeaviateGraphqlPostHandler == nil {
		unregistered = append(unregistered, "graphql.WeaviateGraphqlPostHandler")
	}

	if o.KeysWeaviateKeyCreateHandler == nil {
		unregistered = append(unregistered, "keys.WeaviateKeyCreateHandler")
	}

	if o.KeysWeaviateKeysChildrenGetHandler == nil {
		unregistered = append(unregistered, "keys.WeaviateKeysChildrenGetHandler")
	}

	if o.KeysWeaviateKeysDeleteHandler == nil {
		unregistered = append(unregistered, "keys.WeaviateKeysDeleteHandler")
	}

	if o.KeysWeaviateKeysGetHandler == nil {
		unregistered = append(unregistered, "keys.WeaviateKeysGetHandler")
	}

	if o.KeysWeaviateKeysMeChildrenGetHandler == nil {
		unregistered = append(unregistered, "keys.WeaviateKeysMeChildrenGetHandler")
	}

	if o.KeysWeaviateKeysMeGetHandler == nil {
		unregistered = append(unregistered, "keys.WeaviateKeysMeGetHandler")
	}

	if o.KeysWeaviateKeysRenewTokenHandler == nil {
		unregistered = append(unregistered, "keys.WeaviateKeysRenewTokenHandler")
	}

	if o.MetaWeaviateMetaGetHandler == nil {
		unregistered = append(unregistered, "meta.WeaviateMetaGetHandler")
	}

	if o.P2PWeaviatePeersAnnounceHandler == nil {
		unregistered = append(unregistered, "p2_p.WeaviatePeersAnnounceHandler")
	}

	if o.P2PWeaviatePeersAnswersCreateHandler == nil {
		unregistered = append(unregistered, "p2_p.WeaviatePeersAnswersCreateHandler")
	}

	if o.P2PWeaviatePeersEchoHandler == nil {
		unregistered = append(unregistered, "p2_p.WeaviatePeersEchoHandler")
	}

	if o.P2PWeaviatePeersQuestionsCreateHandler == nil {
		unregistered = append(unregistered, "p2_p.WeaviatePeersQuestionsCreateHandler")
	}

	if o.ThingsWeaviateThingHistoryGetHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingHistoryGetHandler")
	}

	if o.ThingsWeaviateThingsActionsListHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsActionsListHandler")
	}

	if o.ThingsWeaviateThingsCreateHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsCreateHandler")
	}

	if o.ThingsWeaviateThingsDeleteHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsDeleteHandler")
	}

	if o.ThingsWeaviateThingsGetHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsGetHandler")
	}

	if o.ThingsWeaviateThingsListHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsListHandler")
	}

	if o.ThingsWeaviateThingsPatchHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsPatchHandler")
	}

	if o.ThingsWeaviateThingsUpdateHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsUpdateHandler")
	}

	if o.ThingsWeaviateThingsValidateHandler == nil {
		unregistered = append(unregistered, "things.WeaviateThingsValidateHandler")
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

			result[name] = o.APIKeyAuthenticator(scheme.Name, scheme.In, o.APIKeyAuth)

		case "apiToken":

			result[name] = o.APIKeyAuthenticator(scheme.Name, scheme.In, o.APITokenAuth)

		}
	}
	return result

}

// Authorizer returns the registered authorizer
func (o *WeaviateAPI) Authorizer() runtime.Authorizer {

	return o.APIAuthorizer

}

// ConsumersFor gets the consumers for the specified media types
func (o *WeaviateAPI) ConsumersFor(mediaTypes []string) map[string]runtime.Consumer {

	result := make(map[string]runtime.Consumer)
	for _, mt := range mediaTypes {
		switch mt {

		case "application/json":
			result["application/json"] = o.JSONConsumer

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

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/actions/{actionId}/history"] = actions.NewWeaviateActionHistoryGet(o.context, o.ActionsWeaviateActionHistoryGetHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/actions/{actionId}"] = actions.NewWeaviateActionUpdate(o.context, o.ActionsWeaviateActionUpdateHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/actions"] = actions.NewWeaviateActionsCreate(o.context, o.ActionsWeaviateActionsCreateHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/actions/{actionId}"] = actions.NewWeaviateActionsDelete(o.context, o.ActionsWeaviateActionsDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/actions/{actionId}"] = actions.NewWeaviateActionsGet(o.context, o.ActionsWeaviateActionsGetHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/actions/{actionId}"] = actions.NewWeaviateActionsPatch(o.context, o.ActionsWeaviateActionsPatchHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/actions/validate"] = actions.NewWeaviateActionsValidate(o.context, o.ActionsWeaviateActionsValidateHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/graphql"] = graphql.NewWeaviateGraphqlPost(o.context, o.GraphqlWeaviateGraphqlPostHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/keys"] = keys.NewWeaviateKeyCreate(o.context, o.KeysWeaviateKeyCreateHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/keys/{keyId}/children"] = keys.NewWeaviateKeysChildrenGet(o.context, o.KeysWeaviateKeysChildrenGetHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/keys/{keyId}"] = keys.NewWeaviateKeysDelete(o.context, o.KeysWeaviateKeysDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/keys/{keyId}"] = keys.NewWeaviateKeysGet(o.context, o.KeysWeaviateKeysGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/keys/me/children"] = keys.NewWeaviateKeysMeChildrenGet(o.context, o.KeysWeaviateKeysMeChildrenGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/keys/me"] = keys.NewWeaviateKeysMeGet(o.context, o.KeysWeaviateKeysMeGetHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/keys/{keyId}/renew-token"] = keys.NewWeaviateKeysRenewToken(o.context, o.KeysWeaviateKeysRenewTokenHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/meta"] = meta.NewWeaviateMetaGet(o.context, o.MetaWeaviateMetaGetHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/peers"] = p2_p.NewWeaviatePeersAnnounce(o.context, o.P2PWeaviatePeersAnnounceHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/peers/answers/{answerId}"] = p2_p.NewWeaviatePeersAnswersCreate(o.context, o.P2PWeaviatePeersAnswersCreateHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/peers/echo"] = p2_p.NewWeaviatePeersEcho(o.context, o.P2PWeaviatePeersEchoHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/peers/questions"] = p2_p.NewWeaviatePeersQuestionsCreate(o.context, o.P2PWeaviatePeersQuestionsCreateHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/things/{thingId}/history"] = things.NewWeaviateThingHistoryGet(o.context, o.ThingsWeaviateThingHistoryGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/things/{thingId}/actions"] = things.NewWeaviateThingsActionsList(o.context, o.ThingsWeaviateThingsActionsListHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/things"] = things.NewWeaviateThingsCreate(o.context, o.ThingsWeaviateThingsCreateHandler)

	if o.handlers["DELETE"] == nil {
		o.handlers["DELETE"] = make(map[string]http.Handler)
	}
	o.handlers["DELETE"]["/things/{thingId}"] = things.NewWeaviateThingsDelete(o.context, o.ThingsWeaviateThingsDeleteHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/things/{thingId}"] = things.NewWeaviateThingsGet(o.context, o.ThingsWeaviateThingsGetHandler)

	if o.handlers["GET"] == nil {
		o.handlers["GET"] = make(map[string]http.Handler)
	}
	o.handlers["GET"]["/things"] = things.NewWeaviateThingsList(o.context, o.ThingsWeaviateThingsListHandler)

	if o.handlers["PATCH"] == nil {
		o.handlers["PATCH"] = make(map[string]http.Handler)
	}
	o.handlers["PATCH"]["/things/{thingId}"] = things.NewWeaviateThingsPatch(o.context, o.ThingsWeaviateThingsPatchHandler)

	if o.handlers["PUT"] == nil {
		o.handlers["PUT"] = make(map[string]http.Handler)
	}
	o.handlers["PUT"]["/things/{thingId}"] = things.NewWeaviateThingsUpdate(o.context, o.ThingsWeaviateThingsUpdateHandler)

	if o.handlers["POST"] == nil {
		o.handlers["POST"] = make(map[string]http.Handler)
	}
	o.handlers["POST"]["/things/validate"] = things.NewWeaviateThingsValidate(o.context, o.ThingsWeaviateThingsValidateHandler)

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
