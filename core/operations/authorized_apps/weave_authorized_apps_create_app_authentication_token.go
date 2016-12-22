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
 package authorized_apps


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveAuthorizedAppsCreateAppAuthenticationTokenHandlerFunc turns a function with the right signature into a weave authorized apps create app authentication token handler
type WeaveAuthorizedAppsCreateAppAuthenticationTokenHandlerFunc func(WeaveAuthorizedAppsCreateAppAuthenticationTokenParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveAuthorizedAppsCreateAppAuthenticationTokenHandlerFunc) Handle(params WeaveAuthorizedAppsCreateAppAuthenticationTokenParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveAuthorizedAppsCreateAppAuthenticationTokenHandler interface for that can handle valid weave authorized apps create app authentication token params
type WeaveAuthorizedAppsCreateAppAuthenticationTokenHandler interface {
	Handle(WeaveAuthorizedAppsCreateAppAuthenticationTokenParams, interface{}) middleware.Responder
}

// NewWeaveAuthorizedAppsCreateAppAuthenticationToken creates a new http.Handler for the weave authorized apps create app authentication token operation
func NewWeaveAuthorizedAppsCreateAppAuthenticationToken(ctx *middleware.Context, handler WeaveAuthorizedAppsCreateAppAuthenticationTokenHandler) *WeaveAuthorizedAppsCreateAppAuthenticationToken {
	return &WeaveAuthorizedAppsCreateAppAuthenticationToken{Context: ctx, Handler: handler}
}

/*WeaveAuthorizedAppsCreateAppAuthenticationToken swagger:route POST /authorizedApps/createAppAuthenticationToken authorizedApps weaveAuthorizedAppsCreateAppAuthenticationToken

Generate a token used to authenticate an authorized app.

*/
type WeaveAuthorizedAppsCreateAppAuthenticationToken struct {
	Context *middleware.Context
	Handler WeaveAuthorizedAppsCreateAppAuthenticationTokenHandler
}

func (o *WeaveAuthorizedAppsCreateAppAuthenticationToken) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveAuthorizedAppsCreateAppAuthenticationTokenParams()

	uprinc, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	var principal interface{}
	if uprinc != nil {
		principal = uprinc
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
