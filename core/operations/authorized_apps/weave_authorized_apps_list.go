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

// WeaveAuthorizedAppsListHandlerFunc turns a function with the right signature into a weave authorized apps list handler
type WeaveAuthorizedAppsListHandlerFunc func(WeaveAuthorizedAppsListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveAuthorizedAppsListHandlerFunc) Handle(params WeaveAuthorizedAppsListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveAuthorizedAppsListHandler interface for that can handle valid weave authorized apps list params
type WeaveAuthorizedAppsListHandler interface {
	Handle(WeaveAuthorizedAppsListParams, interface{}) middleware.Responder
}

// NewWeaveAuthorizedAppsList creates a new http.Handler for the weave authorized apps list operation
func NewWeaveAuthorizedAppsList(ctx *middleware.Context, handler WeaveAuthorizedAppsListHandler) *WeaveAuthorizedAppsList {
	return &WeaveAuthorizedAppsList{Context: ctx, Handler: handler}
}

/*WeaveAuthorizedAppsList swagger:route GET /authorizedApps authorizedApps weaveAuthorizedAppsList

The actual list of authorized apps.

*/
type WeaveAuthorizedAppsList struct {
	Context *middleware.Context
	Handler WeaveAuthorizedAppsListHandler
}

func (o *WeaveAuthorizedAppsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveAuthorizedAppsListParams()

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
