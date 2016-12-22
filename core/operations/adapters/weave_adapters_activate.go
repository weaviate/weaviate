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
 package adapters


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveAdaptersActivateHandlerFunc turns a function with the right signature into a weave adapters activate handler
type WeaveAdaptersActivateHandlerFunc func(WeaveAdaptersActivateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveAdaptersActivateHandlerFunc) Handle(params WeaveAdaptersActivateParams) middleware.Responder {
	return fn(params)
}

// WeaveAdaptersActivateHandler interface for that can handle valid weave adapters activate params
type WeaveAdaptersActivateHandler interface {
	Handle(WeaveAdaptersActivateParams) middleware.Responder
}

// NewWeaveAdaptersActivate creates a new http.Handler for the weave adapters activate operation
func NewWeaveAdaptersActivate(ctx *middleware.Context, handler WeaveAdaptersActivateHandler) *WeaveAdaptersActivate {
	return &WeaveAdaptersActivate{Context: ctx, Handler: handler}
}

/*WeaveAdaptersActivate swagger:route POST /adapters/{adapterId}/activate adapters weaveAdaptersActivate

Activates an adapter. The activation will be contingent upon the adapter provider accepting the activation. If the activation is not accepted within 15 minutes, the activation will be deleted.

*/
type WeaveAdaptersActivate struct {
	Context *middleware.Context
	Handler WeaveAdaptersActivateHandler
}

func (o *WeaveAdaptersActivate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveAdaptersActivateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
