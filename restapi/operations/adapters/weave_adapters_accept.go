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




import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveAdaptersAcceptHandlerFunc turns a function with the right signature into a weave adapters accept handler
type WeaveAdaptersAcceptHandlerFunc func(WeaveAdaptersAcceptParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveAdaptersAcceptHandlerFunc) Handle(params WeaveAdaptersAcceptParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveAdaptersAcceptHandler interface for that can handle valid weave adapters accept params
type WeaveAdaptersAcceptHandler interface {
	Handle(WeaveAdaptersAcceptParams, interface{}) middleware.Responder
}

// NewWeaveAdaptersAccept creates a new http.Handler for the weave adapters accept operation
func NewWeaveAdaptersAccept(ctx *middleware.Context, handler WeaveAdaptersAcceptHandler) *WeaveAdaptersAccept {
	return &WeaveAdaptersAccept{Context: ctx, Handler: handler}
}

/*WeaveAdaptersAccept swagger:route POST /adapters/accept adapters weaveAdaptersAccept

Used by an adapter provider to accept an activation and prevent it from expiring.

*/
type WeaveAdaptersAccept struct {
	Context *middleware.Context
	Handler WeaveAdaptersAcceptHandler
}

func (o *WeaveAdaptersAccept) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveAdaptersAcceptParams()

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
