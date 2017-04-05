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
 package commands




import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveCommandsPatchHandlerFunc turns a function with the right signature into a weave commands patch handler
type WeaveCommandsPatchHandlerFunc func(WeaveCommandsPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveCommandsPatchHandlerFunc) Handle(params WeaveCommandsPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveCommandsPatchHandler interface for that can handle valid weave commands patch params
type WeaveCommandsPatchHandler interface {
	Handle(WeaveCommandsPatchParams, interface{}) middleware.Responder
}

// NewWeaveCommandsPatch creates a new http.Handler for the weave commands patch operation
func NewWeaveCommandsPatch(ctx *middleware.Context, handler WeaveCommandsPatchHandler) *WeaveCommandsPatch {
	return &WeaveCommandsPatch{Context: ctx, Handler: handler}
}

/*WeaveCommandsPatch swagger:route PATCH /commands/{commandId} commands weaveCommandsPatch

Updates a command. This method may be used only by devices. This method supports patch semantics.

*/
type WeaveCommandsPatch struct {
	Context *middleware.Context
	Handler WeaveCommandsPatchHandler
}

func (o *WeaveCommandsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveCommandsPatchParams()

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
