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

// WeaveCommandsDeleteHandlerFunc turns a function with the right signature into a weave commands delete handler
type WeaveCommandsDeleteHandlerFunc func(WeaveCommandsDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveCommandsDeleteHandlerFunc) Handle(params WeaveCommandsDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveCommandsDeleteHandler interface for that can handle valid weave commands delete params
type WeaveCommandsDeleteHandler interface {
	Handle(WeaveCommandsDeleteParams, interface{}) middleware.Responder
}

// NewWeaveCommandsDelete creates a new http.Handler for the weave commands delete operation
func NewWeaveCommandsDelete(ctx *middleware.Context, handler WeaveCommandsDeleteHandler) *WeaveCommandsDelete {
	return &WeaveCommandsDelete{Context: ctx, Handler: handler}
}

/*WeaveCommandsDelete swagger:route DELETE /commands/{commandId} commands weaveCommandsDelete

Deletes a command.

*/
type WeaveCommandsDelete struct {
	Context *middleware.Context
	Handler WeaveCommandsDeleteHandler
}

func (o *WeaveCommandsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveCommandsDeleteParams()

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
