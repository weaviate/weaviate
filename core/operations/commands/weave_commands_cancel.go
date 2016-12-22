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


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveCommandsCancelHandlerFunc turns a function with the right signature into a weave commands cancel handler
type WeaveCommandsCancelHandlerFunc func(WeaveCommandsCancelParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveCommandsCancelHandlerFunc) Handle(params WeaveCommandsCancelParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveCommandsCancelHandler interface for that can handle valid weave commands cancel params
type WeaveCommandsCancelHandler interface {
	Handle(WeaveCommandsCancelParams, interface{}) middleware.Responder
}

// NewWeaveCommandsCancel creates a new http.Handler for the weave commands cancel operation
func NewWeaveCommandsCancel(ctx *middleware.Context, handler WeaveCommandsCancelHandler) *WeaveCommandsCancel {
	return &WeaveCommandsCancel{Context: ctx, Handler: handler}
}

/*WeaveCommandsCancel swagger:route POST /commands/{commandId}/cancel commands weaveCommandsCancel

Cancels a command.

*/
type WeaveCommandsCancel struct {
	Context *middleware.Context
	Handler WeaveCommandsCancelHandler
}

func (o *WeaveCommandsCancel) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveCommandsCancelParams()

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
