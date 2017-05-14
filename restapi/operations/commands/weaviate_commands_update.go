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
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package commands


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateCommandsUpdateHandlerFunc turns a function with the right signature into a weaviate commands update handler
type WeaviateCommandsUpdateHandlerFunc func(WeaviateCommandsUpdateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateCommandsUpdateHandlerFunc) Handle(params WeaviateCommandsUpdateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateCommandsUpdateHandler interface for that can handle valid weaviate commands update params
type WeaviateCommandsUpdateHandler interface {
	Handle(WeaviateCommandsUpdateParams, interface{}) middleware.Responder
}

// NewWeaviateCommandsUpdate creates a new http.Handler for the weaviate commands update operation
func NewWeaviateCommandsUpdate(ctx *middleware.Context, handler WeaviateCommandsUpdateHandler) *WeaviateCommandsUpdate {
	return &WeaviateCommandsUpdate{Context: ctx, Handler: handler}
}

/*WeaviateCommandsUpdate swagger:route PUT /commands/{commandId} commands weaviateCommandsUpdate

Updates a command. This method may be used only by devices.

*/
type WeaviateCommandsUpdate struct {
	Context *middleware.Context
	Handler WeaviateCommandsUpdateHandler
}

func (o *WeaviateCommandsUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateCommandsUpdateParams()

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
