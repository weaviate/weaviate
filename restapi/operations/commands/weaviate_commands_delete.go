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

// WeaviateCommandsDeleteHandlerFunc turns a function with the right signature into a weaviate commands delete handler
type WeaviateCommandsDeleteHandlerFunc func(WeaviateCommandsDeleteParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateCommandsDeleteHandlerFunc) Handle(params WeaviateCommandsDeleteParams) middleware.Responder {
	return fn(params)
}

// WeaviateCommandsDeleteHandler interface for that can handle valid weaviate commands delete params
type WeaviateCommandsDeleteHandler interface {
	Handle(WeaviateCommandsDeleteParams) middleware.Responder
}

// NewWeaviateCommandsDelete creates a new http.Handler for the weaviate commands delete operation
func NewWeaviateCommandsDelete(ctx *middleware.Context, handler WeaviateCommandsDeleteHandler) *WeaviateCommandsDelete {
	return &WeaviateCommandsDelete{Context: ctx, Handler: handler}
}

/*WeaviateCommandsDelete swagger:route DELETE /commands/{commandId} commands weaviateCommandsDelete

Deletes a command.

*/
type WeaviateCommandsDelete struct {
	Context *middleware.Context
	Handler WeaviateCommandsDeleteHandler
}

func (o *WeaviateCommandsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateCommandsDeleteParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
