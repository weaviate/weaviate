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

// WeaviateCommandsGetQueueHandlerFunc turns a function with the right signature into a weaviate commands get queue handler
type WeaviateCommandsGetQueueHandlerFunc func(WeaviateCommandsGetQueueParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateCommandsGetQueueHandlerFunc) Handle(params WeaviateCommandsGetQueueParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateCommandsGetQueueHandler interface for that can handle valid weaviate commands get queue params
type WeaviateCommandsGetQueueHandler interface {
	Handle(WeaviateCommandsGetQueueParams, interface{}) middleware.Responder
}

// NewWeaviateCommandsGetQueue creates a new http.Handler for the weaviate commands get queue operation
func NewWeaviateCommandsGetQueue(ctx *middleware.Context, handler WeaviateCommandsGetQueueHandler) *WeaviateCommandsGetQueue {
	return &WeaviateCommandsGetQueue{Context: ctx, Handler: handler}
}

/*WeaviateCommandsGetQueue swagger:route GET /commands/queue commands weaviateCommandsGetQueue

Returns all queued commands that thing is supposed to execute. This method may be used only by things.

*/
type WeaviateCommandsGetQueue struct {
	Context *middleware.Context
	Handler WeaviateCommandsGetQueueHandler
}

func (o *WeaviateCommandsGetQueue) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateCommandsGetQueueParams()

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
