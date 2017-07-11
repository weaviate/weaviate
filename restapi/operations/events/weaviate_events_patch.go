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
  package events

 
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateEventsPatchHandlerFunc turns a function with the right signature into a weaviate events patch handler
type WeaviateEventsPatchHandlerFunc func(WeaviateEventsPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateEventsPatchHandlerFunc) Handle(params WeaviateEventsPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateEventsPatchHandler interface for that can handle valid weaviate events patch params
type WeaviateEventsPatchHandler interface {
	Handle(WeaviateEventsPatchParams, interface{}) middleware.Responder
}

// NewWeaviateEventsPatch creates a new http.Handler for the weaviate events patch operation
func NewWeaviateEventsPatch(ctx *middleware.Context, handler WeaviateEventsPatchHandler) *WeaviateEventsPatch {
	return &WeaviateEventsPatch{Context: ctx, Handler: handler}
}

/*WeaviateEventsPatch swagger:route PATCH /events/{eventId} events weaviateEventsPatch

Update an event based on its uuid (using patch semantics) related to this key.

Updates an event. This method supports patch semantics.

*/
type WeaviateEventsPatch struct {
	Context *middleware.Context
	Handler WeaviateEventsPatchHandler
}

func (o *WeaviateEventsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateEventsPatchParams()

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
