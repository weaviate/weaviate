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

// WeaviateEventsGetHandlerFunc turns a function with the right signature into a weaviate events get handler
type WeaviateEventsGetHandlerFunc func(WeaviateEventsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateEventsGetHandlerFunc) Handle(params WeaviateEventsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateEventsGetHandler interface for that can handle valid weaviate events get params
type WeaviateEventsGetHandler interface {
	Handle(WeaviateEventsGetParams, interface{}) middleware.Responder
}

// NewWeaviateEventsGet creates a new http.Handler for the weaviate events get operation
func NewWeaviateEventsGet(ctx *middleware.Context, handler WeaviateEventsGetHandler) *WeaviateEventsGet {
	return &WeaviateEventsGet{Context: ctx, Handler: handler}
}

/*WeaviateEventsGet swagger:route GET /events/{eventId} events weaviateEventsGet

Returns a particular event data.

*/
type WeaviateEventsGet struct {
	Context *middleware.Context
	Handler WeaviateEventsGetHandler
}

func (o *WeaviateEventsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateEventsGetParams()

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
