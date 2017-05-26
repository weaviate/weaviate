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

// WeaviateThingsEventsListHandlerFunc turns a function with the right signature into a weaviate things events list handler
type WeaviateThingsEventsListHandlerFunc func(WeaviateThingsEventsListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsEventsListHandlerFunc) Handle(params WeaviateThingsEventsListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsEventsListHandler interface for that can handle valid weaviate things events list params
type WeaviateThingsEventsListHandler interface {
	Handle(WeaviateThingsEventsListParams, interface{}) middleware.Responder
}

// NewWeaviateThingsEventsList creates a new http.Handler for the weaviate things events list operation
func NewWeaviateThingsEventsList(ctx *middleware.Context, handler WeaviateThingsEventsListHandler) *WeaviateThingsEventsList {
	return &WeaviateThingsEventsList{Context: ctx, Handler: handler}
}

/*WeaviateThingsEventsList swagger:route GET /things/{thingId}/events events weaviateThingsEventsList

Get a list of events based on a thing's uuid (also available as MQTT channel) related to this key.

Lists events.

*/
type WeaviateThingsEventsList struct {
	Context *middleware.Context
	Handler WeaviateThingsEventsListHandler
}

func (o *WeaviateThingsEventsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateThingsEventsListParams()

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
