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

// WeaviateThingsEventsCreateHandlerFunc turns a function with the right signature into a weaviate things events create handler
type WeaviateThingsEventsCreateHandlerFunc func(WeaviateThingsEventsCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsEventsCreateHandlerFunc) Handle(params WeaviateThingsEventsCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsEventsCreateHandler interface for that can handle valid weaviate things events create params
type WeaviateThingsEventsCreateHandler interface {
	Handle(WeaviateThingsEventsCreateParams, interface{}) middleware.Responder
}

// NewWeaviateThingsEventsCreate creates a new http.Handler for the weaviate things events create operation
func NewWeaviateThingsEventsCreate(ctx *middleware.Context, handler WeaviateThingsEventsCreateHandler) *WeaviateThingsEventsCreate {
	return &WeaviateThingsEventsCreate{Context: ctx, Handler: handler}
}

/*WeaviateThingsEventsCreate swagger:route POST /things/{thingId}/events events weaviateThingsEventsCreate

Create events for a thing (also available as MQTT channel) related to this key.

Create event.

*/
type WeaviateThingsEventsCreate struct {
	Context *middleware.Context
	Handler WeaviateThingsEventsCreateHandler
}

func (o *WeaviateThingsEventsCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsEventsCreateParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
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
