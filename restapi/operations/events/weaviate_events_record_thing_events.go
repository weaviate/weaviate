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

// WeaviateEventsRecordThingEventsHandlerFunc turns a function with the right signature into a weaviate events record thing events handler
type WeaviateEventsRecordThingEventsHandlerFunc func(WeaviateEventsRecordThingEventsParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateEventsRecordThingEventsHandlerFunc) Handle(params WeaviateEventsRecordThingEventsParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateEventsRecordThingEventsHandler interface for that can handle valid weaviate events record thing events params
type WeaviateEventsRecordThingEventsHandler interface {
	Handle(WeaviateEventsRecordThingEventsParams, interface{}) middleware.Responder
}

// NewWeaviateEventsRecordThingEvents creates a new http.Handler for the weaviate events record thing events operation
func NewWeaviateEventsRecordThingEvents(ctx *middleware.Context, handler WeaviateEventsRecordThingEventsHandler) *WeaviateEventsRecordThingEvents {
	return &WeaviateEventsRecordThingEvents{Context: ctx, Handler: handler}
}

/*WeaviateEventsRecordThingEvents swagger:route POST /events/recordThingEvents events weaviateEventsRecordThingEvents

Enables or disables recording of a particular thing's events based on a boolean parameter. Enabled by default.

*/
type WeaviateEventsRecordThingEvents struct {
	Context *middleware.Context
	Handler WeaviateEventsRecordThingEventsHandler
}

func (o *WeaviateEventsRecordThingEvents) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateEventsRecordThingEventsParams()

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
