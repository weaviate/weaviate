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

// WeaviateGroupsEventsGetHandlerFunc turns a function with the right signature into a weaviate groups events get handler
type WeaviateGroupsEventsGetHandlerFunc func(WeaviateGroupsEventsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateGroupsEventsGetHandlerFunc) Handle(params WeaviateGroupsEventsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateGroupsEventsGetHandler interface for that can handle valid weaviate groups events get params
type WeaviateGroupsEventsGetHandler interface {
	Handle(WeaviateGroupsEventsGetParams, interface{}) middleware.Responder
}

// NewWeaviateGroupsEventsGet creates a new http.Handler for the weaviate groups events get operation
func NewWeaviateGroupsEventsGet(ctx *middleware.Context, handler WeaviateGroupsEventsGetHandler) *WeaviateGroupsEventsGet {
	return &WeaviateGroupsEventsGet{Context: ctx, Handler: handler}
}

/*WeaviateGroupsEventsGet swagger:route GET /groups/{groupId}/events/{eventId} events weaviateGroupsEventsGet

Get a specific event based on its uuid and a group uuid related to this key.

Lists events.

*/
type WeaviateGroupsEventsGet struct {
	Context *middleware.Context
	Handler WeaviateGroupsEventsGetHandler
}

func (o *WeaviateGroupsEventsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateGroupsEventsGetParams()

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
