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
  package locations

 
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateLocationsDeleteHandlerFunc turns a function with the right signature into a weaviate locations delete handler
type WeaviateLocationsDeleteHandlerFunc func(WeaviateLocationsDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateLocationsDeleteHandlerFunc) Handle(params WeaviateLocationsDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateLocationsDeleteHandler interface for that can handle valid weaviate locations delete params
type WeaviateLocationsDeleteHandler interface {
	Handle(WeaviateLocationsDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateLocationsDelete creates a new http.Handler for the weaviate locations delete operation
func NewWeaviateLocationsDelete(ctx *middleware.Context, handler WeaviateLocationsDeleteHandler) *WeaviateLocationsDelete {
	return &WeaviateLocationsDelete{Context: ctx, Handler: handler}
}

/*WeaviateLocationsDelete swagger:route DELETE /locations/{locationId} locations weaviateLocationsDelete

Delete a location based on its uuid related to this key.

Deletes an location.

*/
type WeaviateLocationsDelete struct {
	Context *middleware.Context
	Handler WeaviateLocationsDeleteHandler
}

func (o *WeaviateLocationsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateLocationsDeleteParams()

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
