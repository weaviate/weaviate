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

// WeaviateLocationsInsertHandlerFunc turns a function with the right signature into a weaviate locations insert handler
type WeaviateLocationsInsertHandlerFunc func(WeaviateLocationsInsertParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateLocationsInsertHandlerFunc) Handle(params WeaviateLocationsInsertParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateLocationsInsertHandler interface for that can handle valid weaviate locations insert params
type WeaviateLocationsInsertHandler interface {
	Handle(WeaviateLocationsInsertParams, interface{}) middleware.Responder
}

// NewWeaviateLocationsInsert creates a new http.Handler for the weaviate locations insert operation
func NewWeaviateLocationsInsert(ctx *middleware.Context, handler WeaviateLocationsInsertHandler) *WeaviateLocationsInsert {
	return &WeaviateLocationsInsert{Context: ctx, Handler: handler}
}

/*WeaviateLocationsInsert swagger:route POST /locations locations weaviateLocationsInsert

Create a new location related to this key.

Inserts location.

*/
type WeaviateLocationsInsert struct {
	Context *middleware.Context
	Handler WeaviateLocationsInsertHandler
}

func (o *WeaviateLocationsInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateLocationsInsertParams()

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
