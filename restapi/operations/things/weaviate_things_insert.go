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
 package things


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateThingsInsertHandlerFunc turns a function with the right signature into a weaviate things insert handler
type WeaviateThingsInsertHandlerFunc func(WeaviateThingsInsertParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsInsertHandlerFunc) Handle(params WeaviateThingsInsertParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsInsertHandler interface for that can handle valid weaviate things insert params
type WeaviateThingsInsertHandler interface {
	Handle(WeaviateThingsInsertParams, interface{}) middleware.Responder
}

// NewWeaviateThingsInsert creates a new http.Handler for the weaviate things insert operation
func NewWeaviateThingsInsert(ctx *middleware.Context, handler WeaviateThingsInsertHandler) *WeaviateThingsInsert {
	return &WeaviateThingsInsert{Context: ctx, Handler: handler}
}

/*WeaviateThingsInsert swagger:route POST /things things weaviateThingsInsert

Create a new thing based on a thing template related to this key.

Registers a new thing. This method may be used only by aggregator things or adapters.

*/
type WeaviateThingsInsert struct {
	Context *middleware.Context
	Handler WeaviateThingsInsertHandler
}

func (o *WeaviateThingsInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateThingsInsertParams()

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
