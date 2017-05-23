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

// WeaviateLocationsPatchHandlerFunc turns a function with the right signature into a weaviate locations patch handler
type WeaviateLocationsPatchHandlerFunc func(WeaviateLocationsPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateLocationsPatchHandlerFunc) Handle(params WeaviateLocationsPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateLocationsPatchHandler interface for that can handle valid weaviate locations patch params
type WeaviateLocationsPatchHandler interface {
	Handle(WeaviateLocationsPatchParams, interface{}) middleware.Responder
}

// NewWeaviateLocationsPatch creates a new http.Handler for the weaviate locations patch operation
func NewWeaviateLocationsPatch(ctx *middleware.Context, handler WeaviateLocationsPatchHandler) *WeaviateLocationsPatch {
	return &WeaviateLocationsPatch{Context: ctx, Handler: handler}
}

/*WeaviateLocationsPatch swagger:route PATCH /locations/{locationId} locations weaviateLocationsPatch

Updates an location. This method supports patch semantics.

*/
type WeaviateLocationsPatch struct {
	Context *middleware.Context
	Handler WeaviateLocationsPatchHandler
}

func (o *WeaviateLocationsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateLocationsPatchParams()

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
