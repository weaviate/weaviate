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
 package adapters


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateAdaptersPatchHandlerFunc turns a function with the right signature into a weaviate adapters patch handler
type WeaviateAdaptersPatchHandlerFunc func(WeaviateAdaptersPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateAdaptersPatchHandlerFunc) Handle(params WeaviateAdaptersPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateAdaptersPatchHandler interface for that can handle valid weaviate adapters patch params
type WeaviateAdaptersPatchHandler interface {
	Handle(WeaviateAdaptersPatchParams, interface{}) middleware.Responder
}

// NewWeaviateAdaptersPatch creates a new http.Handler for the weaviate adapters patch operation
func NewWeaviateAdaptersPatch(ctx *middleware.Context, handler WeaviateAdaptersPatchHandler) *WeaviateAdaptersPatch {
	return &WeaviateAdaptersPatch{Context: ctx, Handler: handler}
}

/*WeaviateAdaptersPatch swagger:route PATCH /adapters/{adapterId} adapters weaviateAdaptersPatch

Updates an adapter. This method supports patch semantics.

*/
type WeaviateAdaptersPatch struct {
	Context *middleware.Context
	Handler WeaviateAdaptersPatchHandler
}

func (o *WeaviateAdaptersPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateAdaptersPatchParams()

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
