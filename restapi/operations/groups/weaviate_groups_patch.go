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
 package groups


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateGroupsPatchHandlerFunc turns a function with the right signature into a weaviate groups patch handler
type WeaviateGroupsPatchHandlerFunc func(WeaviateGroupsPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateGroupsPatchHandlerFunc) Handle(params WeaviateGroupsPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateGroupsPatchHandler interface for that can handle valid weaviate groups patch params
type WeaviateGroupsPatchHandler interface {
	Handle(WeaviateGroupsPatchParams, interface{}) middleware.Responder
}

// NewWeaviateGroupsPatch creates a new http.Handler for the weaviate groups patch operation
func NewWeaviateGroupsPatch(ctx *middleware.Context, handler WeaviateGroupsPatchHandler) *WeaviateGroupsPatch {
	return &WeaviateGroupsPatch{Context: ctx, Handler: handler}
}

/*WeaviateGroupsPatch swagger:route PATCH /groups/{groupId} groups weaviateGroupsPatch

Updates an group. This method supports patch semantics.

*/
type WeaviateGroupsPatch struct {
	Context *middleware.Context
	Handler WeaviateGroupsPatchHandler
}

func (o *WeaviateGroupsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateGroupsPatchParams()

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
