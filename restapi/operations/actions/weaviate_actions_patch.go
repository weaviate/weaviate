/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package actions

// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateActionsPatchHandlerFunc turns a function with the right signature into a weaviate actions patch handler
type WeaviateActionsPatchHandlerFunc func(WeaviateActionsPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateActionsPatchHandlerFunc) Handle(params WeaviateActionsPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateActionsPatchHandler interface for that can handle valid weaviate actions patch params
type WeaviateActionsPatchHandler interface {
	Handle(WeaviateActionsPatchParams, interface{}) middleware.Responder
}

// NewWeaviateActionsPatch creates a new http.Handler for the weaviate actions patch operation
func NewWeaviateActionsPatch(ctx *middleware.Context, handler WeaviateActionsPatchHandler) *WeaviateActionsPatch {
	return &WeaviateActionsPatch{Context: ctx, Handler: handler}
}

/*WeaviateActionsPatch swagger:route PATCH /actions/{actionId} actions weaviateActionsPatch

Update an action based on its uuid (using patch semantics) related to this key.

Updates an action. This method supports patch semantics.

*/
type WeaviateActionsPatch struct {
	Context *middleware.Context
	Handler WeaviateActionsPatchHandler
}

func (o *WeaviateActionsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateActionsPatchParams()

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
