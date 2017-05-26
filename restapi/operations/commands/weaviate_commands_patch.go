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
 package commands


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateCommandsPatchHandlerFunc turns a function with the right signature into a weaviate commands patch handler
type WeaviateCommandsPatchHandlerFunc func(WeaviateCommandsPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateCommandsPatchHandlerFunc) Handle(params WeaviateCommandsPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateCommandsPatchHandler interface for that can handle valid weaviate commands patch params
type WeaviateCommandsPatchHandler interface {
	Handle(WeaviateCommandsPatchParams, interface{}) middleware.Responder
}

// NewWeaviateCommandsPatch creates a new http.Handler for the weaviate commands patch operation
func NewWeaviateCommandsPatch(ctx *middleware.Context, handler WeaviateCommandsPatchHandler) *WeaviateCommandsPatch {
	return &WeaviateCommandsPatch{Context: ctx, Handler: handler}
}

/*WeaviateCommandsPatch swagger:route PATCH /commands/{commandId} commands weaviateCommandsPatch

Update a command based on its uuid by using patch semantics related to this key.

Updates a command. This method may be used only by things. This method supports patch semantics.

*/
type WeaviateCommandsPatch struct {
	Context *middleware.Context
	Handler WeaviateCommandsPatchHandler
}

func (o *WeaviateCommandsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateCommandsPatchParams()

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
