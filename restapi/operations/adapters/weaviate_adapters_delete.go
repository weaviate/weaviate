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

// WeaviateAdaptersDeleteHandlerFunc turns a function with the right signature into a weaviate adapters delete handler
type WeaviateAdaptersDeleteHandlerFunc func(WeaviateAdaptersDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateAdaptersDeleteHandlerFunc) Handle(params WeaviateAdaptersDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateAdaptersDeleteHandler interface for that can handle valid weaviate adapters delete params
type WeaviateAdaptersDeleteHandler interface {
	Handle(WeaviateAdaptersDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateAdaptersDelete creates a new http.Handler for the weaviate adapters delete operation
func NewWeaviateAdaptersDelete(ctx *middleware.Context, handler WeaviateAdaptersDeleteHandler) *WeaviateAdaptersDelete {
	return &WeaviateAdaptersDelete{Context: ctx, Handler: handler}
}

/*WeaviateAdaptersDelete swagger:route DELETE /adapters/{adapterId} adapters weaviateAdaptersDelete

Deletes an adapter.

*/
type WeaviateAdaptersDelete struct {
	Context *middleware.Context
	Handler WeaviateAdaptersDeleteHandler
}

func (o *WeaviateAdaptersDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateAdaptersDeleteParams()

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
