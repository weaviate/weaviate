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
 package keys


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateChildrenGetHandlerFunc turns a function with the right signature into a weaviate children get handler
type WeaviateChildrenGetHandlerFunc func(WeaviateChildrenGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateChildrenGetHandlerFunc) Handle(params WeaviateChildrenGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateChildrenGetHandler interface for that can handle valid weaviate children get params
type WeaviateChildrenGetHandler interface {
	Handle(WeaviateChildrenGetParams, interface{}) middleware.Responder
}

// NewWeaviateChildrenGet creates a new http.Handler for the weaviate children get operation
func NewWeaviateChildrenGet(ctx *middleware.Context, handler WeaviateChildrenGetHandler) *WeaviateChildrenGet {
	return &WeaviateChildrenGet{Context: ctx, Handler: handler}
}

/*WeaviateChildrenGet swagger:route GET /keys/{keyId}/children keys weaviateChildrenGet

Get children or a key, only one step deep. A child can have children of its own.

*/
type WeaviateChildrenGet struct {
	Context *middleware.Context
	Handler WeaviateChildrenGetHandler
}

func (o *WeaviateChildrenGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateChildrenGetParams()

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
