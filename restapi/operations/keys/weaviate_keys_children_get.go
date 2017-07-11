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

// WeaviateKeysChildrenGetHandlerFunc turns a function with the right signature into a weaviate keys children get handler
type WeaviateKeysChildrenGetHandlerFunc func(WeaviateKeysChildrenGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateKeysChildrenGetHandlerFunc) Handle(params WeaviateKeysChildrenGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateKeysChildrenGetHandler interface for that can handle valid weaviate keys children get params
type WeaviateKeysChildrenGetHandler interface {
	Handle(WeaviateKeysChildrenGetParams, interface{}) middleware.Responder
}

// NewWeaviateKeysChildrenGet creates a new http.Handler for the weaviate keys children get operation
func NewWeaviateKeysChildrenGet(ctx *middleware.Context, handler WeaviateKeysChildrenGetHandler) *WeaviateKeysChildrenGet {
	return &WeaviateKeysChildrenGet{Context: ctx, Handler: handler}
}

/*WeaviateKeysChildrenGet swagger:route GET /keys/{keyId}/children keys weaviateKeysChildrenGet

Get an object of this keys' children related to this key.

Get children of a key, only one step deep. A child can have children of its own.

*/
type WeaviateKeysChildrenGet struct {
	Context *middleware.Context
	Handler WeaviateKeysChildrenGetHandler
}

func (o *WeaviateKeysChildrenGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateKeysChildrenGetParams()

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
