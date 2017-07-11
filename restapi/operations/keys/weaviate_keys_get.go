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

// WeaviateKeysGetHandlerFunc turns a function with the right signature into a weaviate keys get handler
type WeaviateKeysGetHandlerFunc func(WeaviateKeysGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateKeysGetHandlerFunc) Handle(params WeaviateKeysGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateKeysGetHandler interface for that can handle valid weaviate keys get params
type WeaviateKeysGetHandler interface {
	Handle(WeaviateKeysGetParams, interface{}) middleware.Responder
}

// NewWeaviateKeysGet creates a new http.Handler for the weaviate keys get operation
func NewWeaviateKeysGet(ctx *middleware.Context, handler WeaviateKeysGetHandler) *WeaviateKeysGet {
	return &WeaviateKeysGet{Context: ctx, Handler: handler}
}

/*WeaviateKeysGet swagger:route GET /keys/{keyId} keys weaviateKeysGet

Get a key based on its uuid related to this key.

Get a key.

*/
type WeaviateKeysGet struct {
	Context *middleware.Context
	Handler WeaviateKeysGetHandler
}

func (o *WeaviateKeysGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateKeysGetParams()

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
