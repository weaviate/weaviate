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

// WeaviateKeysDeleteHandlerFunc turns a function with the right signature into a weaviate keys delete handler
type WeaviateKeysDeleteHandlerFunc func(WeaviateKeysDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateKeysDeleteHandlerFunc) Handle(params WeaviateKeysDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateKeysDeleteHandler interface for that can handle valid weaviate keys delete params
type WeaviateKeysDeleteHandler interface {
	Handle(WeaviateKeysDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateKeysDelete creates a new http.Handler for the weaviate keys delete operation
func NewWeaviateKeysDelete(ctx *middleware.Context, handler WeaviateKeysDeleteHandler) *WeaviateKeysDelete {
	return &WeaviateKeysDelete{Context: ctx, Handler: handler}
}

/*WeaviateKeysDelete swagger:route DELETE /keys/{keyId} keys weaviateKeysDelete

Delete a key based on its uuid related to this key.

Deletes a key. Only parent or self is allowed to delete key.

*/
type WeaviateKeysDelete struct {
	Context *middleware.Context
	Handler WeaviateKeysDeleteHandler
}

func (o *WeaviateKeysDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateKeysDeleteParams()

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
