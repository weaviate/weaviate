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

// WeaviateKeysMeDeleteHandlerFunc turns a function with the right signature into a weaviate keys me delete handler
type WeaviateKeysMeDeleteHandlerFunc func(WeaviateKeysMeDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateKeysMeDeleteHandlerFunc) Handle(params WeaviateKeysMeDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateKeysMeDeleteHandler interface for that can handle valid weaviate keys me delete params
type WeaviateKeysMeDeleteHandler interface {
	Handle(WeaviateKeysMeDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateKeysMeDelete creates a new http.Handler for the weaviate keys me delete operation
func NewWeaviateKeysMeDelete(ctx *middleware.Context, handler WeaviateKeysMeDeleteHandler) *WeaviateKeysMeDelete {
	return &WeaviateKeysMeDelete{Context: ctx, Handler: handler}
}

/*WeaviateKeysMeDelete swagger:route DELETE /keys/me keys weaviateKeysMeDelete

Delete a key based on the key used to do the request.

Deletes key used to do the request.

*/
type WeaviateKeysMeDelete struct {
	Context *middleware.Context
	Handler WeaviateKeysMeDeleteHandler
}

func (o *WeaviateKeysMeDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateKeysMeDeleteParams()

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
