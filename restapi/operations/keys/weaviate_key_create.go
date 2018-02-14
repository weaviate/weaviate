/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package keys

// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateKeyCreateHandlerFunc turns a function with the right signature into a weaviate key create handler
type WeaviateKeyCreateHandlerFunc func(WeaviateKeyCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateKeyCreateHandlerFunc) Handle(params WeaviateKeyCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateKeyCreateHandler interface for that can handle valid weaviate key create params
type WeaviateKeyCreateHandler interface {
	Handle(WeaviateKeyCreateParams, interface{}) middleware.Responder
}

// NewWeaviateKeyCreate creates a new http.Handler for the weaviate key create operation
func NewWeaviateKeyCreate(ctx *middleware.Context, handler WeaviateKeyCreateHandler) *WeaviateKeyCreate {
	return &WeaviateKeyCreate{Context: ctx, Handler: handler}
}

/*WeaviateKeyCreate swagger:route POST /keys keys weaviateKeyCreate

Create a new key related to this key.

Creates a new key.

*/
type WeaviateKeyCreate struct {
	Context *middleware.Context
	Handler WeaviateKeyCreateHandler
}

func (o *WeaviateKeyCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateKeyCreateParams()

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
