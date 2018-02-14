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

// WeaviateKeysMeGetRenewHandlerFunc turns a function with the right signature into a weaviate keys me get renew handler
type WeaviateKeysMeGetRenewHandlerFunc func(WeaviateKeysMeGetRenewParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateKeysMeGetRenewHandlerFunc) Handle(params WeaviateKeysMeGetRenewParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateKeysMeGetRenewHandler interface for that can handle valid weaviate keys me get renew params
type WeaviateKeysMeGetRenewHandler interface {
	Handle(WeaviateKeysMeGetRenewParams, interface{}) middleware.Responder
}

// NewWeaviateKeysMeGetRenew creates a new http.Handler for the weaviate keys me get renew operation
func NewWeaviateKeysMeGetRenew(ctx *middleware.Context, handler WeaviateKeysMeGetRenewHandler) *WeaviateKeysMeGetRenew {
	return &WeaviateKeysMeGetRenew{Context: ctx, Handler: handler}
}

/*WeaviateKeysMeGetRenew swagger:route PUT /keys/me/renew keys weaviateKeysMeGetRenew

Renews a key based on the key used to do the request.

Renews the related key.

*/
type WeaviateKeysMeGetRenew struct {
	Context *middleware.Context
	Handler WeaviateKeysMeGetRenewHandler
}

func (o *WeaviateKeysMeGetRenew) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateKeysMeGetRenewParams()

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
