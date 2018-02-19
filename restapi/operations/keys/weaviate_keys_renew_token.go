/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 - 2018 Weaviate. All rights reserved.
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

// WeaviateKeysRenewTokenHandlerFunc turns a function with the right signature into a weaviate keys renew token handler
type WeaviateKeysRenewTokenHandlerFunc func(WeaviateKeysRenewTokenParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateKeysRenewTokenHandlerFunc) Handle(params WeaviateKeysRenewTokenParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateKeysRenewTokenHandler interface for that can handle valid weaviate keys renew token params
type WeaviateKeysRenewTokenHandler interface {
	Handle(WeaviateKeysRenewTokenParams, interface{}) middleware.Responder
}

// NewWeaviateKeysRenewToken creates a new http.Handler for the weaviate keys renew token operation
func NewWeaviateKeysRenewToken(ctx *middleware.Context, handler WeaviateKeysRenewTokenHandler) *WeaviateKeysRenewToken {
	return &WeaviateKeysRenewToken{Context: ctx, Handler: handler}
}

/*WeaviateKeysRenewToken swagger:route PUT /keys/{keyId}/renew-token keys weaviateKeysRenewToken

Renews a key based on the key given in the query string.

Renews the related key.

*/
type WeaviateKeysRenewToken struct {
	Context *middleware.Context
	Handler WeaviateKeysRenewTokenHandler
}

func (o *WeaviateKeysRenewToken) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateKeysRenewTokenParams()

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
