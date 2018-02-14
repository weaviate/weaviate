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

package things

// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateThingsListHandlerFunc turns a function with the right signature into a weaviate things list handler
type WeaviateThingsListHandlerFunc func(WeaviateThingsListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsListHandlerFunc) Handle(params WeaviateThingsListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsListHandler interface for that can handle valid weaviate things list params
type WeaviateThingsListHandler interface {
	Handle(WeaviateThingsListParams, interface{}) middleware.Responder
}

// NewWeaviateThingsList creates a new http.Handler for the weaviate things list operation
func NewWeaviateThingsList(ctx *middleware.Context, handler WeaviateThingsListHandler) *WeaviateThingsList {
	return &WeaviateThingsList{Context: ctx, Handler: handler}
}

/*WeaviateThingsList swagger:route GET /things things weaviateThingsList

Get a list of things related to this key.

Lists all things in reverse order of creation, owned by the user that belongs to the used token.

*/
type WeaviateThingsList struct {
	Context *middleware.Context
	Handler WeaviateThingsListHandler
}

func (o *WeaviateThingsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsListParams()

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
