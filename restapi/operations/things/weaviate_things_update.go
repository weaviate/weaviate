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

// WeaviateThingsUpdateHandlerFunc turns a function with the right signature into a weaviate things update handler
type WeaviateThingsUpdateHandlerFunc func(WeaviateThingsUpdateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsUpdateHandlerFunc) Handle(params WeaviateThingsUpdateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsUpdateHandler interface for that can handle valid weaviate things update params
type WeaviateThingsUpdateHandler interface {
	Handle(WeaviateThingsUpdateParams, interface{}) middleware.Responder
}

// NewWeaviateThingsUpdate creates a new http.Handler for the weaviate things update operation
func NewWeaviateThingsUpdate(ctx *middleware.Context, handler WeaviateThingsUpdateHandler) *WeaviateThingsUpdate {
	return &WeaviateThingsUpdate{Context: ctx, Handler: handler}
}

/*WeaviateThingsUpdate swagger:route PUT /things/{thingId} things weaviateThingsUpdate

Update a thing based on its uuid related to this key.

Updates a thing data.

*/
type WeaviateThingsUpdate struct {
	Context *middleware.Context
	Handler WeaviateThingsUpdateHandler
}

func (o *WeaviateThingsUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsUpdateParams()

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
