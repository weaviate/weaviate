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

package things

// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateThingsActionsListHandlerFunc turns a function with the right signature into a weaviate things actions list handler
type WeaviateThingsActionsListHandlerFunc func(WeaviateThingsActionsListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsActionsListHandlerFunc) Handle(params WeaviateThingsActionsListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsActionsListHandler interface for that can handle valid weaviate things actions list params
type WeaviateThingsActionsListHandler interface {
	Handle(WeaviateThingsActionsListParams, interface{}) middleware.Responder
}

// NewWeaviateThingsActionsList creates a new http.Handler for the weaviate things actions list operation
func NewWeaviateThingsActionsList(ctx *middleware.Context, handler WeaviateThingsActionsListHandler) *WeaviateThingsActionsList {
	return &WeaviateThingsActionsList{Context: ctx, Handler: handler}
}

/*WeaviateThingsActionsList swagger:route GET /things/{thingId}/actions things weaviateThingsActionsList

Get a thing based on its uuid related to this thing. Also available as MQTT.

Returns the actions of a thing in a list.

*/
type WeaviateThingsActionsList struct {
	Context *middleware.Context
	Handler WeaviateThingsActionsListHandler
}

func (o *WeaviateThingsActionsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsActionsListParams()

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
