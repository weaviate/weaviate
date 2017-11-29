/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package actions

// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateActionsDeleteHandlerFunc turns a function with the right signature into a weaviate actions delete handler
type WeaviateActionsDeleteHandlerFunc func(WeaviateActionsDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateActionsDeleteHandlerFunc) Handle(params WeaviateActionsDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateActionsDeleteHandler interface for that can handle valid weaviate actions delete params
type WeaviateActionsDeleteHandler interface {
	Handle(WeaviateActionsDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateActionsDelete creates a new http.Handler for the weaviate actions delete operation
func NewWeaviateActionsDelete(ctx *middleware.Context, handler WeaviateActionsDeleteHandler) *WeaviateActionsDelete {
	return &WeaviateActionsDelete{Context: ctx, Handler: handler}
}

/*WeaviateActionsDelete swagger:route DELETE /actions/{actionId} actions weaviateActionsDelete

Delete an action based on its uuid related to this key.

Deletes an action from the system.

*/
type WeaviateActionsDelete struct {
	Context *middleware.Context
	Handler WeaviateActionsDeleteHandler
}

func (o *WeaviateActionsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateActionsDeleteParams()

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
