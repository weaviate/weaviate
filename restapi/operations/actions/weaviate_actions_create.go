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
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package actions

// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateActionsCreateHandlerFunc turns a function with the right signature into a weaviate actions create handler
type WeaviateActionsCreateHandlerFunc func(WeaviateActionsCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateActionsCreateHandlerFunc) Handle(params WeaviateActionsCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateActionsCreateHandler interface for that can handle valid weaviate actions create params
type WeaviateActionsCreateHandler interface {
	Handle(WeaviateActionsCreateParams, interface{}) middleware.Responder
}

// NewWeaviateActionsCreate creates a new http.Handler for the weaviate actions create operation
func NewWeaviateActionsCreate(ctx *middleware.Context, handler WeaviateActionsCreateHandler) *WeaviateActionsCreate {
	return &WeaviateActionsCreate{Context: ctx, Handler: handler}
}

/*WeaviateActionsCreate swagger:route POST /actions actions weaviateActionsCreate

Create actions between two things (object and subject).

Create action.

*/
type WeaviateActionsCreate struct {
	Context *middleware.Context
	Handler WeaviateActionsCreateHandler
}

func (o *WeaviateActionsCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateActionsCreateParams()

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
