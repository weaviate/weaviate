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

// WeaviateActionsValidateHandlerFunc turns a function with the right signature into a weaviate actions validate handler
type WeaviateActionsValidateHandlerFunc func(WeaviateActionsValidateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateActionsValidateHandlerFunc) Handle(params WeaviateActionsValidateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateActionsValidateHandler interface for that can handle valid weaviate actions validate params
type WeaviateActionsValidateHandler interface {
	Handle(WeaviateActionsValidateParams, interface{}) middleware.Responder
}

// NewWeaviateActionsValidate creates a new http.Handler for the weaviate actions validate operation
func NewWeaviateActionsValidate(ctx *middleware.Context, handler WeaviateActionsValidateHandler) *WeaviateActionsValidate {
	return &WeaviateActionsValidate{Context: ctx, Handler: handler}
}

/*WeaviateActionsValidate swagger:route POST /actions/validate actions weaviateActionsValidate

Validate an action based on a schema.

Validate an action object. It has to be based on a schema, which is related to the given Thing to be accepted by this validation.

*/
type WeaviateActionsValidate struct {
	Context *middleware.Context
	Handler WeaviateActionsValidateHandler
}

func (o *WeaviateActionsValidate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateActionsValidateParams()

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
