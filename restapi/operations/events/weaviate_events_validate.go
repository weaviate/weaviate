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
   

package events

 
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateEventsValidateHandlerFunc turns a function with the right signature into a weaviate events validate handler
type WeaviateEventsValidateHandlerFunc func(WeaviateEventsValidateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateEventsValidateHandlerFunc) Handle(params WeaviateEventsValidateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateEventsValidateHandler interface for that can handle valid weaviate events validate params
type WeaviateEventsValidateHandler interface {
	Handle(WeaviateEventsValidateParams, interface{}) middleware.Responder
}

// NewWeaviateEventsValidate creates a new http.Handler for the weaviate events validate operation
func NewWeaviateEventsValidate(ctx *middleware.Context, handler WeaviateEventsValidateHandler) *WeaviateEventsValidate {
	return &WeaviateEventsValidate{Context: ctx, Handler: handler}
}

/*WeaviateEventsValidate swagger:route POST /events/validate events weaviateEventsValidate

Validate a event based on a command.

Validate a event object. It has to be based on a command, which is related to the given Thing(Template) to be accepted by this validation.

*/
type WeaviateEventsValidate struct {
	Context *middleware.Context
	Handler WeaviateEventsValidateHandler
}

func (o *WeaviateEventsValidate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateEventsValidateParams()

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
