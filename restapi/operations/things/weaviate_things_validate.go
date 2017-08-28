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

// WeaviateThingsValidateHandlerFunc turns a function with the right signature into a weaviate things validate handler
type WeaviateThingsValidateHandlerFunc func(WeaviateThingsValidateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsValidateHandlerFunc) Handle(params WeaviateThingsValidateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsValidateHandler interface for that can handle valid weaviate things validate params
type WeaviateThingsValidateHandler interface {
	Handle(WeaviateThingsValidateParams, interface{}) middleware.Responder
}

// NewWeaviateThingsValidate creates a new http.Handler for the weaviate things validate operation
func NewWeaviateThingsValidate(ctx *middleware.Context, handler WeaviateThingsValidateHandler) *WeaviateThingsValidate {
	return &WeaviateThingsValidate{Context: ctx, Handler: handler}
}

/*WeaviateThingsValidate swagger:route POST /things/validate things weaviateThingsValidate

Validate Things schema.

Validate thing.

*/
type WeaviateThingsValidate struct {
	Context *middleware.Context
	Handler WeaviateThingsValidateHandler
}

func (o *WeaviateThingsValidate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsValidateParams()

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
