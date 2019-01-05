/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
// Code generated by go-swagger; DO NOT EDIT.

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateThingsPropertiesDeleteHandlerFunc turns a function with the right signature into a weaviate things properties delete handler
type WeaviateThingsPropertiesDeleteHandlerFunc func(WeaviateThingsPropertiesDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsPropertiesDeleteHandlerFunc) Handle(params WeaviateThingsPropertiesDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsPropertiesDeleteHandler interface for that can handle valid weaviate things properties delete params
type WeaviateThingsPropertiesDeleteHandler interface {
	Handle(WeaviateThingsPropertiesDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateThingsPropertiesDelete creates a new http.Handler for the weaviate things properties delete operation
func NewWeaviateThingsPropertiesDelete(ctx *middleware.Context, handler WeaviateThingsPropertiesDeleteHandler) *WeaviateThingsPropertiesDelete {
	return &WeaviateThingsPropertiesDelete{Context: ctx, Handler: handler}
}

/*WeaviateThingsPropertiesDelete swagger:route DELETE /things/{thingId}/properties/{propertyName} things weaviateThingsPropertiesDelete

Delete the single reference that is given in the body from the list of references that this property has.

Delete the single reference that is given in the body from the list of references that this property has.

*/
type WeaviateThingsPropertiesDelete struct {
	Context *middleware.Context
	Handler WeaviateThingsPropertiesDeleteHandler
}

func (o *WeaviateThingsPropertiesDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsPropertiesDeleteParams()

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
