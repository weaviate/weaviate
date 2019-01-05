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

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateSchemaThingsDeleteHandlerFunc turns a function with the right signature into a weaviate schema things delete handler
type WeaviateSchemaThingsDeleteHandlerFunc func(WeaviateSchemaThingsDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateSchemaThingsDeleteHandlerFunc) Handle(params WeaviateSchemaThingsDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateSchemaThingsDeleteHandler interface for that can handle valid weaviate schema things delete params
type WeaviateSchemaThingsDeleteHandler interface {
	Handle(WeaviateSchemaThingsDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateSchemaThingsDelete creates a new http.Handler for the weaviate schema things delete operation
func NewWeaviateSchemaThingsDelete(ctx *middleware.Context, handler WeaviateSchemaThingsDeleteHandler) *WeaviateSchemaThingsDelete {
	return &WeaviateSchemaThingsDelete{Context: ctx, Handler: handler}
}

/*WeaviateSchemaThingsDelete swagger:route DELETE /schema/things/{className} schema weaviateSchemaThingsDelete

Remove a Thing class (and all data in the instances) from the ontology.

*/
type WeaviateSchemaThingsDelete struct {
	Context *middleware.Context
	Handler WeaviateSchemaThingsDeleteHandler
}

func (o *WeaviateSchemaThingsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateSchemaThingsDeleteParams()

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
