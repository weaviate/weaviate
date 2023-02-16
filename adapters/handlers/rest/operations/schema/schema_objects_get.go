//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// SchemaObjectsGetHandlerFunc turns a function with the right signature into a schema objects get handler
type SchemaObjectsGetHandlerFunc func(SchemaObjectsGetParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn SchemaObjectsGetHandlerFunc) Handle(params SchemaObjectsGetParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// SchemaObjectsGetHandler interface for that can handle valid schema objects get params
type SchemaObjectsGetHandler interface {
	Handle(SchemaObjectsGetParams, *models.Principal) middleware.Responder
}

// NewSchemaObjectsGet creates a new http.Handler for the schema objects get operation
func NewSchemaObjectsGet(ctx *middleware.Context, handler SchemaObjectsGetHandler) *SchemaObjectsGet {
	return &SchemaObjectsGet{Context: ctx, Handler: handler}
}

/*SchemaObjectsGet swagger:route GET /schema/{className} schema schemaObjectsGet

Get a single class from the schema

*/
type SchemaObjectsGet struct {
	Context *middleware.Context
	Handler SchemaObjectsGetHandler
}

func (o *SchemaObjectsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewSchemaObjectsGetParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
	}
	var principal *models.Principal
	if uprinc != nil {
		principal = uprinc.(*models.Principal) // this is really a models.Principal, I promise
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
