//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

// SchemaObjectsUpdateHandlerFunc turns a function with the right signature into a schema objects update handler
type SchemaObjectsUpdateHandlerFunc func(SchemaObjectsUpdateParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn SchemaObjectsUpdateHandlerFunc) Handle(params SchemaObjectsUpdateParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// SchemaObjectsUpdateHandler interface for that can handle valid schema objects update params
type SchemaObjectsUpdateHandler interface {
	Handle(SchemaObjectsUpdateParams, *models.Principal) middleware.Responder
}

// NewSchemaObjectsUpdate creates a new http.Handler for the schema objects update operation
func NewSchemaObjectsUpdate(ctx *middleware.Context, handler SchemaObjectsUpdateHandler) *SchemaObjectsUpdate {
	return &SchemaObjectsUpdate{Context: ctx, Handler: handler}
}

/*
	SchemaObjectsUpdate swagger:route PUT /schema/{className} schema schemaObjectsUpdate

# Update settings of an existing schema class

Add a property to an existing collection.
*/
type SchemaObjectsUpdate struct {
	Context *middleware.Context
	Handler SchemaObjectsUpdateHandler
}

func (o *SchemaObjectsUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewSchemaObjectsUpdateParams()
	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		*r = *aCtx
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
