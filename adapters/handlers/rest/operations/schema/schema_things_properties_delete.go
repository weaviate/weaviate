//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/semi-technologies/weaviate/entities/models"
)

// SchemaThingsPropertiesDeleteHandlerFunc turns a function with the right signature into a schema things properties delete handler
type SchemaThingsPropertiesDeleteHandlerFunc func(SchemaThingsPropertiesDeleteParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn SchemaThingsPropertiesDeleteHandlerFunc) Handle(params SchemaThingsPropertiesDeleteParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// SchemaThingsPropertiesDeleteHandler interface for that can handle valid schema things properties delete params
type SchemaThingsPropertiesDeleteHandler interface {
	Handle(SchemaThingsPropertiesDeleteParams, *models.Principal) middleware.Responder
}

// NewSchemaThingsPropertiesDelete creates a new http.Handler for the schema things properties delete operation
func NewSchemaThingsPropertiesDelete(ctx *middleware.Context, handler SchemaThingsPropertiesDeleteHandler) *SchemaThingsPropertiesDelete {
	return &SchemaThingsPropertiesDelete{Context: ctx, Handler: handler}
}

/*SchemaThingsPropertiesDelete swagger:route DELETE /schema/things/{className}/properties/{propertyName} schema schemaThingsPropertiesDelete

Remove a property from a Thing class.

*/
type SchemaThingsPropertiesDelete struct {
	Context *middleware.Context
	Handler SchemaThingsPropertiesDeleteHandler
}

func (o *SchemaThingsPropertiesDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewSchemaThingsPropertiesDeleteParams()

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
