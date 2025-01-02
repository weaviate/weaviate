// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// SchemaObjectsDeleteHandlerFunc turns a function with the right signature into a schema objects delete handler
type SchemaObjectsDeleteHandlerFunc func(SchemaObjectsDeleteParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn SchemaObjectsDeleteHandlerFunc) Handle(params SchemaObjectsDeleteParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// SchemaObjectsDeleteHandler interface for that can handle valid schema objects delete params
type SchemaObjectsDeleteHandler interface {
	Handle(SchemaObjectsDeleteParams, *models.Principal) middleware.Responder
}

// NewSchemaObjectsDelete creates a new http.Handler for the schema objects delete operation
func NewSchemaObjectsDelete(ctx *middleware.Context, handler SchemaObjectsDeleteHandler) *SchemaObjectsDelete {
	return &SchemaObjectsDelete{Context: ctx, Handler: handler}
}

/*
	SchemaObjectsDelete swagger:route DELETE /schema/{className} schema schemaObjectsDelete

Remove an Object class (and all data in the instances) from the schema.

Remove a collection from the schema. This will also delete all the objects in the collection.
*/
type SchemaObjectsDelete struct {
	Context *middleware.Context
	Handler SchemaObjectsDeleteHandler
}

func (o *SchemaObjectsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewSchemaObjectsDeleteParams()
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
