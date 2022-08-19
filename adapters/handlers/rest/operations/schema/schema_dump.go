// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/semi-technologies/weaviate/entities/models"
)

// SchemaDumpHandlerFunc turns a function with the right signature into a schema dump handler
type SchemaDumpHandlerFunc func(SchemaDumpParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn SchemaDumpHandlerFunc) Handle(params SchemaDumpParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// SchemaDumpHandler interface for that can handle valid schema dump params
type SchemaDumpHandler interface {
	Handle(SchemaDumpParams, *models.Principal) middleware.Responder
}

// NewSchemaDump creates a new http.Handler for the schema dump operation
func NewSchemaDump(ctx *middleware.Context, handler SchemaDumpHandler) *SchemaDump {
	return &SchemaDump{Context: ctx, Handler: handler}
}

/*SchemaDump swagger:route GET /schema schema schemaDump

Dump the current the database schema.

*/
type SchemaDump struct {
	Context *middleware.Context
	Handler SchemaDumpHandler
}

func (o *SchemaDump) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewSchemaDumpParams()

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
