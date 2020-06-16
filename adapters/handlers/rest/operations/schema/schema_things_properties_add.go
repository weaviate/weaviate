//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
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

// SchemaThingsPropertiesAddHandlerFunc turns a function with the right signature into a schema things properties add handler
type SchemaThingsPropertiesAddHandlerFunc func(SchemaThingsPropertiesAddParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn SchemaThingsPropertiesAddHandlerFunc) Handle(params SchemaThingsPropertiesAddParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// SchemaThingsPropertiesAddHandler interface for that can handle valid schema things properties add params
type SchemaThingsPropertiesAddHandler interface {
	Handle(SchemaThingsPropertiesAddParams, *models.Principal) middleware.Responder
}

// NewSchemaThingsPropertiesAdd creates a new http.Handler for the schema things properties add operation
func NewSchemaThingsPropertiesAdd(ctx *middleware.Context, handler SchemaThingsPropertiesAddHandler) *SchemaThingsPropertiesAdd {
	return &SchemaThingsPropertiesAdd{Context: ctx, Handler: handler}
}

/*SchemaThingsPropertiesAdd swagger:route POST /schema/things/{className}/properties schema schemaThingsPropertiesAdd

Add a property to a Thing class.

*/
type SchemaThingsPropertiesAdd struct {
	Context *middleware.Context
	Handler SchemaThingsPropertiesAddHandler
}

func (o *SchemaThingsPropertiesAdd) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewSchemaThingsPropertiesAddParams()

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
