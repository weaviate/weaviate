//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// ActionsValidateHandlerFunc turns a function with the right signature into a actions validate handler
type ActionsValidateHandlerFunc func(ActionsValidateParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ActionsValidateHandlerFunc) Handle(params ActionsValidateParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ActionsValidateHandler interface for that can handle valid actions validate params
type ActionsValidateHandler interface {
	Handle(ActionsValidateParams, *models.Principal) middleware.Responder
}

// NewActionsValidate creates a new http.Handler for the actions validate operation
func NewActionsValidate(ctx *middleware.Context, handler ActionsValidateHandler) *ActionsValidate {
	return &ActionsValidate{Context: ctx, Handler: handler}
}

/*ActionsValidate swagger:route POST /actions/validate actions actionsValidate

Validate an Action based on a schema.

Validate an Action's schema and meta-data. It has to be based on a schema, which is related to the given Action to be accepted by this validation.

*/
type ActionsValidate struct {
	Context *middleware.Context
	Handler ActionsValidateHandler
}

func (o *ActionsValidate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewActionsValidateParams()

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
