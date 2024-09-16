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

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// AddPolicyHandlerFunc turns a function with the right signature into a add policy handler
type AddPolicyHandlerFunc func(AddPolicyParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn AddPolicyHandlerFunc) Handle(params AddPolicyParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// AddPolicyHandler interface for that can handle valid add policy params
type AddPolicyHandler interface {
	Handle(AddPolicyParams, *models.Principal) middleware.Responder
}

// NewAddPolicy creates a new http.Handler for the add policy operation
func NewAddPolicy(ctx *middleware.Context, handler AddPolicyHandler) *AddPolicy {
	return &AddPolicy{Context: ctx, Handler: handler}
}

/*
	AddPolicy swagger:route POST /authz/policy authz addPolicy

Add a new policy
*/
type AddPolicy struct {
	Context *middleware.Context
	Handler AddPolicyHandler
}

func (o *AddPolicy) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewAddPolicyParams()
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
