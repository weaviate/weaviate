//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

// GetRoleHandlerFunc turns a function with the right signature into a get role handler
type GetRoleHandlerFunc func(GetRoleParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn GetRoleHandlerFunc) Handle(params GetRoleParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// GetRoleHandler interface for that can handle valid get role params
type GetRoleHandler interface {
	Handle(GetRoleParams, *models.Principal) middleware.Responder
}

// NewGetRole creates a new http.Handler for the get role operation
func NewGetRole(ctx *middleware.Context, handler GetRoleHandler) *GetRole {
	return &GetRole{Context: ctx, Handler: handler}
}

/*
	GetRole swagger:route GET /authz/roles/{id} authz getRole

Get a role
*/
type GetRole struct {
	Context *middleware.Context
	Handler GetRoleHandler
}

func (o *GetRole) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewGetRoleParams()
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
