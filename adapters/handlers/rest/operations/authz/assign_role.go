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
	"context"
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/liutizhong/weaviate/entities/models"
)

// AssignRoleHandlerFunc turns a function with the right signature into a assign role handler
type AssignRoleHandlerFunc func(AssignRoleParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn AssignRoleHandlerFunc) Handle(params AssignRoleParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// AssignRoleHandler interface for that can handle valid assign role params
type AssignRoleHandler interface {
	Handle(AssignRoleParams, *models.Principal) middleware.Responder
}

// NewAssignRole creates a new http.Handler for the assign role operation
func NewAssignRole(ctx *middleware.Context, handler AssignRoleHandler) *AssignRole {
	return &AssignRole{Context: ctx, Handler: handler}
}

/*
	AssignRole swagger:route POST /authz/users/{id}/assign authz assignRole

Assign a role to a user
*/
type AssignRole struct {
	Context *middleware.Context
	Handler AssignRoleHandler
}

func (o *AssignRole) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewAssignRoleParams()
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

// AssignRoleBody assign role body
//
// swagger:model AssignRoleBody
type AssignRoleBody struct {

	// the roles that assigned to user
	Roles []string `json:"roles" yaml:"roles"`
}

// Validate validates this assign role body
func (o *AssignRoleBody) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this assign role body based on context it is used
func (o *AssignRoleBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (o *AssignRoleBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *AssignRoleBody) UnmarshalBinary(b []byte) error {
	var res AssignRoleBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
