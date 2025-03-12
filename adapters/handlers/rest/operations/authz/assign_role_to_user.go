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

	"github.com/weaviate/weaviate/entities/models"
)

// AssignRoleToUserHandlerFunc turns a function with the right signature into a assign role to user handler
type AssignRoleToUserHandlerFunc func(AssignRoleToUserParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn AssignRoleToUserHandlerFunc) Handle(params AssignRoleToUserParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// AssignRoleToUserHandler interface for that can handle valid assign role to user params
type AssignRoleToUserHandler interface {
	Handle(AssignRoleToUserParams, *models.Principal) middleware.Responder
}

// NewAssignRoleToUser creates a new http.Handler for the assign role to user operation
func NewAssignRoleToUser(ctx *middleware.Context, handler AssignRoleToUserHandler) *AssignRoleToUser {
	return &AssignRoleToUser{Context: ctx, Handler: handler}
}

/*
	AssignRoleToUser swagger:route POST /authz/users/{id}/assign authz assignRoleToUser

Assign a role to a user
*/
type AssignRoleToUser struct {
	Context *middleware.Context
	Handler AssignRoleToUserHandler
}

func (o *AssignRoleToUser) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewAssignRoleToUserParams()
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

// AssignRoleToUserBody assign role to user body
//
// swagger:model AssignRoleToUserBody
type AssignRoleToUserBody struct {

	// the roles that assigned to user
	Roles []string `json:"roles" yaml:"roles"`
}

// Validate validates this assign role to user body
func (o *AssignRoleToUserBody) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this assign role to user body based on context it is used
func (o *AssignRoleToUserBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (o *AssignRoleToUserBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *AssignRoleToUserBody) UnmarshalBinary(b []byte) error {
	var res AssignRoleToUserBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
