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

// RevokeRoleHandlerFunc turns a function with the right signature into a revoke role handler
type RevokeRoleHandlerFunc func(RevokeRoleParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn RevokeRoleHandlerFunc) Handle(params RevokeRoleParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// RevokeRoleHandler interface for that can handle valid revoke role params
type RevokeRoleHandler interface {
	Handle(RevokeRoleParams, *models.Principal) middleware.Responder
}

// NewRevokeRole creates a new http.Handler for the revoke role operation
func NewRevokeRole(ctx *middleware.Context, handler RevokeRoleHandler) *RevokeRole {
	return &RevokeRole{Context: ctx, Handler: handler}
}

/*
	RevokeRole swagger:route POST /authz/users/{id}/revoke authz revokeRole

Revoke a role from a user
*/
type RevokeRole struct {
	Context *middleware.Context
	Handler RevokeRoleHandler
}

func (o *RevokeRole) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewRevokeRoleParams()
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

// RevokeRoleBody revoke role body
//
// swagger:model RevokeRoleBody
type RevokeRoleBody struct {

	// the roles that revoked from the key or user
	Roles []string `json:"roles" yaml:"roles"`
}

// Validate validates this revoke role body
func (o *RevokeRoleBody) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this revoke role body based on context it is used
func (o *RevokeRoleBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (o *RevokeRoleBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *RevokeRoleBody) UnmarshalBinary(b []byte) error {
	var res RevokeRoleBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
