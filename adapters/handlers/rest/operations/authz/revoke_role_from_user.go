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

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"

	"github.com/weaviate/weaviate/entities/models"
)

// RevokeRoleFromUserHandlerFunc turns a function with the right signature into a revoke role from user handler
type RevokeRoleFromUserHandlerFunc func(RevokeRoleFromUserParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn RevokeRoleFromUserHandlerFunc) Handle(params RevokeRoleFromUserParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// RevokeRoleFromUserHandler interface for that can handle valid revoke role from user params
type RevokeRoleFromUserHandler interface {
	Handle(RevokeRoleFromUserParams, *models.Principal) middleware.Responder
}

// NewRevokeRoleFromUser creates a new http.Handler for the revoke role from user operation
func NewRevokeRoleFromUser(ctx *middleware.Context, handler RevokeRoleFromUserHandler) *RevokeRoleFromUser {
	return &RevokeRoleFromUser{Context: ctx, Handler: handler}
}

/*
	RevokeRoleFromUser swagger:route POST /authz/users/{id}/revoke authz revokeRoleFromUser

Revoke a role from a user
*/
type RevokeRoleFromUser struct {
	Context *middleware.Context
	Handler RevokeRoleFromUserHandler
}

func (o *RevokeRoleFromUser) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewRevokeRoleFromUserParams()
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

// RevokeRoleFromUserBody revoke role from user body
//
// swagger:model RevokeRoleFromUserBody
type RevokeRoleFromUserBody struct {

	// the roles that revoked from the key or user
	Roles []string `json:"roles" yaml:"roles"`

	// user type
	// Required: true
	UserType *models.UserTypes `json:"user_type" yaml:"user_type"`
}

// Validate validates this revoke role from user body
func (o *RevokeRoleFromUserBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateUserType(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *RevokeRoleFromUserBody) validateUserType(formats strfmt.Registry) error {

	if err := validate.Required("body"+"."+"user_type", "body", o.UserType); err != nil {
		return err
	}

	if err := validate.Required("body"+"."+"user_type", "body", o.UserType); err != nil {
		return err
	}

	if o.UserType != nil {
		if err := o.UserType.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("body" + "." + "user_type")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("body" + "." + "user_type")
			}
			return err
		}
	}

	return nil
}

// ContextValidate validate this revoke role from user body based on the context it is used
func (o *RevokeRoleFromUserBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidateUserType(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *RevokeRoleFromUserBody) contextValidateUserType(ctx context.Context, formats strfmt.Registry) error {

	if o.UserType != nil {
		if err := o.UserType.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("body" + "." + "user_type")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("body" + "." + "user_type")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (o *RevokeRoleFromUserBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *RevokeRoleFromUserBody) UnmarshalBinary(b []byte) error {
	var res RevokeRoleFromUserBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
