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
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"

	"github.com/weaviate/weaviate/entities/models"
)

// AddPermissionsHandlerFunc turns a function with the right signature into a add permissions handler
type AddPermissionsHandlerFunc func(AddPermissionsParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn AddPermissionsHandlerFunc) Handle(params AddPermissionsParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// AddPermissionsHandler interface for that can handle valid add permissions params
type AddPermissionsHandler interface {
	Handle(AddPermissionsParams, *models.Principal) middleware.Responder
}

// NewAddPermissions creates a new http.Handler for the add permissions operation
func NewAddPermissions(ctx *middleware.Context, handler AddPermissionsHandler) *AddPermissions {
	return &AddPermissions{Context: ctx, Handler: handler}
}

/*
	AddPermissions swagger:route POST /authz/roles/{id}/add-permissions authz addPermissions

Add permission to a given role.
*/
type AddPermissions struct {
	Context *middleware.Context
	Handler AddPermissionsHandler
}

func (o *AddPermissions) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewAddPermissionsParams()
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

// AddPermissionsBody add permissions body
//
// swagger:model AddPermissionsBody
type AddPermissionsBody struct {

	// permissions to be added to the role
	// Required: true
	Permissions []*models.Permission `json:"permissions" yaml:"permissions"`
}

// Validate validates this add permissions body
func (o *AddPermissionsBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validatePermissions(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *AddPermissionsBody) validatePermissions(formats strfmt.Registry) error {

	if err := validate.Required("body"+"."+"permissions", "body", o.Permissions); err != nil {
		return err
	}

	for i := 0; i < len(o.Permissions); i++ {
		if swag.IsZero(o.Permissions[i]) { // not required
			continue
		}

		if o.Permissions[i] != nil {
			if err := o.Permissions[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// ContextValidate validate this add permissions body based on the context it is used
func (o *AddPermissionsBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidatePermissions(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *AddPermissionsBody) contextValidatePermissions(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Permissions); i++ {

		if o.Permissions[i] != nil {
			if err := o.Permissions[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (o *AddPermissionsBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *AddPermissionsBody) UnmarshalBinary(b []byte) error {
	var res AddPermissionsBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
