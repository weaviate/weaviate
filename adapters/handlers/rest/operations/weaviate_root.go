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

package operations

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

	"github.com/weaviate/weaviate/entities/models"
)

// WeaviateRootHandlerFunc turns a function with the right signature into a weaviate root handler
type WeaviateRootHandlerFunc func(WeaviateRootParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateRootHandlerFunc) Handle(params WeaviateRootParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// WeaviateRootHandler interface for that can handle valid weaviate root params
type WeaviateRootHandler interface {
	Handle(WeaviateRootParams, *models.Principal) middleware.Responder
}

// NewWeaviateRoot creates a new http.Handler for the weaviate root operation
func NewWeaviateRoot(ctx *middleware.Context, handler WeaviateRootHandler) *WeaviateRoot {
	return &WeaviateRoot{Context: ctx, Handler: handler}
}

/*
	WeaviateRoot swagger:route GET / weaviateRoot

# List available endpoints

Home. Discover the REST API
*/
type WeaviateRoot struct {
	Context *middleware.Context
	Handler WeaviateRootHandler
}

func (o *WeaviateRoot) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewWeaviateRootParams()
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

// WeaviateRootOKBody weaviate root o k body
//
// swagger:model WeaviateRootOKBody
type WeaviateRootOKBody struct {

	// links
	Links []*models.Link `json:"links" yaml:"links"`
}

// Validate validates this weaviate root o k body
func (o *WeaviateRootOKBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateLinks(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateRootOKBody) validateLinks(formats strfmt.Registry) error {
	if swag.IsZero(o.Links) { // not required
		return nil
	}

	for i := 0; i < len(o.Links); i++ {
		if swag.IsZero(o.Links[i]) { // not required
			continue
		}

		if o.Links[i] != nil {
			if err := o.Links[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("weaviateRootOK" + "." + "links" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("weaviateRootOK" + "." + "links" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// ContextValidate validate this weaviate root o k body based on the context it is used
func (o *WeaviateRootOKBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidateLinks(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateRootOKBody) contextValidateLinks(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Links); i++ {

		if o.Links[i] != nil {
			if err := o.Links[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("weaviateRootOK" + "." + "links" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("weaviateRootOK" + "." + "links" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (o *WeaviateRootOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *WeaviateRootOKBody) UnmarshalBinary(b []byte) error {
	var res WeaviateRootOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
