/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	errors "github.com/go-openapi/errors"
	middleware "github.com/go-openapi/runtime/middleware"
	strfmt "github.com/go-openapi/strfmt"
	swag "github.com/go-openapi/swag"

	models "github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateSchemaThingsPropertiesUpdateHandlerFunc turns a function with the right signature into a weaviate schema things properties update handler
type WeaviateSchemaThingsPropertiesUpdateHandlerFunc func(WeaviateSchemaThingsPropertiesUpdateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateSchemaThingsPropertiesUpdateHandlerFunc) Handle(params WeaviateSchemaThingsPropertiesUpdateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateSchemaThingsPropertiesUpdateHandler interface for that can handle valid weaviate schema things properties update params
type WeaviateSchemaThingsPropertiesUpdateHandler interface {
	Handle(WeaviateSchemaThingsPropertiesUpdateParams, interface{}) middleware.Responder
}

// NewWeaviateSchemaThingsPropertiesUpdate creates a new http.Handler for the weaviate schema things properties update operation
func NewWeaviateSchemaThingsPropertiesUpdate(ctx *middleware.Context, handler WeaviateSchemaThingsPropertiesUpdateHandler) *WeaviateSchemaThingsPropertiesUpdate {
	return &WeaviateSchemaThingsPropertiesUpdate{Context: ctx, Handler: handler}
}

/*WeaviateSchemaThingsPropertiesUpdate swagger:route PUT /schema/things/{className}/properties/{propertyName} schema weaviateSchemaThingsPropertiesUpdate

Rename, or replace the keywords of the property.

*/
type WeaviateSchemaThingsPropertiesUpdate struct {
	Context *middleware.Context
	Handler WeaviateSchemaThingsPropertiesUpdateHandler
}

func (o *WeaviateSchemaThingsPropertiesUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateSchemaThingsPropertiesUpdateParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
	}
	var principal interface{}
	if uprinc != nil {
		principal = uprinc
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}

// WeaviateSchemaThingsPropertiesUpdateBody weaviate schema things properties update body
// swagger:model WeaviateSchemaThingsPropertiesUpdateBody
type WeaviateSchemaThingsPropertiesUpdateBody struct {

	// keywords
	Keywords models.SemanticSchemaKeywords `json:"keywords,omitempty"`

	// The new name of the property.
	NewName string `json:"newName,omitempty"`
}

// Validate validates this weaviate schema things properties update body
func (o *WeaviateSchemaThingsPropertiesUpdateBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateKeywords(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateSchemaThingsPropertiesUpdateBody) validateKeywords(formats strfmt.Registry) error {

	if swag.IsZero(o.Keywords) { // not required
		return nil
	}

	if err := o.Keywords.Validate(formats); err != nil {
		if ve, ok := err.(*errors.Validation); ok {
			return ve.ValidateName("body" + "." + "keywords")
		}
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (o *WeaviateSchemaThingsPropertiesUpdateBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *WeaviateSchemaThingsPropertiesUpdateBody) UnmarshalBinary(b []byte) error {
	var res WeaviateSchemaThingsPropertiesUpdateBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
