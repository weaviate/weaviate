/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
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

// WeaviateSchemaActionsPropertiesUpdateHandlerFunc turns a function with the right signature into a weaviate schema actions properties update handler
type WeaviateSchemaActionsPropertiesUpdateHandlerFunc func(WeaviateSchemaActionsPropertiesUpdateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateSchemaActionsPropertiesUpdateHandlerFunc) Handle(params WeaviateSchemaActionsPropertiesUpdateParams) middleware.Responder {
	return fn(params)
}

// WeaviateSchemaActionsPropertiesUpdateHandler interface for that can handle valid weaviate schema actions properties update params
type WeaviateSchemaActionsPropertiesUpdateHandler interface {
	Handle(WeaviateSchemaActionsPropertiesUpdateParams) middleware.Responder
}

// NewWeaviateSchemaActionsPropertiesUpdate creates a new http.Handler for the weaviate schema actions properties update operation
func NewWeaviateSchemaActionsPropertiesUpdate(ctx *middleware.Context, handler WeaviateSchemaActionsPropertiesUpdateHandler) *WeaviateSchemaActionsPropertiesUpdate {
	return &WeaviateSchemaActionsPropertiesUpdate{Context: ctx, Handler: handler}
}

/*WeaviateSchemaActionsPropertiesUpdate swagger:route PUT /schema/actions/{className}/properties/{propertyName} schema weaviateSchemaActionsPropertiesUpdate

Rename, or replace the keywords of the property.

*/
type WeaviateSchemaActionsPropertiesUpdate struct {
	Context *middleware.Context
	Handler WeaviateSchemaActionsPropertiesUpdateHandler
}

func (o *WeaviateSchemaActionsPropertiesUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateSchemaActionsPropertiesUpdateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}

// WeaviateSchemaActionsPropertiesUpdateBody weaviate schema actions properties update body
// swagger:model WeaviateSchemaActionsPropertiesUpdateBody
type WeaviateSchemaActionsPropertiesUpdateBody struct {

	// keywords
	Keywords models.SemanticSchemaKeywords `json:"keywords"`

	// The new name of the property.
	NewName string `json:"newName,omitempty"`
}

// Validate validates this weaviate schema actions properties update body
func (o *WeaviateSchemaActionsPropertiesUpdateBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateKeywords(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *WeaviateSchemaActionsPropertiesUpdateBody) validateKeywords(formats strfmt.Registry) error {

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
func (o *WeaviateSchemaActionsPropertiesUpdateBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *WeaviateSchemaActionsPropertiesUpdateBody) UnmarshalBinary(b []byte) error {
	var res WeaviateSchemaActionsPropertiesUpdateBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
