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

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// BatchReference batch reference
//
// swagger:model BatchReference
type BatchReference struct {

	// Long-form beacon-style URI to identify the source of the cross-ref including the property name. Should be in the form of weaviate://localhost/<kinds>/<uuid>/<className>/<propertyName>, where <kinds> must be one of 'objects', 'objects' and <className> and <propertyName> must represent the cross-ref property of source class to be used.
	// Example: weaviate://localhost/Zoo/a5d09582-4239-4702-81c9-92a6e0122bb4/hasAnimals
	// Format: uri
	From strfmt.URI `json:"from,omitempty"`

	// Name of the reference tenant.
	Tenant string `json:"tenant,omitempty"`

	// Short-form URI to point to the cross-ref. Should be in the form of weaviate://localhost/<uuid> for the example of a local cross-ref to an object
	// Example: weaviate://localhost/97525810-a9a5-4eb0-858a-71449aeb007f
	// Format: uri
	To strfmt.URI `json:"to,omitempty"`
}

// Validate validates this batch reference
func (m *BatchReference) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateFrom(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateTo(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *BatchReference) validateFrom(formats strfmt.Registry) error {
	if swag.IsZero(m.From) { // not required
		return nil
	}

	if err := validate.FormatOf("from", "body", "uri", m.From.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *BatchReference) validateTo(formats strfmt.Registry) error {
	if swag.IsZero(m.To) { // not required
		return nil
	}

	if err := validate.FormatOf("to", "body", "uri", m.To.String(), formats); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this batch reference based on context it is used
func (m *BatchReference) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *BatchReference) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *BatchReference) UnmarshalBinary(b []byte) error {
	var res BatchReference
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
