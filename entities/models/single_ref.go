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

// SingleRef Either set beacon (direct reference) or set class and schema (concept reference)
//
// swagger:model SingleRef
type SingleRef struct {

	// If using a direct reference, specify the URI to point to the cross-ref here. Should be in the form of weaviate://localhost/<uuid> for the example of a local cross-ref to an object
	// Format: uri
	Beacon strfmt.URI `json:"beacon,omitempty"`

	// If using a concept reference (rather than a direct reference), specify the desired class name here
	// Format: uri
	Class strfmt.URI `json:"class,omitempty"`

	// Additional Meta information about classifications if the item was part of one
	Classification *ReferenceMetaClassification `json:"classification,omitempty"`

	// If using a direct reference, this read-only fields provides a link to the referenced resource. If 'origin' is globally configured, an absolute URI is shown - a relative URI otherwise.
	// Format: uri
	Href strfmt.URI `json:"href,omitempty"`

	// If using a concept reference (rather than a direct reference), specify the desired properties here
	Schema PropertySchema `json:"schema,omitempty"`
}

// Validate validates this single ref
func (m *SingleRef) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateBeacon(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateClass(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateClassification(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateHref(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *SingleRef) validateBeacon(formats strfmt.Registry) error {
	if swag.IsZero(m.Beacon) { // not required
		return nil
	}

	if err := validate.FormatOf("beacon", "body", "uri", m.Beacon.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *SingleRef) validateClass(formats strfmt.Registry) error {
	if swag.IsZero(m.Class) { // not required
		return nil
	}

	if err := validate.FormatOf("class", "body", "uri", m.Class.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *SingleRef) validateClassification(formats strfmt.Registry) error {
	if swag.IsZero(m.Classification) { // not required
		return nil
	}

	if m.Classification != nil {
		if err := m.Classification.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("classification")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("classification")
			}
			return err
		}
	}

	return nil
}

func (m *SingleRef) validateHref(formats strfmt.Registry) error {
	if swag.IsZero(m.Href) { // not required
		return nil
	}

	if err := validate.FormatOf("href", "body", "uri", m.Href.String(), formats); err != nil {
		return err
	}

	return nil
}

// ContextValidate validate this single ref based on the context it is used
func (m *SingleRef) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := m.contextValidateClassification(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *SingleRef) contextValidateClassification(ctx context.Context, formats strfmt.Registry) error {

	if m.Classification != nil {
		if err := m.Classification.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("classification")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("classification")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *SingleRef) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *SingleRef) UnmarshalBinary(b []byte) error {
	var res SingleRef
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
