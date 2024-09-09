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

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// BatchDelete batch delete
//
// swagger:model BatchDelete
type BatchDelete struct {

	// Timestamp of deletion in milliseconds since epoch UTC.
	DeletionTimeUnixMilli *int64 `json:"deletionTimeUnixMilli,omitempty"`

	// If true, objects will not be deleted yet, but merely listed. Defaults to false.
	DryRun *bool `json:"dryRun,omitempty"`

	// match
	Match *BatchDeleteMatch `json:"match,omitempty"`

	// Controls the verbosity of the output, possible values are: "minimal", "verbose". Defaults to "minimal".
	Output *string `json:"output,omitempty"`
}

// Validate validates this batch delete
func (m *BatchDelete) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateMatch(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *BatchDelete) validateMatch(formats strfmt.Registry) error {
	if swag.IsZero(m.Match) { // not required
		return nil
	}

	if m.Match != nil {
		if err := m.Match.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("match")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("match")
			}
			return err
		}
	}

	return nil
}

// ContextValidate validate this batch delete based on the context it is used
func (m *BatchDelete) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := m.contextValidateMatch(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *BatchDelete) contextValidateMatch(ctx context.Context, formats strfmt.Registry) error {

	if m.Match != nil {
		if err := m.Match.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("match")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("match")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *BatchDelete) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *BatchDelete) UnmarshalBinary(b []byte) error {
	var res BatchDelete
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// BatchDeleteMatch Outlines how to find the objects to be deleted.
//
// swagger:model BatchDeleteMatch
type BatchDeleteMatch struct {

	// Class (name) which objects will be deleted.
	// Example: City
	Class string `json:"class,omitempty"`

	// Filter to limit the objects to be deleted.
	Where *WhereFilter `json:"where,omitempty"`
}

// Validate validates this batch delete match
func (m *BatchDeleteMatch) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateWhere(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *BatchDeleteMatch) validateWhere(formats strfmt.Registry) error {
	if swag.IsZero(m.Where) { // not required
		return nil
	}

	if m.Where != nil {
		if err := m.Where.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("match" + "." + "where")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("match" + "." + "where")
			}
			return err
		}
	}

	return nil
}

// ContextValidate validate this batch delete match based on the context it is used
func (m *BatchDeleteMatch) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := m.contextValidateWhere(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *BatchDeleteMatch) contextValidateWhere(ctx context.Context, formats strfmt.Registry) error {

	if m.Where != nil {
		if err := m.Where.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("match" + "." + "where")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("match" + "." + "where")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *BatchDeleteMatch) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *BatchDeleteMatch) UnmarshalBinary(b []byte) error {
	var res BatchDeleteMatch
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
