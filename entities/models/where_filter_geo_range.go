//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// WhereFilterGeoRange filter within a distance of a georange
//
// swagger:model WhereFilterGeoRange
type WhereFilterGeoRange struct {
	// distance
	Distance *WhereFilterGeoRangeDistance `json:"distance,omitempty"`

	// geo coordinates
	GeoCoordinates *GeoCoordinates `json:"geoCoordinates,omitempty"`
}

// Validate validates this where filter geo range
func (m *WhereFilterGeoRange) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateDistance(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateGeoCoordinates(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *WhereFilterGeoRange) validateDistance(formats strfmt.Registry) error {
	if swag.IsZero(m.Distance) { // not required
		return nil
	}

	if m.Distance != nil {
		if err := m.Distance.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("distance")
			}
			return err
		}
	}

	return nil
}

func (m *WhereFilterGeoRange) validateGeoCoordinates(formats strfmt.Registry) error {
	if swag.IsZero(m.GeoCoordinates) { // not required
		return nil
	}

	if m.GeoCoordinates != nil {
		if err := m.GeoCoordinates.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("geoCoordinates")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *WhereFilterGeoRange) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *WhereFilterGeoRange) UnmarshalBinary(b []byte) error {
	var res WhereFilterGeoRange
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// WhereFilterGeoRangeDistance where filter geo range distance
//
// swagger:model WhereFilterGeoRangeDistance
type WhereFilterGeoRangeDistance struct {
	// max
	Max float64 `json:"max,omitempty"`
}

// Validate validates this where filter geo range distance
func (m *WhereFilterGeoRangeDistance) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *WhereFilterGeoRangeDistance) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *WhereFilterGeoRangeDistance) UnmarshalBinary(b []byte) error {
	var res WhereFilterGeoRangeDistance
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
