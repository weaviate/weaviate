//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// GraphQLError An error response caused by a GraphQL query.
//
// swagger:model GraphQLError
type GraphQLError struct {

	// locations
	Locations []*GraphQLErrorLocationsItems0 `json:"locations"`

	// message
	Message string `json:"message,omitempty"`

	// path
	Path []string `json:"path"`
}

// Validate validates this graph q l error
func (m *GraphQLError) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocations(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *GraphQLError) validateLocations(formats strfmt.Registry) error {

	if swag.IsZero(m.Locations) { // not required
		return nil
	}

	for i := 0; i < len(m.Locations); i++ {
		if swag.IsZero(m.Locations[i]) { // not required
			continue
		}

		if m.Locations[i] != nil {
			if err := m.Locations[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("locations" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *GraphQLError) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *GraphQLError) UnmarshalBinary(b []byte) error {
	var res GraphQLError
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// GraphQLErrorLocationsItems0 graph q l error locations items0
//
// swagger:model GraphQLErrorLocationsItems0
type GraphQLErrorLocationsItems0 struct {

	// column
	Column int64 `json:"column,omitempty"`

	// line
	Line int64 `json:"line,omitempty"`
}

// Validate validates this graph q l error locations items0
func (m *GraphQLErrorLocationsItems0) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *GraphQLErrorLocationsItems0) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *GraphQLErrorLocationsItems0) UnmarshalBinary(b []byte) error {
	var res GraphQLErrorLocationsItems0
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
