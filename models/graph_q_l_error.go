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

package models

import (
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// GraphQLError Error messages responded only if error exists.
// swagger:model GraphQLError

type GraphQLError struct {

	// locations
	Locations []*GraphQLErrorLocationsItems0 `json:"locations"`

	// message
	Message string `json:"message,omitempty"`

	// path
	Path []string `json:"path"`
}

/* polymorph GraphQLError locations false */

/* polymorph GraphQLError message false */

/* polymorph GraphQLError path false */

// Validate validates this graph q l error
func (m *GraphQLError) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocations(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePath(formats); err != nil {
		// prop
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

func (m *GraphQLError) validatePath(formats strfmt.Registry) error {

	if swag.IsZero(m.Path) { // not required
		return nil
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
// swagger:model GraphQLErrorLocationsItems0

type GraphQLErrorLocationsItems0 struct {

	// column
	Column int64 `json:"column,omitempty"`

	// line
	Line int64 `json:"line,omitempty"`
}

/* polymorph GraphQLErrorLocationsItems0 column false */

/* polymorph GraphQLErrorLocationsItems0 line false */

// Validate validates this graph q l error locations items0
func (m *GraphQLErrorLocationsItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
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
