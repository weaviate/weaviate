/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
   

package models


 

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// ActionCreate action create
// swagger:model ActionCreate
type ActionCreate struct {

	// Type of the Action, defined in the schema.
	AtClass string `json:"@class,omitempty"`

	// Available context schema.
	AtContext string `json:"@context,omitempty"`

	// schema
	Schema Schema `json:"schema,omitempty"`

	// things
	Things *ObjectSubject `json:"things,omitempty"`
}

// Validate validates this action create
func (m *ActionCreate) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateThings(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ActionCreate) validateThings(formats strfmt.Registry) error {

	if swag.IsZero(m.Things) { // not required
		return nil
	}

	if m.Things != nil {

		if err := m.Things.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("things")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *ActionCreate) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ActionCreate) UnmarshalBinary(b []byte) error {
	var res ActionCreate
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
