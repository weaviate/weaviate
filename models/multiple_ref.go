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
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// MultipleRef multiple ref
// swagger:model MultipleRef
type MultipleRef struct {

	// ...
	Things []*SingleRef `json:"things"`
}

// Validate validates this multiple ref
func (m *MultipleRef) Validate(formats strfmt.Registry) error {
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

func (m *MultipleRef) validateThings(formats strfmt.Registry) error {

	if swag.IsZero(m.Things) { // not required
		return nil
	}

	for i := 0; i < len(m.Things); i++ {

		if swag.IsZero(m.Things[i]) { // not required
			continue
		}

		if m.Things[i] != nil {

			if err := m.Things[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("things" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *MultipleRef) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *MultipleRef) UnmarshalBinary(b []byte) error {
	var res MultipleRef
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
