/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package models

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// ObjectSubject returns a ref to the object and the subject
// swagger:model ObjectSubject

type ObjectSubject struct {

	// object
	Object *SingleRef `json:"object,omitempty"`

	// subject
	Subject *SingleRef `json:"subject,omitempty"`
}

/* polymorph ObjectSubject object false */

/* polymorph ObjectSubject subject false */

// Validate validates this object subject
func (m *ObjectSubject) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateObject(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateSubject(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ObjectSubject) validateObject(formats strfmt.Registry) error {

	if swag.IsZero(m.Object) { // not required
		return nil
	}

	if m.Object != nil {

		if err := m.Object.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("object")
			}
			return err
		}
	}

	return nil
}

func (m *ObjectSubject) validateSubject(formats strfmt.Registry) error {

	if swag.IsZero(m.Subject) { // not required
		return nil
	}

	if m.Subject != nil {

		if err := m.Subject.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("subject")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *ObjectSubject) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ObjectSubject) UnmarshalBinary(b []byte) error {
	var res ObjectSubject
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
