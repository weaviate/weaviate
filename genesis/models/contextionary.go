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

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// Contextionary contextionary
// swagger:model Contextionary
type Contextionary struct {

	// hash
	Hash *ContextionaryHash `json:"hash,omitempty"`

	// URL where the contextionary can be found
	URL string `json:"url,omitempty"`
}

// Validate validates this contextionary
func (m *Contextionary) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateHash(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Contextionary) validateHash(formats strfmt.Registry) error {

	if swag.IsZero(m.Hash) { // not required
		return nil
	}

	if m.Hash != nil {
		if err := m.Hash.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("hash")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *Contextionary) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Contextionary) UnmarshalBinary(b []byte) error {
	var res Contextionary
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// ContextionaryHash contextionary hash
// swagger:model ContextionaryHash
type ContextionaryHash struct {

	// Hash algorithm
	Algorithm string `json:"algorithm,omitempty"`

	// The actual hash
	Value string `json:"value,omitempty"`
}

// Validate validates this contextionary hash
func (m *ContextionaryHash) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *ContextionaryHash) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ContextionaryHash) UnmarshalBinary(b []byte) error {
	var res ContextionaryHash
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
