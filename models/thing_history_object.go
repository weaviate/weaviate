/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
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

// ThingHistoryObject thing history object
// swagger:model ThingHistoryObject
type ThingHistoryObject struct {
	ThingCreate

	// Timestamp of creation of this Thing history in milliseconds since epoch UTC.
	CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *ThingHistoryObject) UnmarshalJSON(raw []byte) error {
	// AO0
	var aO0 ThingCreate
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.ThingCreate = aO0

	// AO1
	var dataAO1 struct {
		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`
	}
	if err := swag.ReadJSON(raw, &dataAO1); err != nil {
		return err
	}

	m.CreationTimeUnix = dataAO1.CreationTimeUnix

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m ThingHistoryObject) MarshalJSON() ([]byte, error) {
	_parts := make([][]byte, 0, 2)

	aO0, err := swag.WriteJSON(m.ThingCreate)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var dataAO1 struct {
		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`
	}

	dataAO1.CreationTimeUnix = m.CreationTimeUnix

	jsonDataAO1, errAO1 := swag.WriteJSON(dataAO1)
	if errAO1 != nil {
		return nil, errAO1
	}
	_parts = append(_parts, jsonDataAO1)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this thing history object
func (m *ThingHistoryObject) Validate(formats strfmt.Registry) error {
	var res []error

	// validation for a type composition with ThingCreate
	if err := m.ThingCreate.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// MarshalBinary interface implementation
func (m *ThingHistoryObject) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ThingHistoryObject) UnmarshalBinary(b []byte) error {
	var res ThingHistoryObject
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
