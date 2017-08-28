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

// Action action
// swagger:model Action
type Action struct {
	ActionCreate

	// Timestamp of creation of this action in milliseconds since epoch UTC.
	CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

	// key
	Key *SingleRef `json:"key,omitempty"`

	// Timestamp since epoch of last update made to the action.
	LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *Action) UnmarshalJSON(raw []byte) error {

	var aO0 ActionCreate
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.ActionCreate = aO0

	var data struct {
		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

		Key *SingleRef `json:"key,omitempty"`

		LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.CreationTimeUnix = data.CreationTimeUnix

	m.Key = data.Key

	m.LastUpdateTimeUnix = data.LastUpdateTimeUnix

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m Action) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.ActionCreate)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

		Key *SingleRef `json:"key,omitempty"`

		LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`
	}

	data.CreationTimeUnix = m.CreationTimeUnix

	data.Key = m.Key

	data.LastUpdateTimeUnix = m.LastUpdateTimeUnix

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this action
func (m *Action) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.ActionCreate.Validate(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateKey(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Action) validateKey(formats strfmt.Registry) error {

	if swag.IsZero(m.Key) { // not required
		return nil
	}

	if m.Key != nil {

		if err := m.Key.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("key")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *Action) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Action) UnmarshalBinary(b []byte) error {
	var res Action
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
