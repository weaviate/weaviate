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

// KeyGetResponse key get response
// swagger:model KeyGetResponse

type KeyGetResponse struct {
	Key

	// Id of the key.
	KeyID strfmt.UUID `json:"keyId,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *KeyGetResponse) UnmarshalJSON(raw []byte) error {

	var aO0 Key
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.Key = aO0

	var data struct {
		KeyID strfmt.UUID `json:"keyId,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.KeyID = data.KeyID

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m KeyGetResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.Key)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		KeyID strfmt.UUID `json:"keyId,omitempty"`
	}

	data.KeyID = m.KeyID

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this key get response
func (m *KeyGetResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.Key.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// MarshalBinary interface implementation
func (m *KeyGetResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *KeyGetResponse) UnmarshalBinary(b []byte) error {
	var res KeyGetResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
