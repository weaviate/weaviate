/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
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

// ThingGetHistoryResponse thing get history response
// swagger:model ThingGetHistoryResponse

type ThingGetHistoryResponse struct {
	ThingHistory

	// thing Id
	ThingID strfmt.UUID `json:"thingId,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *ThingGetHistoryResponse) UnmarshalJSON(raw []byte) error {

	var aO0 ThingHistory
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.ThingHistory = aO0

	var data struct {
		ThingID strfmt.UUID `json:"thingId,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.ThingID = data.ThingID

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m ThingGetHistoryResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.ThingHistory)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		ThingID strfmt.UUID `json:"thingId,omitempty"`
	}

	data.ThingID = m.ThingID

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this thing get history response
func (m *ThingGetHistoryResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.ThingHistory.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// MarshalBinary interface implementation
func (m *ThingGetHistoryResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ThingGetHistoryResponse) UnmarshalBinary(b []byte) error {
	var res ThingGetHistoryResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
