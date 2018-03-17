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
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// ActionGetHistoryResponse action get history response
// swagger:model ActionGetHistoryResponse

type ActionGetHistoryResponse struct {
	ActionHistory

	// action Id
	ActionID strfmt.UUID `json:"actionId,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *ActionGetHistoryResponse) UnmarshalJSON(raw []byte) error {

	var aO0 ActionHistory
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.ActionHistory = aO0

	var data struct {
		ActionID strfmt.UUID `json:"actionId,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.ActionID = data.ActionID

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m ActionGetHistoryResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.ActionHistory)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		ActionID strfmt.UUID `json:"actionId,omitempty"`
	}

	data.ActionID = m.ActionID

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this action get history response
func (m *ActionGetHistoryResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.ActionHistory.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// MarshalBinary interface implementation
func (m *ActionGetHistoryResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ActionGetHistoryResponse) UnmarshalBinary(b []byte) error {
	var res ActionGetHistoryResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
