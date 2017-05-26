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

// CommandGetResponse command get response
// swagger:model CommandGetResponse
type CommandGetResponse struct {
	Command

	// id
	ID strfmt.UUID `json:"id,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *CommandGetResponse) UnmarshalJSON(raw []byte) error {

	var aO0 Command
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.Command = aO0

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.ID = data.ID

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m CommandGetResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.Command)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`
	}

	data.ID = m.ID

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this command get response
func (m *CommandGetResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.Command.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
