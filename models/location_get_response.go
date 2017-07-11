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

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// LocationGetResponse Location on the world (inspired by Google Maps).
// swagger:model LocationGetResponse
type LocationGetResponse struct {
	Location

	// ID of the location.
	ID strfmt.UUID `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weaviate#locationGetResponse".
	Kind *string `json:"kind,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *LocationGetResponse) UnmarshalJSON(raw []byte) error {

	var aO0 Location
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.Location = aO0

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`

		Kind *string `json:"kind,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.ID = data.ID

	m.Kind = data.Kind

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m LocationGetResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.Location)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`

		Kind *string `json:"kind,omitempty"`
	}

	data.ID = m.ID

	data.Kind = m.Kind

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this location get response
func (m *LocationGetResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.Location.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
