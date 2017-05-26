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

// EventGetResponse event get response
// swagger:model EventGetResponse
type EventGetResponse struct {
	Event

	// command progress
	CommandProgress CommandProgress `json:"commandProgress,omitempty"`

	// command results
	CommandResults *CommandResults `json:"commandResults,omitempty"`

	// ID of the event.
	ID strfmt.UUID `json:"id,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *EventGetResponse) UnmarshalJSON(raw []byte) error {

	var aO0 Event
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.Event = aO0

	var data struct {
		CommandProgress CommandProgress `json:"commandProgress,omitempty"`

		CommandResults *CommandResults `json:"commandResults,omitempty"`

		ID strfmt.UUID `json:"id,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.CommandProgress = data.CommandProgress

	m.CommandResults = data.CommandResults

	m.ID = data.ID

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m EventGetResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.Event)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		CommandProgress CommandProgress `json:"commandProgress,omitempty"`

		CommandResults *CommandResults `json:"commandResults,omitempty"`

		ID strfmt.UUID `json:"id,omitempty"`
	}

	data.CommandProgress = m.CommandProgress

	data.CommandResults = m.CommandResults

	data.ID = m.ID

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this event get response
func (m *EventGetResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.Event.Validate(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateCommandProgress(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *EventGetResponse) validateCommandProgress(formats strfmt.Registry) error {

	if swag.IsZero(m.CommandProgress) { // not required
		return nil
	}

	if err := m.CommandProgress.Validate(formats); err != nil {
		if ve, ok := err.(*errors.Validation); ok {
			return ve.ValidateName("commandProgress")
		}
		return err
	}

	return nil
}
