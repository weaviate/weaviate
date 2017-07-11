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

// EventGetResponse event get response
// swagger:model EventGetResponse
type EventGetResponse struct {
	Event

	// command progress
	CommandProgress CommandProgress `json:"commandProgress,omitempty"`

	// command results
	CommandResults *CommandResults `json:"commandResults,omitempty"`

	// Timestamp of creation of this event in milliseconds since epoch UTC.
	CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

	// ID of the event.
	ID strfmt.UUID `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weaviate#eventGetResponse".
	Kind *string `json:"kind,omitempty"`

	// Timestamp since epoch of last update made to the command.
	LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`
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

		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

		ID strfmt.UUID `json:"id,omitempty"`

		Kind *string `json:"kind,omitempty"`

		LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.CommandProgress = data.CommandProgress

	m.CommandResults = data.CommandResults

	m.CreationTimeUnix = data.CreationTimeUnix

	m.ID = data.ID

	m.Kind = data.Kind

	m.LastUpdateTimeUnix = data.LastUpdateTimeUnix

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

		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

		ID strfmt.UUID `json:"id,omitempty"`

		Kind *string `json:"kind,omitempty"`

		LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`
	}

	data.CommandProgress = m.CommandProgress

	data.CommandResults = m.CommandResults

	data.CreationTimeUnix = m.CreationTimeUnix

	data.ID = m.ID

	data.Kind = m.Kind

	data.LastUpdateTimeUnix = m.LastUpdateTimeUnix

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
