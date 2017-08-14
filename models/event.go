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

// Event event
// swagger:model Event
type Event struct {
	EventCreate

	// command progress
	CommandProgress CommandProgress `json:"commandProgress,omitempty"`

	// command results
	CommandResults *CommandResults `json:"commandResults,omitempty"`

	// Timestamp of creation of this event in milliseconds since epoch UTC.
	CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

	// Timestamp since epoch of last update made to the command.
	LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`

	// Thing id.
	ThingID strfmt.UUID `json:"thingId,omitempty"`

	// User that caused the event (if applicable).
	UserKey strfmt.UUID `json:"userKey,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *Event) UnmarshalJSON(raw []byte) error {

	var aO0 EventCreate
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.EventCreate = aO0

	var data struct {
		CommandProgress CommandProgress `json:"commandProgress,omitempty"`

		CommandResults *CommandResults `json:"commandResults,omitempty"`

		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

		LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`

		ThingID strfmt.UUID `json:"thingId,omitempty"`

		UserKey strfmt.UUID `json:"userKey,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.CommandProgress = data.CommandProgress

	m.CommandResults = data.CommandResults

	m.CreationTimeUnix = data.CreationTimeUnix

	m.LastUpdateTimeUnix = data.LastUpdateTimeUnix

	m.ThingID = data.ThingID

	m.UserKey = data.UserKey

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m Event) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.EventCreate)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		CommandProgress CommandProgress `json:"commandProgress,omitempty"`

		CommandResults *CommandResults `json:"commandResults,omitempty"`

		CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

		LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`

		ThingID strfmt.UUID `json:"thingId,omitempty"`

		UserKey strfmt.UUID `json:"userKey,omitempty"`
	}

	data.CommandProgress = m.CommandProgress

	data.CommandResults = m.CommandResults

	data.CreationTimeUnix = m.CreationTimeUnix

	data.LastUpdateTimeUnix = m.LastUpdateTimeUnix

	data.ThingID = m.ThingID

	data.UserKey = m.UserKey

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this event
func (m *Event) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.EventCreate.Validate(formats); err != nil {
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

func (m *Event) validateCommandProgress(formats strfmt.Registry) error {

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

// MarshalBinary interface implementation
func (m *Event) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Event) UnmarshalBinary(b []byte) error {
	var res Event
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
