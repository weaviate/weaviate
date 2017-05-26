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

	// command
	Command *EventCommand `json:"command,omitempty"`

	// Command id.
	CommandID strfmt.UUID `json:"commandId,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weaviate#event".
	Kind *string `json:"kind,omitempty"`

	// Time the event was generated in milliseconds since epoch UTC.
	TimeMs int64 `json:"timeMs,omitempty"`

	// User that caused the event (if applicable).
	Userkey string `json:"userkey,omitempty"`
}

// Validate validates this event
func (m *Event) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCommand(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Event) validateCommand(formats strfmt.Registry) error {

	if swag.IsZero(m.Command) { // not required
		return nil
	}

	if m.Command != nil {

		if err := m.Command.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("command")
			}
			return err
		}
	}

	return nil
}

// EventCommand event command
// swagger:model EventCommand
type EventCommand struct {

	// command parameters
	CommandParameters *CommandParameters `json:"commandParameters,omitempty"`
}

// Validate validates this event command
func (m *EventCommand) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
