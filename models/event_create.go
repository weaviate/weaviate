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

// EventCreate event create
// swagger:model EventCreate
type EventCreate struct {

	// command
	Command *EventCreateCommand `json:"command,omitempty"`

	// Command id.
	CommandID strfmt.UUID `json:"commandId,omitempty"`
}

// Validate validates this event create
func (m *EventCreate) Validate(formats strfmt.Registry) error {
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

func (m *EventCreate) validateCommand(formats strfmt.Registry) error {

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

// EventCreateCommand event create command
// swagger:model EventCreateCommand
type EventCreateCommand struct {

	// command parameters
	CommandParameters *CommandParameters `json:"commandParameters,omitempty"`
}

// Validate validates this event create command
func (m *EventCreateCommand) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
