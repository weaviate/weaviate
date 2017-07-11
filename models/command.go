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

// Command command
// swagger:model Command
type Command struct {

	// User that created the command (by key).
	CreatorKey string `json:"creatorKey,omitempty"`

	// error
	Error *CommandError `json:"error,omitempty"`

	// Timestamp since epoch of command expiration.
	ExpirationTimeMs int64 `json:"expirationTimeMs,omitempty"`

	// Expiration timeout for the command since its creation, 10 seconds min, 30 days max.
	ExpirationTimeoutMs int64 `json:"expirationTimeoutMs,omitempty"`

	// Full command name, including trait.
	Name string `json:"name,omitempty"`

	// parameters
	Parameters *CommandParameters `json:"parameters,omitempty"`

	// results
	Results *CommandResults `json:"results,omitempty"`

	// Pending command state that is not acknowledged by the thing yet.
	UserAction string `json:"userAction,omitempty"`
}

// Validate validates this command
func (m *Command) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateError(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Command) validateError(formats strfmt.Registry) error {

	if swag.IsZero(m.Error) { // not required
		return nil
	}

	if m.Error != nil {

		if err := m.Error.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("error")
			}
			return err
		}
	}

	return nil
}

// CommandError Error descriptor.
// swagger:model CommandError
type CommandError struct {

	// Positional error arguments used for error message formatting.
	Arguments []string `json:"arguments"`

	// Error code.
	Code string `json:"code,omitempty"`

	// User-visible error message populated by the cloud based on command name and error code.
	Message string `json:"message,omitempty"`
}

// Validate validates this command error
func (m *CommandError) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateArguments(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *CommandError) validateArguments(formats strfmt.Registry) error {

	if swag.IsZero(m.Arguments) { // not required
		return nil
	}

	return nil
}
