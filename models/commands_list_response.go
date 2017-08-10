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
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// CommandsListResponse List of commands.
// swagger:model CommandsListResponse
type CommandsListResponse struct {

	// The actual list of commands.
	Commands []*CommandGetResponse `json:"commands"`

	// Identifies what kind of resource this is. Value: the fixed string "weaviate#commandsListResponse".
	Kind *string `json:"kind,omitempty"`

	// The total number of commands for the query. The number of items in a response may be smaller due to paging.
	TotalResults int64 `json:"totalResults,omitempty"`
}

// Validate validates this commands list response
func (m *CommandsListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCommands(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *CommandsListResponse) validateCommands(formats strfmt.Registry) error {

	if swag.IsZero(m.Commands) { // not required
		return nil
	}

	for i := 0; i < len(m.Commands); i++ {

		if swag.IsZero(m.Commands[i]) { // not required
			continue
		}

		if m.Commands[i] != nil {

			if err := m.Commands[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("commands" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *CommandsListResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *CommandsListResponse) UnmarshalBinary(b []byte) error {
	var res CommandsListResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
