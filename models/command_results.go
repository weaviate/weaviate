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

// CommandResults Results of the command. Will be published in events.
// swagger:model CommandResults
type CommandResults struct {
	JSONObject
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *CommandResults) UnmarshalJSON(raw []byte) error {

	var aO0 JSONObject
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.JSONObject = aO0

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m CommandResults) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.JSONObject)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this command results
func (m *CommandResults) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.JSONObject.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
