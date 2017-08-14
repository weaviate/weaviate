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

// This file was generated by the swagger tool.
 

import (
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// Thing thing
// swagger:model Thing
type Thing struct {

	// Available context. For now only schema.org
	AtContext string `json:"@context,omitempty"`

	// Timestamp of creation of this thing in milliseconds since epoch UTC.
	CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

	// Timestamp of the last request from this thing in milliseconds since epoch UTC. Supported only for things with XMPP channel type.
	LastSeenTimeMs int64 `json:"lastSeenTimeMs,omitempty"`

	// Timestamp of the last thing update in milliseconds since epoch UTC.
	LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty"`

	// Timestamp of the last thing usage in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// The id of the actions that this device is able to execute.
	PotentialActionIds []strfmt.UUID `json:"potentialActionIds"`

	// schema
	Schema Schema `json:"schema,omitempty"`
}

// Validate validates this thing
func (m *Thing) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAtContext(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePotentialActionIds(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var thingTypeAtContextPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["http://schema.org"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		thingTypeAtContextPropEnum = append(thingTypeAtContextPropEnum, v)
	}
}

const (
	// ThingAtContextHTTPSchemaOrg captures enum value "http://schema.org"
	ThingAtContextHTTPSchemaOrg string = "http://schema.org"
)

// prop value enum
func (m *Thing) validateAtContextEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, thingTypeAtContextPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *Thing) validateAtContext(formats strfmt.Registry) error {

	if swag.IsZero(m.AtContext) { // not required
		return nil
	}

	// value enum
	if err := m.validateAtContextEnum("@context", "body", m.AtContext); err != nil {
		return err
	}

	return nil
}

func (m *Thing) validatePotentialActionIds(formats strfmt.Registry) error {

	if swag.IsZero(m.PotentialActionIds) { // not required
		return nil
	}

	return nil
}

// MarshalBinary interface implementation
func (m *Thing) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Thing) UnmarshalBinary(b []byte) error {
	var res Thing
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
