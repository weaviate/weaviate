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
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// SingleRef single ref
// swagger:model SingleRef
type SingleRef struct {

	// Location of the cross reference.
	NrDollarCref string `json:"$cref,omitempty"`

	// url of location. http://localhost means this database. This option can be used to refer to other databases.
	LocationURL *string `json:"locationUrl,omitempty"`

	// Type should be Thing, Action or Key
	Type string `json:"type,omitempty"`
}

// Validate validates this single ref
func (m *SingleRef) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateType(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var singleRefTypeTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["Thing","Action","Key"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		singleRefTypeTypePropEnum = append(singleRefTypeTypePropEnum, v)
	}
}

const (
	// SingleRefTypeThing captures enum value "Thing"
	SingleRefTypeThing string = "Thing"
	// SingleRefTypeAction captures enum value "Action"
	SingleRefTypeAction string = "Action"
	// SingleRefTypeKey captures enum value "Key"
	SingleRefTypeKey string = "Key"
)

// prop value enum
func (m *SingleRef) validateTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, singleRefTypeTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *SingleRef) validateType(formats strfmt.Registry) error {

	if swag.IsZero(m.Type) { // not required
		return nil
	}

	// value enum
	if err := m.validateTypeEnum("type", "body", m.Type); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *SingleRef) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *SingleRef) UnmarshalBinary(b []byte) error {
	var res SingleRef
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
