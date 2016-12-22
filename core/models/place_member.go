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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package models

import (
	"encoding/json"

	"github.com/go-openapi/errors"
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// PlaceMember place member
// swagger:model PlaceMember
type PlaceMember struct {

	// creator email
	CreatorEmail string `json:"creatorEmail,omitempty"`

	// id
	ID string `json:"id,omitempty"`

	// pending
	Pending bool `json:"pending,omitempty"`

	// role
	Role string `json:"role,omitempty"`
}

// Validate validates this place member
func (m *PlaceMember) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateRole(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var placeMemberTypeRolePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["manager","owner","unknownRole"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		placeMemberTypeRolePropEnum = append(placeMemberTypeRolePropEnum, v)
	}
}

const (
	placeMemberRoleManager     string = "manager"
	placeMemberRoleOwner       string = "owner"
	placeMemberRoleUnknownRole string = "unknownRole"
)

// prop value enum
func (m *PlaceMember) validateRoleEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, placeMemberTypeRolePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *PlaceMember) validateRole(formats strfmt.Registry) error {

	if swag.IsZero(m.Role) { // not required
		return nil
	}

	// value enum
	if err := m.validateRoleEnum("role", "body", m.Role); err != nil {
		return err
	}

	return nil
}
