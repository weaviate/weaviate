package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
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
