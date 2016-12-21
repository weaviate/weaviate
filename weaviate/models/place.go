package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// Place place
// swagger:model Place
type Place struct {

	// id
	ID string `json:"id,omitempty"`

	// invitation
	Invitation *PlaceInvitation `json:"invitation,omitempty"`

	// member
	Member []*PlaceMember `json:"member"`

	// name
	Name string `json:"name,omitempty"`
}

// Validate validates this place
func (m *Place) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateInvitation(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateMember(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Place) validateInvitation(formats strfmt.Registry) error {

	if swag.IsZero(m.Invitation) { // not required
		return nil
	}

	if m.Invitation != nil {

		if err := m.Invitation.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

func (m *Place) validateMember(formats strfmt.Registry) error {

	if swag.IsZero(m.Member) { // not required
		return nil
	}

	for i := 0; i < len(m.Member); i++ {

		if swag.IsZero(m.Member[i]) { // not required
			continue
		}

		if m.Member[i] != nil {

			if err := m.Member[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
