package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// Invitation invitation
// swagger:model Invitation
type Invitation struct {

	// ACL entry associated with this invitation.
	ACLEntry *ACLEntry `json:"aclEntry,omitempty"`

	// Email of a user who created this invitation.
	CreatorEmail string `json:"creatorEmail,omitempty"`
}

// Validate validates this invitation
func (m *Invitation) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateACLEntry(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Invitation) validateACLEntry(formats strfmt.Registry) error {

	if swag.IsZero(m.ACLEntry) { // not required
		return nil
	}

	if m.ACLEntry != nil {

		if err := m.ACLEntry.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
