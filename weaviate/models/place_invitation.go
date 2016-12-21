package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// PlaceInvitation place invitation
// swagger:model PlaceInvitation
type PlaceInvitation struct {

	// member
	Member *PlaceMember `json:"member,omitempty"`

	// place Id
	PlaceID string `json:"placeId,omitempty"`
}

// Validate validates this place invitation
func (m *PlaceInvitation) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateMember(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PlaceInvitation) validateMember(formats strfmt.Registry) error {

	if swag.IsZero(m.Member) { // not required
		return nil
	}

	if m.Member != nil {

		if err := m.Member.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
