package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// PlacesAddMemberResponse places add member response
// swagger:model PlacesAddMemberResponse
type PlacesAddMemberResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#placesAddMemberResponse".
	Kind *string `json:"kind,omitempty"`

	// Modified place.
	Place *Place `json:"place,omitempty"`
}

// Validate validates this places add member response
func (m *PlacesAddMemberResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validatePlace(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PlacesAddMemberResponse) validatePlace(formats strfmt.Registry) error {

	if swag.IsZero(m.Place) { // not required
		return nil
	}

	if m.Place != nil {

		if err := m.Place.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
