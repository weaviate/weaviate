package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// PlacesRemoveMemberResponse places remove member response
// swagger:model PlacesRemoveMemberResponse
type PlacesRemoveMemberResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#placesRemoveMemberResponse".
	Kind *string `json:"kind,omitempty"`

	// Modified place.
	Place *Place `json:"place,omitempty"`
}

// Validate validates this places remove member response
func (m *PlacesRemoveMemberResponse) Validate(formats strfmt.Registry) error {
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

func (m *PlacesRemoveMemberResponse) validatePlace(formats strfmt.Registry) error {

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
