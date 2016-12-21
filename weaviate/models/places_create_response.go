package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// PlacesCreateResponse places create response
// swagger:model PlacesCreateResponse
type PlacesCreateResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#placesCreateResponse".
	Kind *string `json:"kind,omitempty"`

	// Newly created place.
	Place *Place `json:"place,omitempty"`
}

// Validate validates this places create response
func (m *PlacesCreateResponse) Validate(formats strfmt.Registry) error {
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

func (m *PlacesCreateResponse) validatePlace(formats strfmt.Registry) error {

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
