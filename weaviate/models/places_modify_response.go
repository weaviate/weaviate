package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// PlacesModifyResponse places modify response
// swagger:model PlacesModifyResponse
type PlacesModifyResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#placesModifyResponse".
	Kind *string `json:"kind,omitempty"`

	// Modified place.
	Place *Place `json:"place,omitempty"`
}

// Validate validates this places modify response
func (m *PlacesModifyResponse) Validate(formats strfmt.Registry) error {
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

func (m *PlacesModifyResponse) validatePlace(formats strfmt.Registry) error {

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
