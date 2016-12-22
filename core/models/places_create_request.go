package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// PlacesCreateRequest places create request
// swagger:model PlacesCreateRequest
type PlacesCreateRequest struct {

	// Name of the new place.
	Name string `json:"name,omitempty"`
}

// Validate validates this places create request
func (m *PlacesCreateRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
