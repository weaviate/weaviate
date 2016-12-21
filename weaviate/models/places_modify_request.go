package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// PlacesModifyRequest places modify request
// swagger:model PlacesModifyRequest
type PlacesModifyRequest struct {

	// New name of the place.
	Name string `json:"name,omitempty"`
}

// Validate validates this places modify request
func (m *PlacesModifyRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
