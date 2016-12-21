package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// PlacesRemoveMemberRequest places remove member request
// swagger:model PlacesRemoveMemberRequest
type PlacesRemoveMemberRequest struct {

	// Email of the member to be removed.
	Member string `json:"member,omitempty"`
}

// Validate validates this places remove member request
func (m *PlacesRemoveMemberRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
