package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// RoomsCreateRequest rooms create request
// swagger:model RoomsCreateRequest
type RoomsCreateRequest struct {

	// name
	Name string `json:"name,omitempty"`

	// type
	Type string `json:"type,omitempty"`
}

// Validate validates this rooms create request
func (m *RoomsCreateRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
