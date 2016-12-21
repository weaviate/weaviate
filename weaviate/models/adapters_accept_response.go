package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// AdaptersAcceptResponse adapters accept response
// swagger:model AdaptersAcceptResponse
type AdaptersAcceptResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#adaptersAcceptResponse".
	Kind *string `json:"kind,omitempty"`
}

// Validate validates this adapters accept response
func (m *AdaptersAcceptResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
