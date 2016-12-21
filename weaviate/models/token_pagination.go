package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// TokenPagination token pagination
// swagger:model TokenPagination
type TokenPagination struct {

	// next page token
	NextPageToken string `json:"nextPageToken,omitempty"`

	// previous page token
	PreviousPageToken string `json:"previousPageToken,omitempty"`
}

// Validate validates this token pagination
func (m *TokenPagination) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
