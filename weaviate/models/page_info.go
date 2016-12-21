package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// PageInfo page info
// swagger:model PageInfo
type PageInfo struct {

	// result per page
	ResultPerPage int32 `json:"resultPerPage,omitempty"`

	// start index
	StartIndex int32 `json:"startIndex,omitempty"`

	// total results
	TotalResults int32 `json:"totalResults,omitempty"`
}

// Validate validates this page info
func (m *PageInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
