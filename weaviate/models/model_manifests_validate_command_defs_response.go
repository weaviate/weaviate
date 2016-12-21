package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// ModelManifestsValidateCommandDefsResponse model manifests validate command defs response
// swagger:model ModelManifestsValidateCommandDefsResponse
type ModelManifestsValidateCommandDefsResponse struct {

	// Validation errors in command definitions.
	ValidationErrors []string `json:"validationErrors"`
}

// Validate validates this model manifests validate command defs response
func (m *ModelManifestsValidateCommandDefsResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateValidationErrors(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ModelManifestsValidateCommandDefsResponse) validateValidationErrors(formats strfmt.Registry) error {

	if swag.IsZero(m.ValidationErrors) { // not required
		return nil
	}

	return nil
}
