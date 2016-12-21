package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// ModelManifestsValidateDeviceStateResponse model manifests validate device state response
// swagger:model ModelManifestsValidateDeviceStateResponse
type ModelManifestsValidateDeviceStateResponse struct {

	// Validation errors in device state.
	ValidationErrors []string `json:"validationErrors"`
}

// Validate validates this model manifests validate device state response
func (m *ModelManifestsValidateDeviceStateResponse) Validate(formats strfmt.Registry) error {
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

func (m *ModelManifestsValidateDeviceStateResponse) validateValidationErrors(formats strfmt.Registry) error {

	if swag.IsZero(m.ValidationErrors) { // not required
		return nil
	}

	return nil
}
