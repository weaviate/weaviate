package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// ModelManifestsValidateDeviceStateRequest model manifests validate device state request
// swagger:model ModelManifestsValidateDeviceStateRequest
type ModelManifestsValidateDeviceStateRequest struct {

	// Device state object.
	State JSONObject `json:"state,omitempty"`
}

// Validate validates this model manifests validate device state request
func (m *ModelManifestsValidateDeviceStateRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
