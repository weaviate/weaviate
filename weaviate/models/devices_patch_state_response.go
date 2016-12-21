package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// DevicesPatchStateResponse devices patch state response
// swagger:model DevicesPatchStateResponse
type DevicesPatchStateResponse struct {

	// Updated device state.
	State JSONObject `json:"state,omitempty"`
}

// Validate validates this devices patch state response
func (m *DevicesPatchStateResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
