package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// DevicesCreateLocalAuthTokensRequest devices create local auth tokens request
// swagger:model DevicesCreateLocalAuthTokensRequest
type DevicesCreateLocalAuthTokensRequest struct {

	// Device IDs.
	DeviceIds []string `json:"deviceIds"`
}

// Validate validates this devices create local auth tokens request
func (m *DevicesCreateLocalAuthTokensRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateDeviceIds(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *DevicesCreateLocalAuthTokensRequest) validateDeviceIds(formats strfmt.Registry) error {

	if swag.IsZero(m.DeviceIds) { // not required
		return nil
	}

	return nil
}
