package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// DevicesUpsertLocalAuthInfoRequest devices upsert local auth info request
// swagger:model DevicesUpsertLocalAuthInfoRequest
type DevicesUpsertLocalAuthInfoRequest struct {

	// The local auth info of the device.
	LocalAuthInfo *LocalAuthInfo `json:"localAuthInfo,omitempty"`
}

// Validate validates this devices upsert local auth info request
func (m *DevicesUpsertLocalAuthInfoRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocalAuthInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *DevicesUpsertLocalAuthInfoRequest) validateLocalAuthInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.LocalAuthInfo) { // not required
		return nil
	}

	if m.LocalAuthInfo != nil {

		if err := m.LocalAuthInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
