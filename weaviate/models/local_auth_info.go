package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// LocalAuthInfo local auth info
// swagger:model LocalAuthInfo
type LocalAuthInfo struct {

	// cert fingerprint
	CertFingerprint string `json:"certFingerprint,omitempty"`

	// client token
	ClientToken string `json:"clientToken,omitempty"`

	// device token
	DeviceToken string `json:"deviceToken,omitempty"`

	// local Id
	LocalID string `json:"localId,omitempty"`

	// public local auth info
	PublicLocalAuthInfo *PublicLocalAuthInfo `json:"publicLocalAuthInfo,omitempty"`

	// revocation client token
	RevocationClientToken string `json:"revocationClientToken,omitempty"`
}

// Validate validates this local auth info
func (m *LocalAuthInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validatePublicLocalAuthInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *LocalAuthInfo) validatePublicLocalAuthInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.PublicLocalAuthInfo) { // not required
		return nil
	}

	if m.PublicLocalAuthInfo != nil {

		if err := m.PublicLocalAuthInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
