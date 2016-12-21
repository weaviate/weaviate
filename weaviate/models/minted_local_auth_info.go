package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// MintedLocalAuthInfo minted local auth info
// swagger:model MintedLocalAuthInfo
type MintedLocalAuthInfo struct {

	// client token
	ClientToken string `json:"clientToken,omitempty"`

	// device Id
	DeviceID string `json:"deviceId,omitempty"`

	// device token
	DeviceToken string `json:"deviceToken,omitempty"`

	// local access info
	LocalAccessInfo *LocalAccessInfo `json:"localAccessInfo,omitempty"`

	// retry after
	RetryAfter int64 `json:"retryAfter,omitempty"`
}

// Validate validates this minted local auth info
func (m *MintedLocalAuthInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocalAccessInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *MintedLocalAuthInfo) validateLocalAccessInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.LocalAccessInfo) { // not required
		return nil
	}

	if m.LocalAccessInfo != nil {

		if err := m.LocalAccessInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
