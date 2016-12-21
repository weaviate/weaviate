package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// PublicLocalAuthInfo public local auth info
// swagger:model PublicLocalAuthInfo
type PublicLocalAuthInfo struct {

	// cert fingerprint
	CertFingerprint string `json:"certFingerprint,omitempty"`

	// local Id
	LocalID string `json:"localId,omitempty"`
}

// Validate validates this public local auth info
func (m *PublicLocalAuthInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
