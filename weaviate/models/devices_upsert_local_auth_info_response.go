package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// DevicesUpsertLocalAuthInfoResponse devices upsert local auth info response
// swagger:model DevicesUpsertLocalAuthInfoResponse
type DevicesUpsertLocalAuthInfoResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#devicesUpsertLocalAuthInfoResponse".
	Kind *string `json:"kind,omitempty"`

	// The non-secret local auth info.
	LocalAuthInfo interface{} `json:"localAuthInfo,omitempty"`
}

// Validate validates this devices upsert local auth info response
func (m *DevicesUpsertLocalAuthInfoResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
