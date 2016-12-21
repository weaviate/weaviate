package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// PersonalizedInfo personalized info
// swagger:model PersonalizedInfo
type PersonalizedInfo struct {

	// Unique personalizedInfo ID. Value: the fixed string "me".
	ID *string `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#personalizedInfo".
	Kind *string `json:"kind,omitempty"`

	// Timestamp of the last device usage by the user in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// Personalized device location.
	Location string `json:"location,omitempty"`

	// Personalized device display name.
	Name string `json:"name,omitempty"`
}

// Validate validates this personalized info
func (m *PersonalizedInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
