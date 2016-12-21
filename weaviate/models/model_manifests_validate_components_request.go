package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// ModelManifestsValidateComponentsRequest model manifests validate components request
// swagger:model ModelManifestsValidateComponentsRequest
type ModelManifestsValidateComponentsRequest struct {

	// components
	Components string `json:"components,omitempty"`

	// traits
	Traits string `json:"traits,omitempty"`
}

// Validate validates this model manifests validate components request
func (m *ModelManifestsValidateComponentsRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
