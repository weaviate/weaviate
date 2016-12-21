package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// ModelManifestsValidateCommandDefsRequest model manifests validate command defs request
// swagger:model ModelManifestsValidateCommandDefsRequest
type ModelManifestsValidateCommandDefsRequest struct {

	// Description of commands.
	CommandDefs map[string]PackageDef `json:"commandDefs,omitempty"`
}

// Validate validates this model manifests validate command defs request
func (m *ModelManifestsValidateCommandDefsRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
