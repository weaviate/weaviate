package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// PlacesHandleInvitationResponse places handle invitation response
// swagger:model PlacesHandleInvitationResponse
type PlacesHandleInvitationResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#placesHandleInvitationResponse".
	Kind *string `json:"kind,omitempty"`
}

// Validate validates this places handle invitation response
func (m *PlacesHandleInvitationResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
