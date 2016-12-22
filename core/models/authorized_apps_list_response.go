package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// AuthorizedAppsListResponse List of authorized apps.
// swagger:model AuthorizedAppsListResponse
type AuthorizedAppsListResponse struct {

	// The list of authorized apps.
	AuthorizedApps []*AuthorizedApp `json:"authorizedApps"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#authorizedAppsListResponse".
	Kind *string `json:"kind,omitempty"`
}

// Validate validates this authorized apps list response
func (m *AuthorizedAppsListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAuthorizedApps(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *AuthorizedAppsListResponse) validateAuthorizedApps(formats strfmt.Registry) error {

	if swag.IsZero(m.AuthorizedApps) { // not required
		return nil
	}

	for i := 0; i < len(m.AuthorizedApps); i++ {

		if swag.IsZero(m.AuthorizedApps[i]) { // not required
			continue
		}

		if m.AuthorizedApps[i] != nil {

			if err := m.AuthorizedApps[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
