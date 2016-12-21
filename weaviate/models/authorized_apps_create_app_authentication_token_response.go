package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// AuthorizedAppsCreateAppAuthenticationTokenResponse Generate a token used to authenticate an authorized app.
// swagger:model AuthorizedAppsCreateAppAuthenticationTokenResponse
type AuthorizedAppsCreateAppAuthenticationTokenResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#authorizedAppsCreateAppAuthenticationTokenResponse".
	Kind *string `json:"kind,omitempty"`

	// Generated authentication token for an authorized app.
	Token string `json:"token,omitempty"`
}

// Validate validates this authorized apps create app authentication token response
func (m *AuthorizedAppsCreateAppAuthenticationTokenResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
