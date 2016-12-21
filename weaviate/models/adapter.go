package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// Adapter adapter
// swagger:model Adapter
type Adapter struct {

	// URL to adapter web flow to activate the adapter. Deprecated, use the activationUrl returned in the response of the Adapters.activate API.
	ActivateURL string `json:"activateUrl,omitempty"`

	// Whether this adapter has been activated for the current user.
	Activated bool `json:"activated,omitempty"`

	// URL to adapter web flow to disconnect the adapter. Deprecated, the adapter will be notified via pubsub.
	DeactivateURL string `json:"deactivateUrl,omitempty"`

	// Display name of the adapter.
	DisplayName string `json:"displayName,omitempty"`

	// URL to an icon that represents the adapter.
	IconURL string `json:"iconUrl,omitempty"`

	// ID of the adapter.
	ID string `json:"id,omitempty"`

	// URL to adapter web flow to connect new devices. Only used for adapters that cannot automatically detect new devices. This field is returned only if the user has already activated the adapter.
	ManageURL string `json:"manageUrl,omitempty"`
}

// Validate validates this adapter
func (m *Adapter) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
