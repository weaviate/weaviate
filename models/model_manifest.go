/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// ModelManifest Model manifest info.
// swagger:model ModelManifest
type ModelManifest struct {

	// For gateways, a list of device ids that are allowed to connect to it.
	AllowedChildModelManifestIds []string `json:"allowedChildModelManifestIds"`

	// List of applications recommended to use with a device model.
	Applications []*Application `json:"applications"`

	// URL of image showing a confirmation button.
	ConfirmationImageURL string `json:"confirmationImageUrl,omitempty"`

	// URL of device image.
	DeviceImageURL string `json:"deviceImageUrl,omitempty"`

	// Device kind, see "deviceKind" field of the Device resource. See list of device kinds values.
	DeviceKind string `json:"deviceKind,omitempty"`

	// Unique model manifest ID.
	ID string `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#modelManifest".
	Kind *string `json:"kind,omitempty"`

	// User readable device model description.
	ModelDescription string `json:"modelDescription,omitempty"`

	// User readable device model name.
	ModelName string `json:"modelName,omitempty"`

	// User readable name of device model manufacturer.
	OemName string `json:"oemName,omitempty"`

	// URL of device support page.
	SupportPageURL string `json:"supportPageUrl,omitempty"`
}

// Validate validates this model manifest
func (m *ModelManifest) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAllowedChildModelManifestIds(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateApplications(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ModelManifest) validateAllowedChildModelManifestIds(formats strfmt.Registry) error {

	if swag.IsZero(m.AllowedChildModelManifestIds) { // not required
		return nil
	}

	return nil
}

func (m *ModelManifest) validateApplications(formats strfmt.Registry) error {

	if swag.IsZero(m.Applications) { // not required
		return nil
	}

	for i := 0; i < len(m.Applications); i++ {

		if swag.IsZero(m.Applications[i]) { // not required
			continue
		}

		if m.Applications[i] != nil {

			if err := m.Applications[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
