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
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package models




import (
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// ModelManifest Model manifest info.
// swagger:model ModelManifest
type ModelManifest struct {

	// For gateways, a list of thing ids that are allowed to connect to it.
	AllowedChildModelManifestIds []string `json:"allowedChildModelManifestIds"`

	// List of applications recommended to use with a thing model.
	Applications []*Application `json:"applications"`

	// URL of image showing a confirmation button.
	ConfirmationImageURL string `json:"confirmationImageUrl,omitempty"`

	// The list of groups.
	Groups []*Group `json:"groups"`

	// Unique model manifest ID.
	ID string `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#modelManifest".
	Kind *string `json:"kind,omitempty"`

	// User readable thing model description.
	ModelDescription string `json:"modelDescription,omitempty"`

	// User readable thing model name.
	ModelName string `json:"modelName,omitempty"`

	// User readable name of thing model manufacturer.
	OemName string `json:"oemName,omitempty"`

	// URL of thing support page.
	SupportPageURL string `json:"supportPageUrl,omitempty"`

	// URL of thing image.
	ThingImageURL string `json:"thingImageUrl,omitempty"`

	// Thing kind, see "thingKind" field of the Thing resource. See list of thing kinds values.
	ThingKind string `json:"thingKind,omitempty"`
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

	if err := m.validateGroups(formats); err != nil {
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
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("applications" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *ModelManifest) validateGroups(formats strfmt.Registry) error {

	if swag.IsZero(m.Groups) { // not required
		return nil
	}

	for i := 0; i < len(m.Groups); i++ {

		if swag.IsZero(m.Groups[i]) { // not required
			continue
		}

		if m.Groups[i] != nil {

			if err := m.Groups[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("groups" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}
