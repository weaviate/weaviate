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

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// ThingTemplate thing template
// swagger:model ThingTemplate
type ThingTemplate struct {

	// The id of the commands that this device is able to execute.
	CommandsIds []strfmt.UUID `json:"commandsIds"`

	// Name of this thing provided by the manufacturer.
	Name string `json:"name,omitempty"`

	// thing model template
	ThingModelTemplate *ThingTemplateThingModelTemplate `json:"thingModelTemplate,omitempty"`
}

// Validate validates this thing template
func (m *ThingTemplate) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCommandsIds(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateThingModelTemplate(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ThingTemplate) validateCommandsIds(formats strfmt.Registry) error {

	if swag.IsZero(m.CommandsIds) { // not required
		return nil
	}

	return nil
}

func (m *ThingTemplate) validateThingModelTemplate(formats strfmt.Registry) error {

	if swag.IsZero(m.ThingModelTemplate) { // not required
		return nil
	}

	if m.ThingModelTemplate != nil {

		if err := m.ThingModelTemplate.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("thingModelTemplate")
			}
			return err
		}
	}

	return nil
}

// ThingTemplateThingModelTemplate Thing template information provided by the thing template of this thing.
// swagger:model ThingTemplateThingModelTemplate
type ThingTemplateThingModelTemplate struct {

	// Thing model name.
	ModelName string `json:"modelName,omitempty"`

	// OEM additions as key values
	OemAdditions map[string]JSONValue `json:"oemAdditions,omitempty"`

	// Contact information in URL format.
	OemContact string `json:"oemContact,omitempty"`

	// Image of icon.
	OemIcon string `json:"oemIcon,omitempty"`

	// Name of thing model manufacturer.
	OemName string `json:"oemName,omitempty"`

	// Unique OEM oemNumber
	OemNumber string `json:"oemNumber,omitempty"`
}

// Validate validates this thing template thing model template
func (m *ThingTemplateThingModelTemplate) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
