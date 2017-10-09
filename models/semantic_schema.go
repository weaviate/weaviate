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
	"encoding/json"
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// SemanticSchema Definitions of semantic schemas (also see: https://github.com/weaviate/weaviate-semantic-schemas)
// swagger:model SemanticSchema

type SemanticSchema struct {

	// URL of the context
	AtContext strfmt.URI `json:"@context,omitempty"`

	// Semantic classes that are available.
	Classes []*SemanticSchemaClassesItems0 `json:"classes"`

	// Email of the maintainer.
	Maintainer strfmt.Email `json:"maintainer,omitempty"`

	// Name of the schema
	Name string `json:"name,omitempty"`

	// Type of schema, should be "thing" or "action".
	Type string `json:"type,omitempty"`

	// Version number of the schema in semver format.
	Version string `json:"version,omitempty"`
}

/* polymorph SemanticSchema @context false */

/* polymorph SemanticSchema classes false */

/* polymorph SemanticSchema maintainer false */

/* polymorph SemanticSchema name false */

/* polymorph SemanticSchema type false */

/* polymorph SemanticSchema version false */

// Validate validates this semantic schema
func (m *SemanticSchema) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateClasses(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateType(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *SemanticSchema) validateClasses(formats strfmt.Registry) error {

	if swag.IsZero(m.Classes) { // not required
		return nil
	}

	for i := 0; i < len(m.Classes); i++ {

		if swag.IsZero(m.Classes[i]) { // not required
			continue
		}

		if m.Classes[i] != nil {

			if err := m.Classes[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("classes" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

var semanticSchemaTypeTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["thing","action"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		semanticSchemaTypeTypePropEnum = append(semanticSchemaTypeTypePropEnum, v)
	}
}

const (
	// SemanticSchemaTypeThing captures enum value "thing"
	SemanticSchemaTypeThing string = "thing"
	// SemanticSchemaTypeAction captures enum value "action"
	SemanticSchemaTypeAction string = "action"
)

// prop value enum
func (m *SemanticSchema) validateTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, semanticSchemaTypeTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *SemanticSchema) validateType(formats strfmt.Registry) error {

	if swag.IsZero(m.Type) { // not required
		return nil
	}

	// value enum
	if err := m.validateTypeEnum("type", "body", m.Type); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *SemanticSchema) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *SemanticSchema) UnmarshalBinary(b []byte) error {
	var res SemanticSchema
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// SemanticSchemaClassesItems0 semantic schema classes items0
// swagger:model SemanticSchemaClassesItems0

type SemanticSchemaClassesItems0 struct {

	// Name of the class as URI relative to the schema URL.
	Class string `json:"class,omitempty"`

	// Description of the class
	Description string `json:"description,omitempty"`

	// properties
	Properties *SemanticSchemaClassesItems0Properties `json:"properties,omitempty"`
}

/* polymorph SemanticSchemaClassesItems0 class false */

/* polymorph SemanticSchemaClassesItems0 description false */

/* polymorph SemanticSchemaClassesItems0 properties false */

// Validate validates this semantic schema classes items0
func (m *SemanticSchemaClassesItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateProperties(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *SemanticSchemaClassesItems0) validateProperties(formats strfmt.Registry) error {

	if swag.IsZero(m.Properties) { // not required
		return nil
	}

	if m.Properties != nil {

		if err := m.Properties.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("properties")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *SemanticSchemaClassesItems0) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *SemanticSchemaClassesItems0) UnmarshalBinary(b []byte) error {
	var res SemanticSchemaClassesItems0
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// SemanticSchemaClassesItems0Properties The properties of the class.
// swagger:model SemanticSchemaClassesItems0Properties

type SemanticSchemaClassesItems0Properties struct {

	// Can be a reference ($cref) to another type when starts with a capital (for example Person) otherwise "string" or "int".
	AtDataType []string `json:"@dataType"`

	// Description of the property
	Description string `json:"description,omitempty"`

	// Name of the property as URI relative to the schema URL.
	Name strfmt.URI `json:"name,omitempty"`
}

/* polymorph SemanticSchemaClassesItems0Properties @dataType false */

/* polymorph SemanticSchemaClassesItems0Properties description false */

/* polymorph SemanticSchemaClassesItems0Properties name false */

// Validate validates this semantic schema classes items0 properties
func (m *SemanticSchemaClassesItems0Properties) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAtDataType(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *SemanticSchemaClassesItems0Properties) validateAtDataType(formats strfmt.Registry) error {

	if swag.IsZero(m.AtDataType) { // not required
		return nil
	}

	return nil
}

// MarshalBinary interface implementation
func (m *SemanticSchemaClassesItems0Properties) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *SemanticSchemaClassesItems0Properties) UnmarshalBinary(b []byte) error {
	var res SemanticSchemaClassesItems0Properties
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
