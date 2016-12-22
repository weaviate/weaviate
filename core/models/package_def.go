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
	"encoding/json"

	"github.com/go-openapi/errors"
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// PackageDef package def
// swagger:model PackageDef
type PackageDef map[string]PackageDefAnon

// Validate validates this package def
func (m PackageDef) Validate(formats strfmt.Registry) error {
	var res []error

	if swag.IsZero(m) { // not required
		return nil
	}

	for k := range m {

		if swag.IsZero(m[k]) { // not required
			continue
		}

		if val, ok := m[k]; ok {

			if err := val.Validate(formats); err != nil {
				return err
			}
		}

	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// PackageDefAnon package def anon
// swagger:model PackageDefAnon
type PackageDefAnon struct {

	// Display name of the command.
	DisplayName string `json:"displayName,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#commandDef".
	Kind *string `json:"kind,omitempty"`

	// Minimal role required to execute command.
	MinimalRole string `json:"minimalRole,omitempty"`

	// Parameters of the command.
	Parameters map[string]JSONObject `json:"parameters,omitempty"`
}

// Validate validates this package def anon
func (m *PackageDefAnon) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateMinimalRole(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var packageDefAnonTypeMinimalRolePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["manager","owner","user","viewer"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		packageDefAnonTypeMinimalRolePropEnum = append(packageDefAnonTypeMinimalRolePropEnum, v)
	}
}

const (
	packageDefAnonMinimalRoleManager string = "manager"
	packageDefAnonMinimalRoleOwner   string = "owner"
	packageDefAnonMinimalRoleUser    string = "user"
	packageDefAnonMinimalRoleViewer  string = "viewer"
)

// prop value enum
func (m *PackageDefAnon) validateMinimalRoleEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, packageDefAnonTypeMinimalRolePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *PackageDefAnon) validateMinimalRole(formats strfmt.Registry) error {

	if swag.IsZero(m.MinimalRole) { // not required
		return nil
	}

	// value enum
	if err := m.validateMinimalRoleEnum("minimalRole", "body", m.MinimalRole); err != nil {
		return err
	}

	return nil
}
