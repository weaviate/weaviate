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

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// LocalAccessEntry local access entry
// swagger:model LocalAccessEntry
type LocalAccessEntry struct {

	// Whether this belongs to a delegated app or user.
	IsApp bool `json:"isApp,omitempty"`

	// Access role of the user.
	LocalAccessRole string `json:"localAccessRole,omitempty"`

	// Project id of the app that this access info is associated with.
	ProjectID int64 `json:"projectId,omitempty"`
}

// Validate validates this local access entry
func (m *LocalAccessEntry) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocalAccessRole(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var localAccessEntryTypeLocalAccessRolePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["manager","owner","robot","user","viewer"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		localAccessEntryTypeLocalAccessRolePropEnum = append(localAccessEntryTypeLocalAccessRolePropEnum, v)
	}
}

const (
	localAccessEntryLocalAccessRoleManager string = "manager"
	localAccessEntryLocalAccessRoleOwner   string = "owner"
	localAccessEntryLocalAccessRoleRobot   string = "robot"
	localAccessEntryLocalAccessRoleUser    string = "user"
	localAccessEntryLocalAccessRoleViewer  string = "viewer"
)

// prop value enum
func (m *LocalAccessEntry) validateLocalAccessRoleEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, localAccessEntryTypeLocalAccessRolePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *LocalAccessEntry) validateLocalAccessRole(formats strfmt.Registry) error {

	if swag.IsZero(m.LocalAccessRole) { // not required
		return nil
	}

	// value enum
	if err := m.validateLocalAccessRoleEnum("localAccessRole", "body", m.LocalAccessRole); err != nil {
		return err
	}

	return nil
}
