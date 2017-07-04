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

// Group Group.
// swagger:model Group
type Group struct {

	// The items in the group.
	Ids []*GroupIdsItems0 `json:"ids"`

	// Name of the group.
	Name string `json:"name,omitempty"`
}

// Validate validates this group
func (m *Group) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateIds(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Group) validateIds(formats strfmt.Registry) error {

	if swag.IsZero(m.Ids) { // not required
		return nil
	}

	for i := 0; i < len(m.Ids); i++ {

		if swag.IsZero(m.Ids[i]) { // not required
			continue
		}

		if m.Ids[i] != nil {

			if err := m.Ids[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("ids" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// GroupIdsItems0 group ids items0
// swagger:model GroupIdsItems0
type GroupIdsItems0 struct {

	// ID of object in Group
	ID strfmt.UUID `json:"id,omitempty"`

	// RefType of object in Group
	RefType string `json:"refType,omitempty"`

	// URL of object in Group
	URL string `json:"url,omitempty"`
}

// Validate validates this group ids items0
func (m *GroupIdsItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
