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

// GroupCreate group create
// swagger:model GroupCreate
type GroupCreate struct {
	Group

	// The items in the group.
	Ids []*GroupCreateIdsItems0 `json:"ids"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *GroupCreate) UnmarshalJSON(raw []byte) error {

	var aO0 Group
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.Group = aO0

	var data struct {
		Ids []*GroupCreateIdsItems0 `json:"ids,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.Ids = data.Ids

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m GroupCreate) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.Group)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		Ids []*GroupCreateIdsItems0 `json:"ids,omitempty"`
	}

	data.Ids = m.Ids

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this group create
func (m *GroupCreate) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.Group.Validate(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateIds(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *GroupCreate) validateIds(formats strfmt.Registry) error {

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

// GroupCreateIdsItems0 group create ids items0
// swagger:model GroupCreateIdsItems0
type GroupCreateIdsItems0 struct {
	GroupID
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *GroupCreateIdsItems0) UnmarshalJSON(raw []byte) error {

	var aO0 GroupID
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.GroupID = aO0

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m GroupCreateIdsItems0) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.GroupID)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this group create ids items0
func (m *GroupCreateIdsItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.GroupID.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
