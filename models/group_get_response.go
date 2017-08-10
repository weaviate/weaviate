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

// GroupGetResponse group get response
// swagger:model GroupGetResponse
type GroupGetResponse struct {
	Group

	// ID of the group.
	ID strfmt.UUID `json:"id,omitempty"`

	// The items in the group.
	Ids []*GroupGetResponseIdsItems0 `json:"ids"`

	// Identifies what kind of resource this is. Value: the fixed string "weaviate#groupGetResponse".
	Kind *string `json:"kind,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *GroupGetResponse) UnmarshalJSON(raw []byte) error {

	var aO0 Group
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.Group = aO0

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`

		Ids []*GroupGetResponseIdsItems0 `json:"ids,omitempty"`

		Kind *string `json:"kind,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.ID = data.ID

	m.Ids = data.Ids

	m.Kind = data.Kind

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m GroupGetResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.Group)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`

		Ids []*GroupGetResponseIdsItems0 `json:"ids,omitempty"`

		Kind *string `json:"kind,omitempty"`
	}

	data.ID = m.ID

	data.Ids = m.Ids

	data.Kind = m.Kind

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this group get response
func (m *GroupGetResponse) Validate(formats strfmt.Registry) error {
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

func (m *GroupGetResponse) validateIds(formats strfmt.Registry) error {

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

// MarshalBinary interface implementation
func (m *GroupGetResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *GroupGetResponse) UnmarshalBinary(b []byte) error {
	var res GroupGetResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// GroupGetResponseIdsItems0 group get response ids items0
// swagger:model GroupGetResponseIdsItems0
type GroupGetResponseIdsItems0 struct {
	GroupID

	// RefType of object in Group
	RefType string `json:"refType,omitempty"`

	// URL of object in Group
	URL string `json:"url,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *GroupGetResponseIdsItems0) UnmarshalJSON(raw []byte) error {

	var aO0 GroupID
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.GroupID = aO0

	var data struct {
		RefType string `json:"refType,omitempty"`

		URL string `json:"url,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.RefType = data.RefType

	m.URL = data.URL

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m GroupGetResponseIdsItems0) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.GroupID)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		RefType string `json:"refType,omitempty"`

		URL string `json:"url,omitempty"`
	}

	data.RefType = m.RefType

	data.URL = m.URL

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this group get response ids items0
func (m *GroupGetResponseIdsItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.GroupID.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// MarshalBinary interface implementation
func (m *GroupGetResponseIdsItems0) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *GroupGetResponseIdsItems0) UnmarshalBinary(b []byte) error {
	var res GroupGetResponseIdsItems0
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
