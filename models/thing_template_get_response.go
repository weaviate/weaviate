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
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// ThingTemplateGetResponse thing template get response
// swagger:model ThingTemplateGetResponse
type ThingTemplateGetResponse struct {
	ThingTemplate

	// id
	ID strfmt.UUID `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weaviate#thingTemplateGetResponse".
	Kind *string `json:"kind,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *ThingTemplateGetResponse) UnmarshalJSON(raw []byte) error {

	var aO0 ThingTemplate
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.ThingTemplate = aO0

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`

		Kind *string `json:"kind,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.ID = data.ID

	m.Kind = data.Kind

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m ThingTemplateGetResponse) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.ThingTemplate)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		ID strfmt.UUID `json:"id,omitempty"`

		Kind *string `json:"kind,omitempty"`
	}

	data.ID = m.ID

	data.Kind = m.Kind

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this thing template get response
func (m *ThingTemplateGetResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.ThingTemplate.Validate(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// MarshalBinary interface implementation
func (m *ThingTemplateGetResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ThingTemplateGetResponse) UnmarshalBinary(b []byte) error {
	var res ThingTemplateGetResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
