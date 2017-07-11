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

// Thing thing
// swagger:model Thing
type Thing struct {
	ThingTemplate

	// Thing connection status.
	ConnectionStatus string `json:"connectionStatus,omitempty"`

	// Timestamp of creation of this thing in milliseconds since epoch UTC.
	CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

	// User readable description of this thing.
	Description string `json:"description,omitempty"`

	// The list of groups.
	Groups []strfmt.UUID `json:"groups"`

	// Timestamp of the last request from this thing in milliseconds since epoch UTC. Supported only for things with XMPP channel type.
	LastSeenTimeMs int64 `json:"lastSeenTimeMs,omitempty"`

	// Timestamp of the last thing update in milliseconds since epoch UTC.
	LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty"`

	// Timestamp of the last thing usage in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// ID of the location of this thing.
	LocationID strfmt.UUID `json:"locationId,omitempty"`

	// E-mail address of the thing owner.
	Owner string `json:"owner,omitempty"`

	// Serial number of a thing provided by its manufacturer.
	SerialNumber string `json:"serialNumber,omitempty"`

	// Custom free-form manufacturer tags.
	Tags []string `json:"tags"`

	// Thing template ID of this thing.
	ThingTemplateID strfmt.UUID `json:"thingTemplateId,omitempty"`
}

// UnmarshalJSON unmarshals this object from a JSON structure
func (m *Thing) UnmarshalJSON(raw []byte) error {

	var aO0 ThingTemplate
	if err := swag.ReadJSON(raw, &aO0); err != nil {
		return err
	}
	m.ThingTemplate = aO0

	var data struct {
		ConnectionStatus string `json:"connectionStatus,omitempty"`

		CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

		Description string `json:"description,omitempty"`

		Groups []strfmt.UUID `json:"groups,omitempty"`

		LastSeenTimeMs int64 `json:"lastSeenTimeMs,omitempty"`

		LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty"`

		LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

		LocationID strfmt.UUID `json:"locationId,omitempty"`

		Owner string `json:"owner,omitempty"`

		SerialNumber string `json:"serialNumber,omitempty"`

		Tags []string `json:"tags,omitempty"`

		ThingTemplateID strfmt.UUID `json:"thingTemplateId,omitempty"`
	}
	if err := swag.ReadJSON(raw, &data); err != nil {
		return err
	}

	m.ConnectionStatus = data.ConnectionStatus

	m.CreationTimeMs = data.CreationTimeMs

	m.Description = data.Description

	m.Groups = data.Groups

	m.LastSeenTimeMs = data.LastSeenTimeMs

	m.LastUpdateTimeMs = data.LastUpdateTimeMs

	m.LastUseTimeMs = data.LastUseTimeMs

	m.LocationID = data.LocationID

	m.Owner = data.Owner

	m.SerialNumber = data.SerialNumber

	m.Tags = data.Tags

	m.ThingTemplateID = data.ThingTemplateID

	return nil
}

// MarshalJSON marshals this object to a JSON structure
func (m Thing) MarshalJSON() ([]byte, error) {
	var _parts [][]byte

	aO0, err := swag.WriteJSON(m.ThingTemplate)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, aO0)

	var data struct {
		ConnectionStatus string `json:"connectionStatus,omitempty"`

		CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

		Description string `json:"description,omitempty"`

		Groups []strfmt.UUID `json:"groups,omitempty"`

		LastSeenTimeMs int64 `json:"lastSeenTimeMs,omitempty"`

		LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty"`

		LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

		LocationID strfmt.UUID `json:"locationId,omitempty"`

		Owner string `json:"owner,omitempty"`

		SerialNumber string `json:"serialNumber,omitempty"`

		Tags []string `json:"tags,omitempty"`

		ThingTemplateID strfmt.UUID `json:"thingTemplateId,omitempty"`
	}

	data.ConnectionStatus = m.ConnectionStatus

	data.CreationTimeMs = m.CreationTimeMs

	data.Description = m.Description

	data.Groups = m.Groups

	data.LastSeenTimeMs = m.LastSeenTimeMs

	data.LastUpdateTimeMs = m.LastUpdateTimeMs

	data.LastUseTimeMs = m.LastUseTimeMs

	data.LocationID = m.LocationID

	data.Owner = m.Owner

	data.SerialNumber = m.SerialNumber

	data.Tags = m.Tags

	data.ThingTemplateID = m.ThingTemplateID

	jsonData, err := swag.WriteJSON(data)
	if err != nil {
		return nil, err
	}
	_parts = append(_parts, jsonData)

	return swag.ConcatJSON(_parts...), nil
}

// Validate validates this thing
func (m *Thing) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.ThingTemplate.Validate(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateGroups(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateTags(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Thing) validateGroups(formats strfmt.Registry) error {

	if swag.IsZero(m.Groups) { // not required
		return nil
	}

	return nil
}

func (m *Thing) validateTags(formats strfmt.Registry) error {

	if swag.IsZero(m.Tags) { // not required
		return nil
	}

	return nil
}
