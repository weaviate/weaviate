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

// Event event
// swagger:model Event
type Event struct {

	// command patch
	CommandPatch *EventCommandPatch `json:"commandPatch,omitempty"`

	// New device connection state (if connectivity change event).
	ConnectionStatus string `json:"connectionStatus,omitempty"`

	// The device that was affected by this event.
	DeviceID string `json:"deviceId,omitempty"`

	// ID of the event.
	ID string `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#event".
	Kind *string `json:"kind,omitempty"`

	// state patch
	StatePatch JSONObject `json:"statePatch,omitempty"`

	// Time the event was generated in milliseconds since epoch UTC.
	TimeMs int64 `json:"timeMs,omitempty"`

	// Type of the event.
	Type string `json:"type,omitempty"`

	// User that caused the event (if applicable).
	UserEmail string `json:"userEmail,omitempty"`
}

// Validate validates this event
func (m *Event) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCommandPatch(formats); err != nil {
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

func (m *Event) validateCommandPatch(formats strfmt.Registry) error {

	if swag.IsZero(m.CommandPatch) { // not required
		return nil
	}

	if m.CommandPatch != nil {

		if err := m.CommandPatch.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

var eventTypeTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["adapterDeactivated","commandCancelled","commandCreated","commandDeleted","commandExpired","commandUpdated","deviceAclUpdated","deviceConnectivityChange","deviceCreated","deviceDeleted","devicePlaceUpdated","deviceTransferred","deviceUpdated","deviceUseTimeUpdated","deviceUserAclCreated","deviceUserAclDeleted","deviceUserAclUpdated","eventsDeleted","eventsRecordingDisabled","eventsRecordingEnabled","placeCreated","placeDeleted","placeMemberAdded","placeMemberRemoved","placeUpdated","roomCreated","roomDeleted","roomUpdated"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		eventTypeTypePropEnum = append(eventTypeTypePropEnum, v)
	}
}

const (
	eventTypeAdapterDeactivated       string = "adapterDeactivated"
	eventTypeCommandCancelled         string = "commandCancelled"
	eventTypeCommandCreated           string = "commandCreated"
	eventTypeCommandDeleted           string = "commandDeleted"
	eventTypeCommandExpired           string = "commandExpired"
	eventTypeCommandUpdated           string = "commandUpdated"
	eventTypeDeviceACLUpdated         string = "deviceAclUpdated"
	eventTypeDeviceConnectivityChange string = "deviceConnectivityChange"
	eventTypeDeviceCreated            string = "deviceCreated"
	eventTypeDeviceDeleted            string = "deviceDeleted"
	eventTypeDevicePlaceUpdated       string = "devicePlaceUpdated"
	eventTypeDeviceTransferred        string = "deviceTransferred"
	eventTypeDeviceUpdated            string = "deviceUpdated"
	eventTypeDeviceUseTimeUpdated     string = "deviceUseTimeUpdated"
	eventTypeDeviceUserACLCreated     string = "deviceUserAclCreated"
	eventTypeDeviceUserACLDeleted     string = "deviceUserAclDeleted"
	eventTypeDeviceUserACLUpdated     string = "deviceUserAclUpdated"
	eventTypeEventsDeleted            string = "eventsDeleted"
	eventTypeEventsRecordingDisabled  string = "eventsRecordingDisabled"
	eventTypeEventsRecordingEnabled   string = "eventsRecordingEnabled"
	eventTypePlaceCreated             string = "placeCreated"
	eventTypePlaceDeleted             string = "placeDeleted"
	eventTypePlaceMemberAdded         string = "placeMemberAdded"
	eventTypePlaceMemberRemoved       string = "placeMemberRemoved"
	eventTypePlaceUpdated             string = "placeUpdated"
	eventTypeRoomCreated              string = "roomCreated"
	eventTypeRoomDeleted              string = "roomDeleted"
	eventTypeRoomUpdated              string = "roomUpdated"
)

// prop value enum
func (m *Event) validateTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, eventTypeTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *Event) validateType(formats strfmt.Registry) error {

	if swag.IsZero(m.Type) { // not required
		return nil
	}

	// value enum
	if err := m.validateTypeEnum("type", "body", m.Type); err != nil {
		return err
	}

	return nil
}

// EventCommandPatch Command-related changes (if applicable).
// swagger:model EventCommandPatch
type EventCommandPatch struct {

	// ID of the affected command.
	CommandID string `json:"commandId,omitempty"`

	// New command state.
	State string `json:"state,omitempty"`
}

// Validate validates this event command patch
func (m *EventCommandPatch) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
