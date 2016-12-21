package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// Device device
// swagger:model Device
type Device struct {

	// The HTTPS certificate fingerprint used to secure communication with device..
	CertFingerprint string `json:"certFingerprint,omitempty"`

	// channel
	Channel *DeviceChannel `json:"channel,omitempty"`

	// Description of commands supported by the device. This field is writable only by devices.
	CommandDefs map[string]PackageDef `json:"commandDefs,omitempty"`

	// Hierarchical componenet-based modeling of the device.
	Components JSONObject `json:"components,omitempty"`

	// Device connection status.
	ConnectionStatus string `json:"connectionStatus,omitempty"`

	// Timestamp of creation of this device in milliseconds since epoch UTC.
	CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

	// User readable description of this device.
	Description string `json:"description,omitempty"`

	// Device kind. Deprecated, provide "modelManifestId" instead. See list of device kinds values.
	DeviceKind string `json:"deviceKind,omitempty"`

	// The ID of the device for use on the local network.
	DeviceLocalID string `json:"deviceLocalId,omitempty"`

	// Unique device ID.
	ID string `json:"id,omitempty"`

	// List of pending invitations for the currently logged-in user.
	Invitations []*Invitation `json:"invitations"`

	// Indicates whether event recording is enabled or disabled for this device.
	IsEventRecordingDisabled bool `json:"isEventRecordingDisabled,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#device".
	Kind *string `json:"kind,omitempty"`

	// Timestamp of the last request from this device in milliseconds since epoch UTC. Supported only for devices with XMPP channel type.
	LastSeenTimeMs int64 `json:"lastSeenTimeMs,omitempty"`

	// Timestamp of the last device update in milliseconds since epoch UTC.
	LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty"`

	// Timestamp of the last device usage in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// User readable location of the device (name of the room, office number, building/floor, etc).
	Location string `json:"location,omitempty"`

	// model manifest
	ModelManifest *DeviceModelManifest `json:"modelManifest,omitempty"`

	// Model manifest ID of this device.
	ModelManifestID string `json:"modelManifestId,omitempty"`

	// Name of this device provided by the manufacturer.
	Name string `json:"name,omitempty"`

	// E-mail address of the device owner.
	Owner string `json:"owner,omitempty"`

	// personalized info
	PersonalizedInfo *DevicePersonalizedInfo `json:"personalizedInfo,omitempty"`

	// place Id
	PlaceID string `json:"placeId,omitempty"`

	// Hints for device's room and place names.
	PlacesHints interface{} `json:"placesHints,omitempty"`

	// Weave proposals for device's room and place names.
	PlacesProposal interface{} `json:"placesProposal,omitempty"`

	// room
	Room *Room `json:"room,omitempty"`

	// Serial number of a device provided by its manufacturer.
	SerialNumber string `json:"serialNumber,omitempty"`

	// Device state. This field is writable only by devices.
	State JSONObject `json:"state,omitempty"`

	// Description of the device state. This field is writable only by devices.
	StateDefs map[string]StateDef `json:"stateDefs,omitempty"`

	// Custom free-form manufacturer tags.
	Tags []string `json:"tags"`

	// Traits defined for the device.
	Traits JSONObject `json:"traits,omitempty"`

	// Device kind from the model manifest used in UI applications. See list of device kinds values.
	UIDeviceKind string `json:"uiDeviceKind,omitempty"`
}

// Validate validates this device
func (m *Device) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateChannel(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateInvitations(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateModelManifest(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePersonalizedInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateRoom(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateTags(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Device) validateChannel(formats strfmt.Registry) error {

	if swag.IsZero(m.Channel) { // not required
		return nil
	}

	if m.Channel != nil {

		if err := m.Channel.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

func (m *Device) validateInvitations(formats strfmt.Registry) error {

	if swag.IsZero(m.Invitations) { // not required
		return nil
	}

	for i := 0; i < len(m.Invitations); i++ {

		if swag.IsZero(m.Invitations[i]) { // not required
			continue
		}

		if m.Invitations[i] != nil {

			if err := m.Invitations[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}

func (m *Device) validateModelManifest(formats strfmt.Registry) error {

	if swag.IsZero(m.ModelManifest) { // not required
		return nil
	}

	if m.ModelManifest != nil {

		if err := m.ModelManifest.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

func (m *Device) validatePersonalizedInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.PersonalizedInfo) { // not required
		return nil
	}

	if m.PersonalizedInfo != nil {

		if err := m.PersonalizedInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

func (m *Device) validateRoom(formats strfmt.Registry) error {

	if swag.IsZero(m.Room) { // not required
		return nil
	}

	if m.Room != nil {

		if err := m.Room.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

func (m *Device) validateTags(formats strfmt.Registry) error {

	if swag.IsZero(m.Tags) { // not required
		return nil
	}

	return nil
}

// DeviceChannel Device notification channel description.
// swagger:model DeviceChannel
type DeviceChannel struct {

	// Connection status hint, set by parent device.
	ConnectionStatusHint string `json:"connectionStatusHint,omitempty"`

	// GCM registration ID. Required if device supports GCM delivery channel.
	GcmRegistrationID string `json:"gcmRegistrationId,omitempty"`

	// GCM sender ID. For Chrome apps must be the same as sender ID during registration, usually API project ID.
	GcmSenderID string `json:"gcmSenderId,omitempty"`

	// Parent device ID (aggregator) if it exists.
	ParentID string `json:"parentId,omitempty"`

	// pubsub
	Pubsub *DeviceChannelPubsub `json:"pubsub,omitempty"`

	// Channel type supported by device.
	SupportedType string `json:"supportedType,omitempty"`
}

// Validate validates this device channel
func (m *DeviceChannel) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateConnectionStatusHint(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePubsub(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateSupportedType(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var deviceChannelTypeConnectionStatusHintPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["offline","online","unknown"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		deviceChannelTypeConnectionStatusHintPropEnum = append(deviceChannelTypeConnectionStatusHintPropEnum, v)
	}
}

const (
	deviceChannelConnectionStatusHintOffline string = "offline"
	deviceChannelConnectionStatusHintOnline  string = "online"
	deviceChannelConnectionStatusHintUnknown string = "unknown"
)

// prop value enum
func (m *DeviceChannel) validateConnectionStatusHintEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, deviceChannelTypeConnectionStatusHintPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *DeviceChannel) validateConnectionStatusHint(formats strfmt.Registry) error {

	if swag.IsZero(m.ConnectionStatusHint) { // not required
		return nil
	}

	// value enum
	if err := m.validateConnectionStatusHintEnum("channel"+"."+"connectionStatusHint", "body", m.ConnectionStatusHint); err != nil {
		return err
	}

	return nil
}

func (m *DeviceChannel) validatePubsub(formats strfmt.Registry) error {

	if swag.IsZero(m.Pubsub) { // not required
		return nil
	}

	if m.Pubsub != nil {

		if err := m.Pubsub.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

var deviceChannelTypeSupportedTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["dev_null","gcm","gcp","parent","pubsub","pull","xmpp"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		deviceChannelTypeSupportedTypePropEnum = append(deviceChannelTypeSupportedTypePropEnum, v)
	}
}

const (
	deviceChannelSupportedTypeDevNull string = "dev_null"
	deviceChannelSupportedTypeGcm     string = "gcm"
	deviceChannelSupportedTypeGcp     string = "gcp"
	deviceChannelSupportedTypeParent  string = "parent"
	deviceChannelSupportedTypePubsub  string = "pubsub"
	deviceChannelSupportedTypePull    string = "pull"
	deviceChannelSupportedTypeXMPP    string = "xmpp"
)

// prop value enum
func (m *DeviceChannel) validateSupportedTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, deviceChannelTypeSupportedTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *DeviceChannel) validateSupportedType(formats strfmt.Registry) error {

	if swag.IsZero(m.SupportedType) { // not required
		return nil
	}

	// value enum
	if err := m.validateSupportedTypeEnum("channel"+"."+"supportedType", "body", m.SupportedType); err != nil {
		return err
	}

	return nil
}

// DeviceChannelPubsub device channel pubsub
// swagger:model DeviceChannelPubsub
type DeviceChannelPubsub struct {

	// Device's connection status, as set by the pubsub subscriber.
	ConnectionStatusHint string `json:"connectionStatusHint,omitempty"`

	// Pubsub topic to publish to with device notifications.
	Topic string `json:"topic,omitempty"`
}

// Validate validates this device channel pubsub
func (m *DeviceChannelPubsub) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateConnectionStatusHint(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var deviceChannelPubsubTypeConnectionStatusHintPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["offline","online","unknown"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		deviceChannelPubsubTypeConnectionStatusHintPropEnum = append(deviceChannelPubsubTypeConnectionStatusHintPropEnum, v)
	}
}

const (
	deviceChannelPubsubConnectionStatusHintOffline string = "offline"
	deviceChannelPubsubConnectionStatusHintOnline  string = "online"
	deviceChannelPubsubConnectionStatusHintUnknown string = "unknown"
)

// prop value enum
func (m *DeviceChannelPubsub) validateConnectionStatusHintEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, deviceChannelPubsubTypeConnectionStatusHintPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *DeviceChannelPubsub) validateConnectionStatusHint(formats strfmt.Registry) error {

	if swag.IsZero(m.ConnectionStatusHint) { // not required
		return nil
	}

	// value enum
	if err := m.validateConnectionStatusHintEnum("channel"+"."+"pubsub"+"."+"connectionStatusHint", "body", m.ConnectionStatusHint); err != nil {
		return err
	}

	return nil
}

// DeviceModelManifest Device model information provided by the model manifest of this device.
// swagger:model DeviceModelManifest
type DeviceModelManifest struct {

	// Device model name.
	ModelName string `json:"modelName,omitempty"`

	// Name of device model manufacturer.
	OemName string `json:"oemName,omitempty"`
}

// Validate validates this device model manifest
func (m *DeviceModelManifest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// DevicePersonalizedInfo Personalized device information for currently logged-in user.
// swagger:model DevicePersonalizedInfo
type DevicePersonalizedInfo struct {

	// Timestamp of the last device usage by the user in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// Personalized device location.
	Location string `json:"location,omitempty"`

	// The maximum role on the device.
	MaxRole string `json:"maxRole,omitempty"`

	// Personalized device display name.
	Name string `json:"name,omitempty"`
}

// Validate validates this device personalized info
func (m *DevicePersonalizedInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
