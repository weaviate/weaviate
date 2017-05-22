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

// Thing thing
// swagger:model Thing
type Thing struct {

	// ID of the adapter that created this thing.
	AdapterID string `json:"adapterId,omitempty"`

	// Deprecated, do not use. The HTTPS certificate fingerprint used to secure communication with thing..
	CertFingerprint string `json:"certFingerprint,omitempty"`

	// channel
	Channel *ThingChannel `json:"channel,omitempty"`

	// Deprecated, use "traits" instead. Description of commands supported by the thing. This field is writable only by things.
	CommandDefs map[string]PackageDef `json:"commandDefs,omitempty"`

	// components
	Components JSONObject `json:"components,omitempty"`

	// Thing connection status.
	ConnectionStatus string `json:"connectionStatus,omitempty"`

	// Timestamp of creation of this thing in milliseconds since epoch UTC.
	CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

	// User readable description of this thing.
	Description string `json:"description,omitempty"`

	// The list of groups.
	Groups []*Group `json:"groups"`

	// Unique thing ID.
	ID string `json:"id,omitempty"`

	// List of pending invitations for the currently logged-in user.
	Invitations []*Invitation `json:"invitations"`

	// Indicates whether event recording is enabled or disabled for this thing.
	IsEventRecordingDisabled bool `json:"isEventRecordingDisabled,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#thing".
	Kind *string `json:"kind,omitempty"`

	// Any labels attached to the thing. Use the addLabel and removeLabel APIs to modify this list.
	Labels []*AssociatedLabel `json:"labels"`

	// Timestamp of the last request from this thing in milliseconds since epoch UTC. Supported only for things with XMPP channel type.
	LastSeenTimeMs int64 `json:"lastSeenTimeMs,omitempty"`

	// Timestamp of the last thing update in milliseconds since epoch UTC.
	LastUpdateTimeMs int64 `json:"lastUpdateTimeMs,omitempty"`

	// Timestamp of the last thing usage in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// ID of the location of this thing.
	LocationID int64 `json:"locationId,omitempty"`

	// model manifest
	ModelManifest *ThingModelManifest `json:"modelManifest,omitempty"`

	// Model manifest ID of this thing.
	ModelManifestID string `json:"modelManifestId,omitempty"`

	// Name of this thing provided by the manufacturer.
	Name string `json:"name,omitempty"`

	// Nicknames of the thing. Use the addNickname and removeNickname APIs to modify this list.
	Nicknames []string `json:"nicknames"`

	// E-mail address of the thing owner.
	Owner string `json:"owner,omitempty"`

	// personalized info
	PersonalizedInfo *ThingPersonalizedInfo `json:"personalizedInfo,omitempty"`

	// Serial number of a thing provided by its manufacturer.
	SerialNumber string `json:"serialNumber,omitempty"`

	// state
	State JSONObject `json:"state,omitempty"`

	// Deprecated, do not use. Description of the thing state. This field is writable only by things.
	StateDefs map[string]StateDef `json:"stateDefs,omitempty"`

	// Custom free-form manufacturer tags.
	Tags []string `json:"tags"`

	// Thing kind. Deprecated, provide "modelManifestId" instead. See list of thing kinds values.
	ThingKind string `json:"thingKind,omitempty"`

	// Deprecated, do not use. The ID of the thing for use on the local network.
	ThingLocalID string `json:"thingLocalId,omitempty"`

	// traits
	Traits JSONObject `json:"traits,omitempty"`

	// Thing kind from the model manifest used in UI applications. See list of thing kinds values.
	UIThingKind string `json:"uiThingKind,omitempty"`
}

// Validate validates this thing
func (m *Thing) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateChannel(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateGroups(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateInvitations(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateLabels(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateModelManifest(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateNicknames(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePersonalizedInfo(formats); err != nil {
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

func (m *Thing) validateChannel(formats strfmt.Registry) error {

	if swag.IsZero(m.Channel) { // not required
		return nil
	}

	if m.Channel != nil {

		if err := m.Channel.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("channel")
			}
			return err
		}
	}

	return nil
}

func (m *Thing) validateGroups(formats strfmt.Registry) error {

	if swag.IsZero(m.Groups) { // not required
		return nil
	}

	for i := 0; i < len(m.Groups); i++ {

		if swag.IsZero(m.Groups[i]) { // not required
			continue
		}

		if m.Groups[i] != nil {

			if err := m.Groups[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("groups" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *Thing) validateInvitations(formats strfmt.Registry) error {

	if swag.IsZero(m.Invitations) { // not required
		return nil
	}

	for i := 0; i < len(m.Invitations); i++ {

		if swag.IsZero(m.Invitations[i]) { // not required
			continue
		}

		if m.Invitations[i] != nil {

			if err := m.Invitations[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("invitations" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *Thing) validateLabels(formats strfmt.Registry) error {

	if swag.IsZero(m.Labels) { // not required
		return nil
	}

	for i := 0; i < len(m.Labels); i++ {

		if swag.IsZero(m.Labels[i]) { // not required
			continue
		}

		if m.Labels[i] != nil {

			if err := m.Labels[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("labels" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *Thing) validateModelManifest(formats strfmt.Registry) error {

	if swag.IsZero(m.ModelManifest) { // not required
		return nil
	}

	if m.ModelManifest != nil {

		if err := m.ModelManifest.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("modelManifest")
			}
			return err
		}
	}

	return nil
}

func (m *Thing) validateNicknames(formats strfmt.Registry) error {

	if swag.IsZero(m.Nicknames) { // not required
		return nil
	}

	return nil
}

func (m *Thing) validatePersonalizedInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.PersonalizedInfo) { // not required
		return nil
	}

	if m.PersonalizedInfo != nil {

		if err := m.PersonalizedInfo.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("personalizedInfo")
			}
			return err
		}
	}

	return nil
}

func (m *Thing) validateTags(formats strfmt.Registry) error {

	if swag.IsZero(m.Tags) { // not required
		return nil
	}

	return nil
}

// ThingChannel Thing notification channel description.
// swagger:model ThingChannel
type ThingChannel struct {

	// Connection status hint, set by parent thing.
	ConnectionStatusHint string `json:"connectionStatusHint,omitempty"`

	// GCM registration ID. Required if thing supports GCM delivery channel.
	GcmRegistrationID string `json:"gcmRegistrationId,omitempty"`

	// GCM sender ID. For Chrome apps must be the same as sender ID during registration, usually API project ID.
	GcmSenderID string `json:"gcmSenderId,omitempty"`

	// Parent thing ID (aggregator) if it exists.
	ParentID string `json:"parentId,omitempty"`

	// pubsub
	Pubsub *ThingChannelPubsub `json:"pubsub,omitempty"`

	// Channel type supported by thing. Allowed types are: "gcm", "xmpp", "pubsub", and "parent".
	SupportedType string `json:"supportedType,omitempty"`
}

// Validate validates this thing channel
func (m *ThingChannel) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateConnectionStatusHint(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePubsub(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var thingChannelTypeConnectionStatusHintPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["offline","online","unknown"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		thingChannelTypeConnectionStatusHintPropEnum = append(thingChannelTypeConnectionStatusHintPropEnum, v)
	}
}

const (
	// ThingChannelConnectionStatusHintOffline captures enum value "offline"
	ThingChannelConnectionStatusHintOffline string = "offline"
	// ThingChannelConnectionStatusHintOnline captures enum value "online"
	ThingChannelConnectionStatusHintOnline string = "online"
	// ThingChannelConnectionStatusHintUnknown captures enum value "unknown"
	ThingChannelConnectionStatusHintUnknown string = "unknown"
)

// prop value enum
func (m *ThingChannel) validateConnectionStatusHintEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, thingChannelTypeConnectionStatusHintPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *ThingChannel) validateConnectionStatusHint(formats strfmt.Registry) error {

	if swag.IsZero(m.ConnectionStatusHint) { // not required
		return nil
	}

	// value enum
	if err := m.validateConnectionStatusHintEnum("channel"+"."+"connectionStatusHint", "body", m.ConnectionStatusHint); err != nil {
		return err
	}

	return nil
}

func (m *ThingChannel) validatePubsub(formats strfmt.Registry) error {

	if swag.IsZero(m.Pubsub) { // not required
		return nil
	}

	if m.Pubsub != nil {

		if err := m.Pubsub.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("channel" + "." + "pubsub")
			}
			return err
		}
	}

	return nil
}

// ThingChannelPubsub Pubsub channel details.
// swagger:model ThingChannelPubsub
type ThingChannelPubsub struct {

	// Thing's connection status, as set by the pubsub subscriber.
	ConnectionStatusHint string `json:"connectionStatusHint,omitempty"`

	// Pubsub topic to publish to with thing notifications.
	Topic string `json:"topic,omitempty"`
}

// Validate validates this thing channel pubsub
func (m *ThingChannelPubsub) Validate(formats strfmt.Registry) error {
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

var thingChannelPubsubTypeConnectionStatusHintPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["offline","online","unknown"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		thingChannelPubsubTypeConnectionStatusHintPropEnum = append(thingChannelPubsubTypeConnectionStatusHintPropEnum, v)
	}
}

const (
	// ThingChannelPubsubConnectionStatusHintOffline captures enum value "offline"
	ThingChannelPubsubConnectionStatusHintOffline string = "offline"
	// ThingChannelPubsubConnectionStatusHintOnline captures enum value "online"
	ThingChannelPubsubConnectionStatusHintOnline string = "online"
	// ThingChannelPubsubConnectionStatusHintUnknown captures enum value "unknown"
	ThingChannelPubsubConnectionStatusHintUnknown string = "unknown"
)

// prop value enum
func (m *ThingChannelPubsub) validateConnectionStatusHintEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, thingChannelPubsubTypeConnectionStatusHintPropEnum); err != nil {
		return err
	}
	return nil
}

func (m *ThingChannelPubsub) validateConnectionStatusHint(formats strfmt.Registry) error {

	if swag.IsZero(m.ConnectionStatusHint) { // not required
		return nil
	}

	// value enum
	if err := m.validateConnectionStatusHintEnum("channel"+"."+"pubsub"+"."+"connectionStatusHint", "body", m.ConnectionStatusHint); err != nil {
		return err
	}

	return nil
}

// ThingModelManifest Thing model information provided by the model manifest of this thing.
// swagger:model ThingModelManifest
type ThingModelManifest struct {

	// Thing model name.
	ModelName string `json:"modelName,omitempty"`

	// Name of thing model manufacturer.
	OemName string `json:"oemName,omitempty"`
}

// Validate validates this thing model manifest
func (m *ThingModelManifest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// ThingPersonalizedInfo Personalized thing information for currently logged-in user.
// swagger:model ThingPersonalizedInfo
type ThingPersonalizedInfo struct {

	// Timestamp of the last thing usage by the user in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// Personalized thing location.
	Location string `json:"location,omitempty"`

	// The maximum role on the thing.
	MaxRole string `json:"maxRole,omitempty"`

	// Personalized thing display name.
	Name string `json:"name,omitempty"`
}

// Validate validates this thing personalized info
func (m *ThingPersonalizedInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
