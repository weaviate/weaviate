/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// PeerAnnouncement Announcent of a peer on the network
// swagger:model PeerAnnouncement
type PeerAnnouncement struct {

	// Uuid of the network.
	// Format: uuid
	NetworkUUID strfmt.UUID `json:"networkUuid,omitempty"`

	// Voucher that allows access or not to the network.
	// Format: uuid
	NetworkVoucherUUID strfmt.UUID `json:"networkVoucherUuid,omitempty"`

	// Host or IP of the peer.
	// Format: hostname
	PeerHost strfmt.Hostname `json:"peerHost,omitempty"`

	// Name of the peer in readable format
	PeerName string `json:"peerName,omitempty"`

	// Uuid of the peer.
	// Format: uuid
	PeerUUID strfmt.UUID `json:"peerUuid,omitempty"`
}

// Validate validates this peer announcement
func (m *PeerAnnouncement) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateNetworkUUID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateNetworkVoucherUUID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validatePeerHost(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validatePeerUUID(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PeerAnnouncement) validateNetworkUUID(formats strfmt.Registry) error {

	if swag.IsZero(m.NetworkUUID) { // not required
		return nil
	}

	if err := validate.FormatOf("networkUuid", "body", "uuid", m.NetworkUUID.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *PeerAnnouncement) validateNetworkVoucherUUID(formats strfmt.Registry) error {

	if swag.IsZero(m.NetworkVoucherUUID) { // not required
		return nil
	}

	if err := validate.FormatOf("networkVoucherUuid", "body", "uuid", m.NetworkVoucherUUID.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *PeerAnnouncement) validatePeerHost(formats strfmt.Registry) error {

	if swag.IsZero(m.PeerHost) { // not required
		return nil
	}

	if err := validate.FormatOf("peerHost", "body", "hostname", m.PeerHost.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *PeerAnnouncement) validatePeerUUID(formats strfmt.Registry) error {

	if swag.IsZero(m.PeerUUID) { // not required
		return nil
	}

	if err := validate.FormatOf("peerUuid", "body", "uuid", m.PeerUUID.String(), formats); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *PeerAnnouncement) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *PeerAnnouncement) UnmarshalBinary(b []byte) error {
	var res PeerAnnouncement
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
