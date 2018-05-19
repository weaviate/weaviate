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

package models

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// PeerAnnouncement Announcent of a peer on the network
// swagger:model PeerAnnouncement

type PeerAnnouncement struct {

	// Uuid of the network.
	NetworkUUID strfmt.UUID `json:"networkUuid,omitempty"`

	// Voucher that allows access or not to the network.
	NetworkVoucherUUID strfmt.UUID `json:"networkVoucherUuid,omitempty"`

	// Host or IP of the peer.
	PeerHost strfmt.Hostname `json:"peerHost,omitempty"`

	// Name of the peer in readable format
	PeerName string `json:"peerName,omitempty"`

	// Uuid of the peer.
	PeerUUID strfmt.UUID `json:"peerUuid,omitempty"`
}

/* polymorph PeerAnnouncement networkUuid false */

/* polymorph PeerAnnouncement networkVoucherUuid false */

/* polymorph PeerAnnouncement peerHost false */

/* polymorph PeerAnnouncement peerName false */

/* polymorph PeerAnnouncement peerUuid false */

// Validate validates this peer announcement
func (m *PeerAnnouncement) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
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
