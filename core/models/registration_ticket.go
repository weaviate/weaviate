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
	"github.com/go-openapi/errors"
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// RegistrationTicket registration ticket
// swagger:model RegistrationTicket
type RegistrationTicket struct {

	// Creation timestamp of the registration ticket in milliseconds since epoch UTC.
	CreationTimeMs int64 `json:"creationTimeMs,omitempty"`

	// Draft of the device being registered.
	DeviceDraft *Device `json:"deviceDraft,omitempty"`

	// ID that device will have after registration is successfully finished.
	DeviceID string `json:"deviceId,omitempty"`

	// Error code. Set only on device registration failures.
	ErrorCode string `json:"errorCode,omitempty"`

	// Expiration timestamp of the registration ticket in milliseconds since epoch UTC.
	ExpirationTimeMs int64 `json:"expirationTimeMs,omitempty"`

	// Registration ticket ID.
	ID string `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#registrationTicket".
	Kind *string `json:"kind,omitempty"`

	// OAuth 2.0 client ID of the device.
	OauthClientID string `json:"oauthClientId,omitempty"`

	// Parent device ID (aggregator) if it exists.
	ParentID string `json:"parentId,omitempty"`

	// Authorization code that can be exchanged to a refresh token.
	RobotAccountAuthorizationCode string `json:"robotAccountAuthorizationCode,omitempty"`

	// E-mail address of robot account assigned to the registered device.
	RobotAccountEmail string `json:"robotAccountEmail,omitempty"`

	// Email address of the owner.
	UserEmail string `json:"userEmail,omitempty"`
}

// Validate validates this registration ticket
func (m *RegistrationTicket) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateDeviceDraft(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *RegistrationTicket) validateDeviceDraft(formats strfmt.Registry) error {

	if swag.IsZero(m.DeviceDraft) { // not required
		return nil
	}

	if m.DeviceDraft != nil {

		if err := m.DeviceDraft.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
