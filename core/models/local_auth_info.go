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

// LocalAuthInfo local auth info
// swagger:model LocalAuthInfo
type LocalAuthInfo struct {

	// cert fingerprint
	CertFingerprint string `json:"certFingerprint,omitempty"`

	// client token
	ClientToken string `json:"clientToken,omitempty"`

	// device token
	DeviceToken string `json:"deviceToken,omitempty"`

	// local Id
	LocalID string `json:"localId,omitempty"`

	// public local auth info
	PublicLocalAuthInfo *PublicLocalAuthInfo `json:"publicLocalAuthInfo,omitempty"`

	// revocation client token
	RevocationClientToken string `json:"revocationClientToken,omitempty"`
}

// Validate validates this local auth info
func (m *LocalAuthInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validatePublicLocalAuthInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *LocalAuthInfo) validatePublicLocalAuthInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.PublicLocalAuthInfo) { // not required
		return nil
	}

	if m.PublicLocalAuthInfo != nil {

		if err := m.PublicLocalAuthInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
