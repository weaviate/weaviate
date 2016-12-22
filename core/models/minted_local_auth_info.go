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

// MintedLocalAuthInfo minted local auth info
// swagger:model MintedLocalAuthInfo
type MintedLocalAuthInfo struct {

	// client token
	ClientToken string `json:"clientToken,omitempty"`

	// device Id
	DeviceID string `json:"deviceId,omitempty"`

	// device token
	DeviceToken string `json:"deviceToken,omitempty"`

	// local access info
	LocalAccessInfo *LocalAccessInfo `json:"localAccessInfo,omitempty"`

	// retry after
	RetryAfter int64 `json:"retryAfter,omitempty"`
}

// Validate validates this minted local auth info
func (m *MintedLocalAuthInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocalAccessInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *MintedLocalAuthInfo) validateLocalAccessInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.LocalAccessInfo) { // not required
		return nil
	}

	if m.LocalAccessInfo != nil {

		if err := m.LocalAccessInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
