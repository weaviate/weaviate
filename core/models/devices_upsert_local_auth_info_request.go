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

// DevicesUpsertLocalAuthInfoRequest devices upsert local auth info request
// swagger:model DevicesUpsertLocalAuthInfoRequest
type DevicesUpsertLocalAuthInfoRequest struct {

	// The local auth info of the device.
	LocalAuthInfo *LocalAuthInfo `json:"localAuthInfo,omitempty"`
}

// Validate validates this devices upsert local auth info request
func (m *DevicesUpsertLocalAuthInfoRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocalAuthInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *DevicesUpsertLocalAuthInfoRequest) validateLocalAuthInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.LocalAuthInfo) { // not required
		return nil
	}

	if m.LocalAuthInfo != nil {

		if err := m.LocalAuthInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
