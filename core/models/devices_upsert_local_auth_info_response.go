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
)

// DevicesUpsertLocalAuthInfoResponse devices upsert local auth info response
// swagger:model DevicesUpsertLocalAuthInfoResponse
type DevicesUpsertLocalAuthInfoResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#devicesUpsertLocalAuthInfoResponse".
	Kind *string `json:"kind,omitempty"`

	// The non-secret local auth info.
	LocalAuthInfo interface{} `json:"localAuthInfo,omitempty"`
}

// Validate validates this devices upsert local auth info response
func (m *DevicesUpsertLocalAuthInfoResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
