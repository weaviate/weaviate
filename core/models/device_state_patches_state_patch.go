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
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// DeviceStatePatchesStatePatch Device state patch with corresponding timestamp.
// swagger:model DeviceStatePatchesStatePatch
type DeviceStatePatchesStatePatch struct {

	// Component name paths separated by '/'.
	Component string `json:"component,omitempty"`

	// State patch.
	Patch JSONObject `json:"patch,omitempty"`

	// Timestamp of a change. Local time, UNIX timestamp or time since last boot can be used.
	TimeMs int64 `json:"timeMs,omitempty"`
}

// Validate validates this device state patches state patch
func (m *DeviceStatePatchesStatePatch) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
