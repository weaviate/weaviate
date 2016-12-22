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

// PersonalizedInfo personalized info
// swagger:model PersonalizedInfo
type PersonalizedInfo struct {

	// Unique personalizedInfo ID. Value: the fixed string "me".
	ID *string `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#personalizedInfo".
	Kind *string `json:"kind,omitempty"`

	// Timestamp of the last device usage by the user in milliseconds since epoch UTC.
	LastUseTimeMs int64 `json:"lastUseTimeMs,omitempty"`

	// Personalized device location.
	Location string `json:"location,omitempty"`

	// Personalized device display name.
	Name string `json:"name,omitempty"`
}

// Validate validates this personalized info
func (m *PersonalizedInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
