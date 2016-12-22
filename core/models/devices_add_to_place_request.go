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

// DevicesAddToPlaceRequest devices add to place request
// swagger:model DevicesAddToPlaceRequest
type DevicesAddToPlaceRequest struct {

	// place Id
	PlaceID string `json:"placeId,omitempty"`

	// room Id
	RoomID string `json:"roomId,omitempty"`

	// share with all members
	ShareWithAllMembers bool `json:"shareWithAllMembers,omitempty"`

	// share with member
	ShareWithMember []string `json:"shareWithMember"`
}

// Validate validates this devices add to place request
func (m *DevicesAddToPlaceRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateShareWithMember(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *DevicesAddToPlaceRequest) validateShareWithMember(formats strfmt.Registry) error {

	if swag.IsZero(m.ShareWithMember) { // not required
		return nil
	}

	return nil
}
