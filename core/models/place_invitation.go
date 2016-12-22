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

// PlaceInvitation place invitation
// swagger:model PlaceInvitation
type PlaceInvitation struct {

	// member
	Member *PlaceMember `json:"member,omitempty"`

	// place Id
	PlaceID string `json:"placeId,omitempty"`
}

// Validate validates this place invitation
func (m *PlaceInvitation) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateMember(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PlaceInvitation) validateMember(formats strfmt.Registry) error {

	if swag.IsZero(m.Member) { // not required
		return nil
	}

	if m.Member != nil {

		if err := m.Member.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
