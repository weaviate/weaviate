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

// Place place
// swagger:model Place
type Place struct {

	// id
	ID string `json:"id,omitempty"`

	// invitation
	Invitation *PlaceInvitation `json:"invitation,omitempty"`

	// member
	Member []*PlaceMember `json:"member"`

	// name
	Name string `json:"name,omitempty"`
}

// Validate validates this place
func (m *Place) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateInvitation(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateMember(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Place) validateInvitation(formats strfmt.Registry) error {

	if swag.IsZero(m.Invitation) { // not required
		return nil
	}

	if m.Invitation != nil {

		if err := m.Invitation.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

func (m *Place) validateMember(formats strfmt.Registry) error {

	if swag.IsZero(m.Member) { // not required
		return nil
	}

	for i := 0; i < len(m.Member); i++ {

		if swag.IsZero(m.Member[i]) { // not required
			continue
		}

		if m.Member[i] != nil {

			if err := m.Member[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
