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

// PlaceSuggestion place suggestion
// swagger:model PlaceSuggestion
type PlaceSuggestion struct {

	// place Id
	PlaceID string `json:"placeId,omitempty"`

	// place name
	PlaceName string `json:"placeName,omitempty"`

	// room suggestion
	RoomSuggestion []*RoomSuggestion `json:"roomSuggestion"`
}

// Validate validates this place suggestion
func (m *PlaceSuggestion) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateRoomSuggestion(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PlaceSuggestion) validateRoomSuggestion(formats strfmt.Registry) error {

	if swag.IsZero(m.RoomSuggestion) { // not required
		return nil
	}

	for i := 0; i < len(m.RoomSuggestion); i++ {

		if swag.IsZero(m.RoomSuggestion[i]) { // not required
			continue
		}

		if m.RoomSuggestion[i] != nil {

			if err := m.RoomSuggestion[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
