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

// PlacesListSuggestionsResponse places list suggestions response
// swagger:model PlacesListSuggestionsResponse
type PlacesListSuggestionsResponse struct {

	// Suggestions for places and rooms for a device.
	DeviceSuggestions []*DeviceSuggestion `json:"deviceSuggestions"`
}

// Validate validates this places list suggestions response
func (m *PlacesListSuggestionsResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateDeviceSuggestions(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PlacesListSuggestionsResponse) validateDeviceSuggestions(formats strfmt.Registry) error {

	if swag.IsZero(m.DeviceSuggestions) { // not required
		return nil
	}

	for i := 0; i < len(m.DeviceSuggestions); i++ {

		if swag.IsZero(m.DeviceSuggestions[i]) { // not required
			continue
		}

		if m.DeviceSuggestions[i] != nil {

			if err := m.DeviceSuggestions[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
