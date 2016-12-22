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

// PlacesCreateResponse places create response
// swagger:model PlacesCreateResponse
type PlacesCreateResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#placesCreateResponse".
	Kind *string `json:"kind,omitempty"`

	// Newly created place.
	Place *Place `json:"place,omitempty"`
}

// Validate validates this places create response
func (m *PlacesCreateResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validatePlace(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PlacesCreateResponse) validatePlace(formats strfmt.Registry) error {

	if swag.IsZero(m.Place) { // not required
		return nil
	}

	if m.Place != nil {

		if err := m.Place.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
