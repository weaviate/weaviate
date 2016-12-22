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

// PlacesListResponse places list response
// swagger:model PlacesListResponse
type PlacesListResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#placesListResponse".
	Kind *string `json:"kind,omitempty"`

	// page info
	PageInfo *PageInfo `json:"pageInfo,omitempty"`

	// The list of places (homes).
	Places []*Place `json:"places"`

	// token pagination
	TokenPagination *TokenPagination `json:"tokenPagination,omitempty"`
}

// Validate validates this places list response
func (m *PlacesListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validatePageInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validatePlaces(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateTokenPagination(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PlacesListResponse) validatePageInfo(formats strfmt.Registry) error {

	if swag.IsZero(m.PageInfo) { // not required
		return nil
	}

	if m.PageInfo != nil {

		if err := m.PageInfo.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}

func (m *PlacesListResponse) validatePlaces(formats strfmt.Registry) error {

	if swag.IsZero(m.Places) { // not required
		return nil
	}

	for i := 0; i < len(m.Places); i++ {

		if swag.IsZero(m.Places[i]) { // not required
			continue
		}

		if m.Places[i] != nil {

			if err := m.Places[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}

func (m *PlacesListResponse) validateTokenPagination(formats strfmt.Registry) error {

	if swag.IsZero(m.TokenPagination) { // not required
		return nil
	}

	if m.TokenPagination != nil {

		if err := m.TokenPagination.Validate(formats); err != nil {
			return err
		}
	}

	return nil
}
