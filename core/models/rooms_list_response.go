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

// RoomsListResponse rooms list response
// swagger:model RoomsListResponse
type RoomsListResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#roomsListResponse".
	Kind *string `json:"kind,omitempty"`

	// page info
	PageInfo *PageInfo `json:"pageInfo,omitempty"`

	// The list of rooms.
	Rooms []*Room `json:"rooms"`

	// token pagination
	TokenPagination *TokenPagination `json:"tokenPagination,omitempty"`
}

// Validate validates this rooms list response
func (m *RoomsListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validatePageInfo(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateRooms(formats); err != nil {
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

func (m *RoomsListResponse) validatePageInfo(formats strfmt.Registry) error {

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

func (m *RoomsListResponse) validateRooms(formats strfmt.Registry) error {

	if swag.IsZero(m.Rooms) { // not required
		return nil
	}

	for i := 0; i < len(m.Rooms); i++ {

		if swag.IsZero(m.Rooms[i]) { // not required
			continue
		}

		if m.Rooms[i] != nil {

			if err := m.Rooms[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}

func (m *RoomsListResponse) validateTokenPagination(formats strfmt.Registry) error {

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
