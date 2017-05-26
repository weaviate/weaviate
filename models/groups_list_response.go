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
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package models




import (
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// GroupsListResponse groups list response
// swagger:model GroupsListResponse
type GroupsListResponse struct {

	// The list of groups.
	Groups []*GroupGetResponse `json:"groups"`
}

// Validate validates this groups list response
func (m *GroupsListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateGroups(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *GroupsListResponse) validateGroups(formats strfmt.Registry) error {

	if swag.IsZero(m.Groups) { // not required
		return nil
	}

	for i := 0; i < len(m.Groups); i++ {

		if swag.IsZero(m.Groups[i]) { // not required
			continue
		}

		if m.Groups[i] != nil {

			if err := m.Groups[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("groups" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}
