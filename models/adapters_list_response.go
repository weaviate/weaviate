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

// AdaptersListResponse adapters list response
// swagger:model AdaptersListResponse
type AdaptersListResponse struct {

	// The list of adapters.
	Adapters []*Adapter `json:"adapters"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#adaptersListResponse".
	Kind *string `json:"kind,omitempty"`
}

// Validate validates this adapters list response
func (m *AdaptersListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAdapters(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *AdaptersListResponse) validateAdapters(formats strfmt.Registry) error {

	if swag.IsZero(m.Adapters) { // not required
		return nil
	}

	for i := 0; i < len(m.Adapters); i++ {

		if swag.IsZero(m.Adapters[i]) { // not required
			continue
		}

		if m.Adapters[i] != nil {

			if err := m.Adapters[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("adapters" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}
