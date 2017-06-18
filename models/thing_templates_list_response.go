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

// ThingTemplatesListResponse List of model manifests.
// swagger:model ThingTemplatesListResponse
type ThingTemplatesListResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weaviate#thingTemplatesListResponse".
	Kind *string `json:"kind,omitempty"`

	// The actual list of model manifests.
	ThingTemplates []*ThingTemplateGetResponse `json:"thingTemplates"`

	// The total number of model manifests for the query. The number of items in a response may be smaller due to paging.
	TotalResults int64 `json:"totalResults,omitempty"`
}

// Validate validates this thing templates list response
func (m *ThingTemplatesListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateThingTemplates(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ThingTemplatesListResponse) validateThingTemplates(formats strfmt.Registry) error {

	if swag.IsZero(m.ThingTemplates) { // not required
		return nil
	}

	for i := 0; i < len(m.ThingTemplates); i++ {

		if swag.IsZero(m.ThingTemplates[i]) { // not required
			continue
		}

		if m.ThingTemplates[i] != nil {

			if err := m.ThingTemplates[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("thingTemplates" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}
