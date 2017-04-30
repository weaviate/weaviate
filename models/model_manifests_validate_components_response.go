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
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// ModelManifestsValidateComponentsResponse model manifests validate components response
// swagger:model ModelManifestsValidateComponentsResponse
type ModelManifestsValidateComponentsResponse struct {

	// Validation errors in component definitions.
	ValidationErrors []string `json:"validationErrors"`
}

// Validate validates this model manifests validate components response
func (m *ModelManifestsValidateComponentsResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateValidationErrors(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ModelManifestsValidateComponentsResponse) validateValidationErrors(formats strfmt.Registry) error {

	if swag.IsZero(m.ValidationErrors) { // not required
		return nil
	}

	return nil
}
