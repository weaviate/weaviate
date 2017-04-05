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
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// AdaptersActivateResponse adapters activate response
// swagger:model AdaptersActivateResponse
type AdaptersActivateResponse struct {

	// An ID that represents the link between the adapter and the user. The activationUrl will give this ID to the adapter for use in the accept API.
	ActivationID string `json:"activationId,omitempty"`

	// A URL to the adapter where the user should go to complete the activation. The URL contains the activationId and the user's email address.
	ActivationURL string `json:"activationUrl,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#adaptersActivateResponse".
	Kind *string `json:"kind,omitempty"`
}

// Validate validates this adapters activate response
func (m *AdaptersActivateResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
