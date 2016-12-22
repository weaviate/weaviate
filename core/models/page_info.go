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
)

// PageInfo page info
// swagger:model PageInfo
type PageInfo struct {

	// result per page
	ResultPerPage int32 `json:"resultPerPage,omitempty"`

	// start index
	StartIndex int32 `json:"startIndex,omitempty"`

	// total results
	TotalResults int32 `json:"totalResults,omitempty"`
}

// Validate validates this page info
func (m *PageInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
