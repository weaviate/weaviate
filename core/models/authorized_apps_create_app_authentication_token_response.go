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

// AuthorizedAppsCreateAppAuthenticationTokenResponse Generate a token used to authenticate an authorized app.
// swagger:model AuthorizedAppsCreateAppAuthenticationTokenResponse
type AuthorizedAppsCreateAppAuthenticationTokenResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#authorizedAppsCreateAppAuthenticationTokenResponse".
	Kind *string `json:"kind,omitempty"`

	// Generated authentication token for an authorized app.
	Token string `json:"token,omitempty"`
}

// Validate validates this authorized apps create app authentication token response
func (m *AuthorizedAppsCreateAppAuthenticationTokenResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
