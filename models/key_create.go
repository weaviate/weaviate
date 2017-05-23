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
)

// KeyCreate key create
// swagger:model KeyCreate
type KeyCreate struct {

	// Is user allowed to delete.
	Delete bool `json:"delete,omitempty"`

	// Email associated with this account.
	Email string `json:"email,omitempty"`

	// Origin of the IP using CIDR notation.
	IPOrigin string `json:"ipOrigin,omitempty"`

	// Time as Unix timestamp that the key expires. Set to 0 for never.
	KeyExpiresUnix float64 `json:"keyExpiresUnix,omitempty"`

	// Is user allowed to read.
	Read bool `json:"read,omitempty"`

	// Is user allowed to write.
	Write bool `json:"write,omitempty"`
}

// Validate validates this key create
func (m *KeyCreate) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
