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

// KeyCreate key create
// swagger:model KeyCreate
type KeyCreate struct {

	// Is user allowed to delete.
	Delete bool `json:"delete,omitempty"`

	// Email associated with this account.
	Email string `json:"email,omitempty"`

	// Is user allowed to execute.
	Execute bool `json:"execute,omitempty"`

	// Origin of the IP using CIDR notation.
	IPOrigin []string `json:"ipOrigin"`

	// Time as Unix timestamp that the key expires. Set to 0 for never.
	KeyExpiresUnix int64 `json:"keyExpiresUnix,omitempty"`

	// Is user allowed to read.
	Read bool `json:"read,omitempty"`

	// Is user allowed to write.
	Write bool `json:"write,omitempty"`
}

// Validate validates this key create
func (m *KeyCreate) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateIPOrigin(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *KeyCreate) validateIPOrigin(formats strfmt.Registry) error {

	if swag.IsZero(m.IPOrigin) { // not required
		return nil
	}

	return nil
}

// MarshalBinary interface implementation
func (m *KeyCreate) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *KeyCreate) UnmarshalBinary(b []byte) error {
	var res KeyCreate
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
