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

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// Invitation invitation
// swagger:model Invitation
type Invitation struct {

	// acl entry
	ACLEntry *ACLEntry `json:"aclEntry,omitempty"`

	// Email of a user who created this invitation.
	CreatorEmail string `json:"creatorEmail,omitempty"`
}

// Validate validates this invitation
func (m *Invitation) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateACLEntry(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Invitation) validateACLEntry(formats strfmt.Registry) error {

	if swag.IsZero(m.ACLEntry) { // not required
		return nil
	}

	if m.ACLEntry != nil {

		if err := m.ACLEntry.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("aclEntry")
			}
			return err
		}
	}

	return nil
}
