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
	"github.com/go-openapi/swag"
)

// SubscriptionsListResponse List of subscriptions.
// swagger:model SubscriptionsListResponse
type SubscriptionsListResponse struct {

	// Identifies what kind of resource this is. Value: the fixed string "weave#subscriptionsListResponse".
	Kind *string `json:"kind,omitempty"`

	// The list of subscriptions.
	Subscriptions []*Subscription `json:"subscriptions"`
}

// Validate validates this subscriptions list response
func (m *SubscriptionsListResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateSubscriptions(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *SubscriptionsListResponse) validateSubscriptions(formats strfmt.Registry) error {

	if swag.IsZero(m.Subscriptions) { // not required
		return nil
	}

	for i := 0; i < len(m.Subscriptions); i++ {

		if swag.IsZero(m.Subscriptions[i]) { // not required
			continue
		}

		if m.Subscriptions[i] != nil {

			if err := m.Subscriptions[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
