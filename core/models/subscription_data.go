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
	"encoding/json"
	"strconv"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// SubscriptionData Subscription template.
// swagger:model SubscriptionData
type SubscriptionData struct {

	// Timestamp in milliseconds since epoch when the subscription expires and new notifications stop being sent.
	ExpirationTimeMs int64 `json:"expirationTimeMs,omitempty"`

	// Subscription event filter.
	//
	// Acceptable values are:
	// - "myDevices"
	// - "myCommands"
	Filters []string `json:"filters"`

	// GCM registration ID.
	GcmRegistrationID string `json:"gcmRegistrationId,omitempty"`

	// For Chrome apps must be the same as sender ID during registration, usually API project ID.
	GcmSenderID string `json:"gcmSenderId,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#oldSubscription".
	Kind *string `json:"kind,omitempty"`
}

// Validate validates this subscription data
func (m *SubscriptionData) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateFilters(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var subscriptionDataFiltersItemsEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["myCommands","myDevices"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		subscriptionDataFiltersItemsEnum = append(subscriptionDataFiltersItemsEnum, v)
	}
}

func (m *SubscriptionData) validateFiltersItemsEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, subscriptionDataFiltersItemsEnum); err != nil {
		return err
	}
	return nil
}

func (m *SubscriptionData) validateFilters(formats strfmt.Registry) error {

	if swag.IsZero(m.Filters) { // not required
		return nil
	}

	for i := 0; i < len(m.Filters); i++ {

		// value enum
		if err := m.validateFiltersItemsEnum("filters"+"."+strconv.Itoa(i), "body", m.Filters[i]); err != nil {
			return err
		}

	}

	return nil
}
