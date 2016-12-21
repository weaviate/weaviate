package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
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
