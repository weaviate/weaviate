package models




import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// ClientNotification Client notification template.
// swagger:model ClientNotification
type ClientNotification struct {

	// The list of events.
	Events []*Event `json:"events"`

	// ID of the notification. This id is generated sequentially starting from 0 within a subscription.
	ID int64 `json:"id,omitempty"`

	// Identifies what kind of resource this is. Value: the fixed string "weave#clientNotification".
	Kind *string `json:"kind,omitempty"`

	// The subscription for which this notification is sent for.
	SubscriptionID string `json:"subscriptionId,omitempty"`
}

// Validate validates this client notification
func (m *ClientNotification) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateEvents(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ClientNotification) validateEvents(formats strfmt.Registry) error {

	if swag.IsZero(m.Events) { // not required
		return nil
	}

	for i := 0; i < len(m.Events); i++ {

		if swag.IsZero(m.Events[i]) { // not required
			continue
		}

		if m.Events[i] != nil {

			if err := m.Events[i].Validate(formats); err != nil {
				return err
			}
		}

	}

	return nil
}
