package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// EventsRecordDeviceEventsRequest events record device events request
// swagger:model EventsRecordDeviceEventsRequest
type EventsRecordDeviceEventsRequest struct {

	// Device ID.
	DeviceID string `json:"deviceId,omitempty"`

	// Flag to indicate whether recording should be enabled or disabled.
	RecordDeviceEvents bool `json:"recordDeviceEvents,omitempty"`
}

// Validate validates this events record device events request
func (m *EventsRecordDeviceEventsRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
