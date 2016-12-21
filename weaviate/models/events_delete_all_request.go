package models




import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// EventsDeleteAllRequest events delete all request
// swagger:model EventsDeleteAllRequest
type EventsDeleteAllRequest struct {

	// Device ID.
	DeviceID string `json:"deviceId,omitempty"`
}

// Validate validates this events delete all request
func (m *EventsDeleteAllRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
