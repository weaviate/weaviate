package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
)

// DevicesSetRoomRequest devices set room request
// swagger:model DevicesSetRoomRequest
type DevicesSetRoomRequest struct {

	// room Id
	RoomID string `json:"roomId,omitempty"`
}

// Validate validates this devices set room request
func (m *DevicesSetRoomRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
