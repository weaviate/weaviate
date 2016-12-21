package models


// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
)

// DevicesAddToPlaceRequest devices add to place request
// swagger:model DevicesAddToPlaceRequest
type DevicesAddToPlaceRequest struct {

	// place Id
	PlaceID string `json:"placeId,omitempty"`

	// room Id
	RoomID string `json:"roomId,omitempty"`

	// share with all members
	ShareWithAllMembers bool `json:"shareWithAllMembers,omitempty"`

	// share with member
	ShareWithMember []string `json:"shareWithMember"`
}

// Validate validates this devices add to place request
func (m *DevicesAddToPlaceRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateShareWithMember(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *DevicesAddToPlaceRequest) validateShareWithMember(formats strfmt.Registry) error {

	if swag.IsZero(m.ShareWithMember) { // not required
		return nil
	}

	return nil
}
