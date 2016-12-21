package models




import (
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// RoomSuggestion room suggestion
// swagger:model RoomSuggestion
type RoomSuggestion struct {

	// room Id
	RoomID string `json:"roomId,omitempty"`

	// room name
	RoomName string `json:"roomName,omitempty"`

	// room type
	RoomType string `json:"roomType,omitempty"`
}

// Validate validates this room suggestion
func (m *RoomSuggestion) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateRoomType(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var roomSuggestionTypeRoomTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["attic","backyard","basement","bathroom","bedroom","default","den","diningRoom","entryway","familyRoom","frontyard","garage","hallway","kitchen","livingRoom","masterBedroom","office","shed","unknownRoomType"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		roomSuggestionTypeRoomTypePropEnum = append(roomSuggestionTypeRoomTypePropEnum, v)
	}
}

const (
	roomSuggestionRoomTypeAttic           string = "attic"
	roomSuggestionRoomTypeBackyard        string = "backyard"
	roomSuggestionRoomTypeBasement        string = "basement"
	roomSuggestionRoomTypeBathroom        string = "bathroom"
	roomSuggestionRoomTypeBedroom         string = "bedroom"
	roomSuggestionRoomTypeDefault         string = "default"
	roomSuggestionRoomTypeDen             string = "den"
	roomSuggestionRoomTypeDiningRoom      string = "diningRoom"
	roomSuggestionRoomTypeEntryway        string = "entryway"
	roomSuggestionRoomTypeFamilyRoom      string = "familyRoom"
	roomSuggestionRoomTypeFrontyard       string = "frontyard"
	roomSuggestionRoomTypeGarage          string = "garage"
	roomSuggestionRoomTypeHallway         string = "hallway"
	roomSuggestionRoomTypeKitchen         string = "kitchen"
	roomSuggestionRoomTypeLivingRoom      string = "livingRoom"
	roomSuggestionRoomTypeMasterBedroom   string = "masterBedroom"
	roomSuggestionRoomTypeOffice          string = "office"
	roomSuggestionRoomTypeShed            string = "shed"
	roomSuggestionRoomTypeUnknownRoomType string = "unknownRoomType"
)

// prop value enum
func (m *RoomSuggestion) validateRoomTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, roomSuggestionTypeRoomTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *RoomSuggestion) validateRoomType(formats strfmt.Registry) error {

	if swag.IsZero(m.RoomType) { // not required
		return nil
	}

	// value enum
	if err := m.validateRoomTypeEnum("roomType", "body", m.RoomType); err != nil {
		return err
	}

	return nil
}
