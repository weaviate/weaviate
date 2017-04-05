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

	strfmt "github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// PlacesHints places hints
// swagger:model PlacesHints
type PlacesHints struct {

	// place name
	PlaceName string `json:"placeName,omitempty"`

	// room name
	RoomName string `json:"roomName,omitempty"`

	// room type
	RoomType string `json:"roomType,omitempty"`
}

// Validate validates this places hints
func (m *PlacesHints) Validate(formats strfmt.Registry) error {
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

var placesHintsTypeRoomTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["attic","backyard","basement","bathroom","bedroom","default","den","diningRoom","entryway","familyRoom","frontyard","garage","hallway","kitchen","livingRoom","masterBedroom","office","other","shed","unknownRoomType"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		placesHintsTypeRoomTypePropEnum = append(placesHintsTypeRoomTypePropEnum, v)
	}
}

const (
	placesHintsRoomTypeAttic           string = "attic"
	placesHintsRoomTypeBackyard        string = "backyard"
	placesHintsRoomTypeBasement        string = "basement"
	placesHintsRoomTypeBathroom        string = "bathroom"
	placesHintsRoomTypeBedroom         string = "bedroom"
	placesHintsRoomTypeDefault         string = "default"
	placesHintsRoomTypeDen             string = "den"
	placesHintsRoomTypeDiningRoom      string = "diningRoom"
	placesHintsRoomTypeEntryway        string = "entryway"
	placesHintsRoomTypeFamilyRoom      string = "familyRoom"
	placesHintsRoomTypeFrontyard       string = "frontyard"
	placesHintsRoomTypeGarage          string = "garage"
	placesHintsRoomTypeHallway         string = "hallway"
	placesHintsRoomTypeKitchen         string = "kitchen"
	placesHintsRoomTypeLivingRoom      string = "livingRoom"
	placesHintsRoomTypeMasterBedroom   string = "masterBedroom"
	placesHintsRoomTypeOffice          string = "office"
	placesHintsRoomTypeOther           string = "other"
	placesHintsRoomTypeShed            string = "shed"
	placesHintsRoomTypeUnknownRoomType string = "unknownRoomType"
)

// prop value enum
func (m *PlacesHints) validateRoomTypeEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, placesHintsTypeRoomTypePropEnum); err != nil {
		return err
	}
	return nil
}

func (m *PlacesHints) validateRoomType(formats strfmt.Registry) error {

	if swag.IsZero(m.RoomType) { // not required
		return nil
	}

	// value enum
	if err := m.validateRoomTypeEnum("roomType", "body", m.RoomType); err != nil {
		return err
	}

	return nil
}
