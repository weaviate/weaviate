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
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
package models

import (
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
)

// Location Location on the world (inspired by Google Maps).
// swagger:model Location
type Location struct {

	// Address descriptor
	AddressComponents []*LocationAddressComponentsItems0 `json:"address_components"`

	// Natural representation of the address.
	FormattedAddress string `json:"formatted_address,omitempty"`

	// geometry
	Geometry *LocationGeometry `json:"geometry,omitempty"`

	// ID of the location.
	ID string `json:"id,omitempty"`

	// The ID of the place corresponding the location.
	PlaceID string `json:"place_id,omitempty"`

	// Location type from list.
	Types []LocationsAddressTypes `json:"types"`
}

// Validate validates this location
func (m *Location) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAddressComponents(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateGeometry(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateTypes(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Location) validateAddressComponents(formats strfmt.Registry) error {

	if swag.IsZero(m.AddressComponents) { // not required
		return nil
	}

	for i := 0; i < len(m.AddressComponents); i++ {

		if swag.IsZero(m.AddressComponents[i]) { // not required
			continue
		}

		if m.AddressComponents[i] != nil {

			if err := m.AddressComponents[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("address_components" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *Location) validateGeometry(formats strfmt.Registry) error {

	if swag.IsZero(m.Geometry) { // not required
		return nil
	}

	if m.Geometry != nil {

		if err := m.Geometry.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("geometry")
			}
			return err
		}
	}

	return nil
}

func (m *Location) validateTypes(formats strfmt.Registry) error {

	if swag.IsZero(m.Types) { // not required
		return nil
	}

	for i := 0; i < len(m.Types); i++ {

		if err := m.Types[i].Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("types" + "." + strconv.Itoa(i))
			}
			return err
		}

	}

	return nil
}

// LocationAddressComponentsItems0 location address components items0
// swagger:model LocationAddressComponentsItems0
type LocationAddressComponentsItems0 struct {

	// Location address long name
	LongName string `json:"long_name,omitempty"`

	// Location address short name
	ShortName string `json:"short_name,omitempty"`

	// Address type from list.
	Types []LocationsAddressTypes `json:"types"`
}

// Validate validates this location address components items0
func (m *LocationAddressComponentsItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateTypes(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *LocationAddressComponentsItems0) validateTypes(formats strfmt.Registry) error {

	if swag.IsZero(m.Types) { // not required
		return nil
	}

	for i := 0; i < len(m.Types); i++ {

		if err := m.Types[i].Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("types" + "." + strconv.Itoa(i))
			}
			return err
		}

	}

	return nil
}

// LocationGeometry location geometry
// swagger:model LocationGeometry
type LocationGeometry struct {

	// location
	Location *LocationGeometryLocation `json:"location,omitempty"`

	// Location's type
	LocationType string `json:"location_type,omitempty"`

	// viewport
	Viewport *LocationGeometryViewport `json:"viewport,omitempty"`
}

// Validate validates this location geometry
func (m *LocationGeometry) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateLocation(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateViewport(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *LocationGeometry) validateLocation(formats strfmt.Registry) error {

	if swag.IsZero(m.Location) { // not required
		return nil
	}

	if m.Location != nil {

		if err := m.Location.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("geometry" + "." + "location")
			}
			return err
		}
	}

	return nil
}

func (m *LocationGeometry) validateViewport(formats strfmt.Registry) error {

	if swag.IsZero(m.Viewport) { // not required
		return nil
	}

	if m.Viewport != nil {

		if err := m.Viewport.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("geometry" + "." + "viewport")
			}
			return err
		}
	}

	return nil
}

// LocationGeometryLocation Location coordinates.
// swagger:model LocationGeometryLocation
type LocationGeometryLocation struct {

	// Elevation in meters (inspired by Elevation API).
	Elevation float32 `json:"elevation,omitempty"`

	// Location's latitude.
	Lat float32 `json:"lat,omitempty"`

	// Location's longitude.
	Lng float32 `json:"lng,omitempty"`

	// The maximum distance between data points from which the elevation was interpolated, in meters (inspired by Elevation API).
	Resolution float32 `json:"resolution,omitempty"`
}

// Validate validates this location geometry location
func (m *LocationGeometryLocation) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// LocationGeometryViewport Viewport corners
// swagger:model LocationGeometryViewport
type LocationGeometryViewport struct {

	// northeast
	Northeast *LocationGeometryViewportNortheast `json:"northeast,omitempty"`

	// southwest
	Southwest *LocationGeometryViewportSouthwest `json:"southwest,omitempty"`
}

// Validate validates this location geometry viewport
func (m *LocationGeometryViewport) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateNortheast(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateSouthwest(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *LocationGeometryViewport) validateNortheast(formats strfmt.Registry) error {

	if swag.IsZero(m.Northeast) { // not required
		return nil
	}

	if m.Northeast != nil {

		if err := m.Northeast.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("geometry" + "." + "viewport" + "." + "northeast")
			}
			return err
		}
	}

	return nil
}

func (m *LocationGeometryViewport) validateSouthwest(formats strfmt.Registry) error {

	if swag.IsZero(m.Southwest) { // not required
		return nil
	}

	if m.Southwest != nil {

		if err := m.Southwest.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("geometry" + "." + "viewport" + "." + "southwest")
			}
			return err
		}
	}

	return nil
}

// LocationGeometryViewportNortheast Northeast corner coordinates.
// swagger:model LocationGeometryViewportNortheast
type LocationGeometryViewportNortheast struct {

	// lat
	Lat float32 `json:"lat,omitempty"`

	// lng
	Lng float32 `json:"lng,omitempty"`
}

// Validate validates this location geometry viewport northeast
func (m *LocationGeometryViewportNortheast) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// LocationGeometryViewportSouthwest Southwest corner coordinates.
// swagger:model LocationGeometryViewportSouthwest
type LocationGeometryViewportSouthwest struct {

	// lat
	Lat float32 `json:"lat,omitempty"`

	// lng
	Lng float32 `json:"lng,omitempty"`
}

// Validate validates this location geometry viewport southwest
func (m *LocationGeometryViewportSouthwest) Validate(formats strfmt.Registry) error {
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
