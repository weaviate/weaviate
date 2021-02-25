//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// Deprecation deprecation
//
// swagger:model Deprecation
type Deprecation struct {

	// Describes which API is effected, usually one of: REST, GraphQL
	APIType string `json:"apiType,omitempty"`

	// The id that uniquely identifies this particular deprecations (mostly used internally)
	ID string `json:"id,omitempty"`

	// The locations within the specified API affected by this deprecation
	Locations []string `json:"locations"`

	// User-required object to not be affected by the (planned) removal
	Mitigation string `json:"mitigation,omitempty"`

	// What this deprecation is about
	Msg string `json:"msg,omitempty"`

	// A best-effort guess of which upcoming version will remove the feature entirely
	PlannedRemovalVersion string `json:"plannedRemovalVersion,omitempty"`

	// If the feature has already been removed, it was removed in this version
	RemovedIn *string `json:"removedIn,omitempty"`

	// If the feature has already been removed, it was removed at this timestamp
	// Format: date-time
	RemovedTime *strfmt.DateTime `json:"removedTime,omitempty"`

	// The deprecation was introduced in this version
	// Format: date-time
	SinceTime strfmt.DateTime `json:"sinceTime,omitempty"`

	// The deprecation was introduced in this version
	SinceVersion string `json:"sinceVersion,omitempty"`

	// Whether the problematic API functionality is deprecated (planned to be removed) or already removed
	Status string `json:"status,omitempty"`
}

// Validate validates this deprecation
func (m *Deprecation) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateRemovedTime(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateSinceTime(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Deprecation) validateRemovedTime(formats strfmt.Registry) error {

	if swag.IsZero(m.RemovedTime) { // not required
		return nil
	}

	if err := validate.FormatOf("removedTime", "body", "date-time", m.RemovedTime.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *Deprecation) validateSinceTime(formats strfmt.Registry) error {

	if swag.IsZero(m.SinceTime) { // not required
		return nil
	}

	if err := validate.FormatOf("sinceTime", "body", "date-time", m.SinceTime.String(), formats); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *Deprecation) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Deprecation) UnmarshalBinary(b []byte) error {
	var res Deprecation
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
