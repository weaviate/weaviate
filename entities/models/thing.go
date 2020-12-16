//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
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

// Thing thing
//
// swagger:model Thing
type Thing struct {

	// If this object was subject of a classificiation, additional meta info about this classification is available here. (Underscore properties are optional, include them using the ?include=_<propName> parameter)
	Classification *UnderscorePropertiesClassification `json:"_classification,omitempty"`

	// A feature projection of the object's vector into lower dimensions for visualization
	FeatureProjection *FeatureProjection `json:"_featureProjection,omitempty"`

	// Additional information about how this property was interpreted at vectorization. (Underscore properties are optional, include them using the ?include=_<propName> parameter)
	Interpretation *Interpretation `json:"_interpretation,omitempty"`

	// Additional information about the neighboring concepts of this element
	NearestNeighbors *NearestNeighbors `json:"_nearestNeighbors,omitempty"`

	// This object's position in the Contextionary vector space. (Underscore properties are optional, include them using the ?include=_<propName> parameter)
	Vector C11yVector `json:"_vector,omitempty"`

	// Class of the Thing, defined in the schema.
	Class string `json:"class,omitempty"`

	// Timestamp of creation of this Thing in milliseconds since epoch UTC.
	CreationTimeUnix int64 `json:"creationTimeUnix,omitempty"`

	// ID of the Thing.
	// Format: uuid
	ID strfmt.UUID `json:"id,omitempty"`

	// Timestamp of the last Thing update in milliseconds since epoch UTC.
	LastUpdateTimeUnix int64 `json:"lastUpdateTimeUnix,omitempty"`

	// schema
	Schema PropertySchema `json:"schema,omitempty"`

	// vector weights
	VectorWeights VectorWeights `json:"vectorWeights,omitempty"`
}

// Validate validates this thing
func (m *Thing) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateClassification(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateFeatureProjection(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateInterpretation(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateNearestNeighbors(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateVector(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateID(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Thing) validateClassification(formats strfmt.Registry) error {

	if swag.IsZero(m.Classification) { // not required
		return nil
	}

	if m.Classification != nil {
		if err := m.Classification.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("_classification")
			}
			return err
		}
	}

	return nil
}

func (m *Thing) validateFeatureProjection(formats strfmt.Registry) error {

	if swag.IsZero(m.FeatureProjection) { // not required
		return nil
	}

	if m.FeatureProjection != nil {
		if err := m.FeatureProjection.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("_featureProjection")
			}
			return err
		}
	}

	return nil
}

func (m *Thing) validateInterpretation(formats strfmt.Registry) error {

	if swag.IsZero(m.Interpretation) { // not required
		return nil
	}

	if m.Interpretation != nil {
		if err := m.Interpretation.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("_interpretation")
			}
			return err
		}
	}

	return nil
}

func (m *Thing) validateNearestNeighbors(formats strfmt.Registry) error {

	if swag.IsZero(m.NearestNeighbors) { // not required
		return nil
	}

	if m.NearestNeighbors != nil {
		if err := m.NearestNeighbors.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("_nearestNeighbors")
			}
			return err
		}
	}

	return nil
}

func (m *Thing) validateVector(formats strfmt.Registry) error {

	if swag.IsZero(m.Vector) { // not required
		return nil
	}

	if err := m.Vector.Validate(formats); err != nil {
		if ve, ok := err.(*errors.Validation); ok {
			return ve.ValidateName("_vector")
		}
		return err
	}

	return nil
}

func (m *Thing) validateID(formats strfmt.Registry) error {

	if swag.IsZero(m.ID) { // not required
		return nil
	}

	if err := validate.FormatOf("id", "body", "uuid", m.ID.String(), formats); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *Thing) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Thing) UnmarshalBinary(b []byte) error {
	var res Thing
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
