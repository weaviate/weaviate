// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// ReferenceMetaClassification This meta field contains additional info about the classified reference property
//
// swagger:model ReferenceMetaClassification
type ReferenceMetaClassification struct {

	// The lowest distance of a neighbor in the losing group. Optional. If k equals the size of the winning group, there is no losing group
	ClosestLosingDistance *float64 `json:"closestLosingDistance,omitempty"`

	// The lowest distance of any neighbor, regardless of whether they were in the winning or losing group
	ClosestOverallDistance float64 `json:"closestOverallDistance,omitempty"`

	// Closest distance of a neighbor from the winning group
	ClosestWinningDistance float64 `json:"closestWinningDistance,omitempty"`

	// size of the losing group, can be 0 if the winning group size euqals k
	LosingCount int64 `json:"losingCount,omitempty"`

	// deprecated - do not use, to be removed in 0.23.0
	LosingDistance *float64 `json:"losingDistance,omitempty"`

	// Mean distance of all neighbors from the losing group. Optional. If k equals the size of the winning group, there is no losing group.
	MeanLosingDistance *float64 `json:"meanLosingDistance,omitempty"`

	// Mean distance of all neighbors from the winning group
	MeanWinningDistance float64 `json:"meanWinningDistance,omitempty"`

	// overall neighbors checked as part of the classification. In most cases this will equal k, but could be lower than k - for example if not enough data was present
	OverallCount int64 `json:"overallCount,omitempty"`

	// size of the winning group, a number between 1..k
	WinningCount int64 `json:"winningCount,omitempty"`

	// deprecated - do not use, to be removed in 0.23.0
	WinningDistance float64 `json:"winningDistance,omitempty"`
}

// Validate validates this reference meta classification
func (m *ReferenceMetaClassification) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *ReferenceMetaClassification) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ReferenceMetaClassification) UnmarshalBinary(b []byte) error {
	var res ReferenceMetaClassification
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
