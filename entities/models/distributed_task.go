//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// DistributedTask Distributed task metadata.
//
// swagger:model DistributedTask
type DistributedTask struct {

	// The high level reason why the task failed.
	Error string `json:"error,omitempty"`

	// The time when the task was finished.
	// Format: date-time
	FinishedAt strfmt.DateTime `json:"finishedAt,omitempty"`

	// The nodes that finished the task.
	FinishedNodes []string `json:"finishedNodes"`

	// The ID of the task.
	ID string `json:"id,omitempty"`

	// The payload of the task.
	Payload interface{} `json:"payload,omitempty"`

	// The time when the task was created.
	// Format: date-time
	StartedAt strfmt.DateTime `json:"startedAt,omitempty"`

	// The status of the task.
	Status string `json:"status,omitempty"`

	// The version of the task.
	Version int64 `json:"version,omitempty"`
}

// Validate validates this distributed task
func (m *DistributedTask) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateFinishedAt(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateStartedAt(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *DistributedTask) validateFinishedAt(formats strfmt.Registry) error {
	if swag.IsZero(m.FinishedAt) { // not required
		return nil
	}

	if err := validate.FormatOf("finishedAt", "body", "date-time", m.FinishedAt.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *DistributedTask) validateStartedAt(formats strfmt.Registry) error {
	if swag.IsZero(m.StartedAt) { // not required
		return nil
	}

	if err := validate.FormatOf("startedAt", "body", "date-time", m.StartedAt.String(), formats); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this distributed task based on context it is used
func (m *DistributedTask) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *DistributedTask) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *DistributedTask) UnmarshalBinary(b []byte) error {
	var res DistributedTask
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
