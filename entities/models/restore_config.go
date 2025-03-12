//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

// RestoreConfig Backup custom configuration
//
// swagger:model RestoreConfig
type RestoreConfig struct {

	// Name of the bucket, container, volume, etc
	Bucket string `json:"Bucket,omitempty"`

	// Desired CPU core utilization ranging from 1%-80%
	// Maximum: 80
	// Minimum: 1
	CPUPercentage int64 `json:"CPUPercentage,omitempty"`

	// name of the endpoint, e.g. s3.amazonaws.com
	Endpoint string `json:"Endpoint,omitempty"`

	// Path within the bucket
	Path string `json:"Path,omitempty"`
}

// Validate validates this restore config
func (m *RestoreConfig) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCPUPercentage(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *RestoreConfig) validateCPUPercentage(formats strfmt.Registry) error {
	if swag.IsZero(m.CPUPercentage) { // not required
		return nil
	}

	if err := validate.MinimumInt("CPUPercentage", "body", m.CPUPercentage, 1, false); err != nil {
		return err
	}

	if err := validate.MaximumInt("CPUPercentage", "body", m.CPUPercentage, 80, false); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this restore config based on context it is used
func (m *RestoreConfig) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *RestoreConfig) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *RestoreConfig) UnmarshalBinary(b []byte) error {
	var res RestoreConfig
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
