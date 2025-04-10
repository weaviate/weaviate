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
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// ReplicationConfig Configure how replication is executed in a cluster
//
// swagger:model ReplicationConfig
type ReplicationConfig struct {

	// Enable asynchronous replication (default: false).
	AsyncEnabled bool `json:"asyncEnabled"`

	// Config for async replication
	AsyncReplicationConfig *AsyncReplicationConfig `json:"asyncReplicationConfig,omitempty"`

	// Conflict resolution strategy for deleted objects.
	// Enum: [NoAutomatedResolution DeleteOnConflict TimeBasedResolution]
	DeletionStrategy string `json:"deletionStrategy,omitempty"`

	// Number of times a class is replicated (default: 1).
	Factor int64 `json:"factor,omitempty"`
}

// Validate validates this replication config
func (m *ReplicationConfig) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateAsyncReplicationConfig(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateDeletionStrategy(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ReplicationConfig) validateAsyncReplicationConfig(formats strfmt.Registry) error {
	if swag.IsZero(m.AsyncReplicationConfig) { // not required
		return nil
	}

	if m.AsyncReplicationConfig != nil {
		if err := m.AsyncReplicationConfig.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("asyncReplicationConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("asyncReplicationConfig")
			}
			return err
		}
	}

	return nil
}

var replicationConfigTypeDeletionStrategyPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["NoAutomatedResolution","DeleteOnConflict","TimeBasedResolution"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		replicationConfigTypeDeletionStrategyPropEnum = append(replicationConfigTypeDeletionStrategyPropEnum, v)
	}
}

const (

	// ReplicationConfigDeletionStrategyNoAutomatedResolution captures enum value "NoAutomatedResolution"
	ReplicationConfigDeletionStrategyNoAutomatedResolution string = "NoAutomatedResolution"

	// ReplicationConfigDeletionStrategyDeleteOnConflict captures enum value "DeleteOnConflict"
	ReplicationConfigDeletionStrategyDeleteOnConflict string = "DeleteOnConflict"

	// ReplicationConfigDeletionStrategyTimeBasedResolution captures enum value "TimeBasedResolution"
	ReplicationConfigDeletionStrategyTimeBasedResolution string = "TimeBasedResolution"
)

// prop value enum
func (m *ReplicationConfig) validateDeletionStrategyEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, replicationConfigTypeDeletionStrategyPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *ReplicationConfig) validateDeletionStrategy(formats strfmt.Registry) error {
	if swag.IsZero(m.DeletionStrategy) { // not required
		return nil
	}

	// value enum
	if err := m.validateDeletionStrategyEnum("deletionStrategy", "body", m.DeletionStrategy); err != nil {
		return err
	}

	return nil
}

// ContextValidate validate this replication config based on the context it is used
func (m *ReplicationConfig) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := m.contextValidateAsyncReplicationConfig(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ReplicationConfig) contextValidateAsyncReplicationConfig(ctx context.Context, formats strfmt.Registry) error {

	if m.AsyncReplicationConfig != nil {
		if err := m.AsyncReplicationConfig.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("asyncReplicationConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("asyncReplicationConfig")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *ReplicationConfig) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ReplicationConfig) UnmarshalBinary(b []byte) error {
	var res ReplicationConfig
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
