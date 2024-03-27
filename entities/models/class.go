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
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// Class class
//
// swagger:model Class
type Class struct {

	// Name of the collection (a.k.a. class). Multiple words should be concatenated in CamelCase, e.g. `ArticleAuthor`.
	Class string `json:"class,omitempty"`

	// Description of the class.
	Description string `json:"description,omitempty"`

	// inverted index config
	InvertedIndexConfig *InvertedIndexConfig `json:"invertedIndexConfig,omitempty"`

	// Configuration specific to modules this Weaviate instance has installed
	ModuleConfig interface{} `json:"moduleConfig,omitempty"`

	// multi tenancy config
	MultiTenancyConfig *MultiTenancyConfig `json:"multiTenancyConfig,omitempty"`

	// The properties of the class.
	Properties []*Property `json:"properties"`

	// replication config
	ReplicationConfig *ReplicationConfig `json:"replicationConfig,omitempty"`

	// sharding config
	ShardingConfig *ShardingConfig `json:"shardingConfig,omitempty"`

	// vector config
	VectorConfig map[string]VectorConfig `json:"vectorConfig,omitempty"`

	// Vector-index config, that is specific to the type of index selected in vectorIndexType
	VectorIndexConfig interface{} `json:"vectorIndexConfig,omitempty"`

	// Name of the vector index to use, eg. (HNSW)
	VectorIndexType string `json:"vectorIndexType,omitempty"`

	// Specify how the vectors for this class should be determined. The options are either 'none' - this means you have to import a vector with each object yourself - or the name of a module that provides vectorization capabilities, such as 'text2vec-contextionary'. If left empty, it will use the globally configured default which can itself either be 'none' or a specific module.
	Vectorizer string `json:"vectorizer,omitempty"`
}

// Validate validates this class
func (m *Class) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateInvertedIndexConfig(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateMultiTenancyConfig(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateProperties(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateReplicationConfig(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateShardingConfig(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateVectorConfig(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Class) validateInvertedIndexConfig(formats strfmt.Registry) error {
	if swag.IsZero(m.InvertedIndexConfig) { // not required
		return nil
	}

	if m.InvertedIndexConfig != nil {
		if err := m.InvertedIndexConfig.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("invertedIndexConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("invertedIndexConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) validateMultiTenancyConfig(formats strfmt.Registry) error {
	if swag.IsZero(m.MultiTenancyConfig) { // not required
		return nil
	}

	if m.MultiTenancyConfig != nil {
		if err := m.MultiTenancyConfig.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("multiTenancyConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("multiTenancyConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) validateProperties(formats strfmt.Registry) error {
	if swag.IsZero(m.Properties) { // not required
		return nil
	}

	for i := 0; i < len(m.Properties); i++ {
		if swag.IsZero(m.Properties[i]) { // not required
			continue
		}

		if m.Properties[i] != nil {
			if err := m.Properties[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("properties" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("properties" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *Class) validateReplicationConfig(formats strfmt.Registry) error {
	if swag.IsZero(m.ReplicationConfig) { // not required
		return nil
	}

	if m.ReplicationConfig != nil {
		if err := m.ReplicationConfig.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("replicationConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("replicationConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) validateShardingConfig(formats strfmt.Registry) error {
	if swag.IsZero(m.ShardingConfig) { // not required
		return nil
	}

	if m.ShardingConfig != nil {
		if err := m.ShardingConfig.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("shardingConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("shardingConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) validateVectorConfig(formats strfmt.Registry) error {
	if swag.IsZero(m.VectorConfig) { // not required
		return nil
	}

	for k := range m.VectorConfig {

		if err := validate.Required("vectorConfig"+"."+k, "body", m.VectorConfig[k]); err != nil {
			return err
		}
		if val, ok := m.VectorConfig[k]; ok {
			if err := val.Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("vectorConfig" + "." + k)
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("vectorConfig" + "." + k)
				}
				return err
			}
		}

	}

	return nil
}

// ContextValidate validate this class based on the context it is used
func (m *Class) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := m.contextValidateInvertedIndexConfig(ctx, formats); err != nil {
		res = append(res, err)
	}

	if err := m.contextValidateMultiTenancyConfig(ctx, formats); err != nil {
		res = append(res, err)
	}

	if err := m.contextValidateProperties(ctx, formats); err != nil {
		res = append(res, err)
	}

	if err := m.contextValidateReplicationConfig(ctx, formats); err != nil {
		res = append(res, err)
	}

	if err := m.contextValidateShardingConfig(ctx, formats); err != nil {
		res = append(res, err)
	}

	if err := m.contextValidateVectorConfig(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Class) contextValidateInvertedIndexConfig(ctx context.Context, formats strfmt.Registry) error {

	if m.InvertedIndexConfig != nil {
		if err := m.InvertedIndexConfig.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("invertedIndexConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("invertedIndexConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) contextValidateMultiTenancyConfig(ctx context.Context, formats strfmt.Registry) error {

	if m.MultiTenancyConfig != nil {
		if err := m.MultiTenancyConfig.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("multiTenancyConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("multiTenancyConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) contextValidateProperties(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(m.Properties); i++ {

		if m.Properties[i] != nil {
			if err := m.Properties[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("properties" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("properties" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *Class) contextValidateReplicationConfig(ctx context.Context, formats strfmt.Registry) error {

	if m.ReplicationConfig != nil {
		if err := m.ReplicationConfig.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("replicationConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("replicationConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) contextValidateShardingConfig(ctx context.Context, formats strfmt.Registry) error {

	if m.ShardingConfig != nil {
		if err := m.ShardingConfig.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("shardingConfig")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("shardingConfig")
			}
			return err
		}
	}

	return nil
}

func (m *Class) contextValidateVectorConfig(ctx context.Context, formats strfmt.Registry) error {

	for k := range m.VectorConfig {

		if val, ok := m.VectorConfig[k]; ok {
			if err := val.ContextValidate(ctx, formats); err != nil {
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *Class) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Class) UnmarshalBinary(b []byte) error {
	var res Class
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
