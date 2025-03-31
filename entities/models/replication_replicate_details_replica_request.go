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

// ReplicationReplicateDetailsReplicaRequest ReplicationDetailsReplicaRequest
//
// swagger:model ReplicationReplicateDetailsReplicaRequest
type ReplicationReplicateDetailsReplicaRequest struct {

	// The name of the collection holding data.
	// Required: true
	Collection *string `json:"collection"`

	// The unique id of the replication operation.
	// Required: true
	ID *string `json:"id"`

	// The id of the shard to collect replication details for.
	// Required: true
	ShardID *string `json:"shardId"`

	// The id of the node where the source replica is allocated.
	// Required: true
	SourceNodeID *string `json:"sourceNodeId"`

	// The id of the replica used as a source of the replication operation.
	// Required: true
	SourceReplicaID *string `json:"sourceReplicaId"`

	// The current status of the shard replica operation.
	// Required: true
	// Enum: [READY INDEXING REPLICATION_FINALIZING REPLICATION_HYDRATING REPLICATION_DEHYDRATING]
	Status *string `json:"status"`

	// The id of the node where the target replica is allocated.
	// Required: true
	TargetNodeID *string `json:"targetNodeId"`

	// The id of the replica used as a target of the replication operation.
	// Required: true
	TargetReplicaID *string `json:"targetReplicaId"`
}

// Validate validates this replication replicate details replica request
func (m *ReplicationReplicateDetailsReplicaRequest) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCollection(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateShardID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateSourceNodeID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateSourceReplicaID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateTargetNodeID(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateTargetReplicaID(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateCollection(formats strfmt.Registry) error {

	if err := validate.Required("collection", "body", m.Collection); err != nil {
		return err
	}

	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateID(formats strfmt.Registry) error {

	if err := validate.Required("id", "body", m.ID); err != nil {
		return err
	}

	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateShardID(formats strfmt.Registry) error {

	if err := validate.Required("shardId", "body", m.ShardID); err != nil {
		return err
	}

	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateSourceNodeID(formats strfmt.Registry) error {

	if err := validate.Required("sourceNodeId", "body", m.SourceNodeID); err != nil {
		return err
	}

	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateSourceReplicaID(formats strfmt.Registry) error {

	if err := validate.Required("sourceReplicaId", "body", m.SourceReplicaID); err != nil {
		return err
	}

	return nil
}

var replicationReplicateDetailsReplicaRequestTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["READY","INDEXING","REPLICATION_FINALIZING","REPLICATION_HYDRATING","REPLICATION_DEHYDRATING"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		replicationReplicateDetailsReplicaRequestTypeStatusPropEnum = append(replicationReplicateDetailsReplicaRequestTypeStatusPropEnum, v)
	}
}

const (

	// ReplicationReplicateDetailsReplicaRequestStatusREADY captures enum value "READY"
	ReplicationReplicateDetailsReplicaRequestStatusREADY string = "READY"

	// ReplicationReplicateDetailsReplicaRequestStatusINDEXING captures enum value "INDEXING"
	ReplicationReplicateDetailsReplicaRequestStatusINDEXING string = "INDEXING"

	// ReplicationReplicateDetailsReplicaRequestStatusREPLICATIONFINALIZING captures enum value "REPLICATION_FINALIZING"
	ReplicationReplicateDetailsReplicaRequestStatusREPLICATIONFINALIZING string = "REPLICATION_FINALIZING"

	// ReplicationReplicateDetailsReplicaRequestStatusREPLICATIONHYDRATING captures enum value "REPLICATION_HYDRATING"
	ReplicationReplicateDetailsReplicaRequestStatusREPLICATIONHYDRATING string = "REPLICATION_HYDRATING"

	// ReplicationReplicateDetailsReplicaRequestStatusREPLICATIONDEHYDRATING captures enum value "REPLICATION_DEHYDRATING"
	ReplicationReplicateDetailsReplicaRequestStatusREPLICATIONDEHYDRATING string = "REPLICATION_DEHYDRATING"
)

// prop value enum
func (m *ReplicationReplicateDetailsReplicaRequest) validateStatusEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, replicationReplicateDetailsReplicaRequestTypeStatusPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateStatus(formats strfmt.Registry) error {

	if err := validate.Required("status", "body", m.Status); err != nil {
		return err
	}

	// value enum
	if err := m.validateStatusEnum("status", "body", *m.Status); err != nil {
		return err
	}

	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateTargetNodeID(formats strfmt.Registry) error {

	if err := validate.Required("targetNodeId", "body", m.TargetNodeID); err != nil {
		return err
	}

	return nil
}

func (m *ReplicationReplicateDetailsReplicaRequest) validateTargetReplicaID(formats strfmt.Registry) error {

	if err := validate.Required("targetReplicaId", "body", m.TargetReplicaID); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this replication replicate details replica request based on context it is used
func (m *ReplicationReplicateDetailsReplicaRequest) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *ReplicationReplicateDetailsReplicaRequest) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ReplicationReplicateDetailsReplicaRequest) UnmarshalBinary(b []byte) error {
	var res ReplicationReplicateDetailsReplicaRequest
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
