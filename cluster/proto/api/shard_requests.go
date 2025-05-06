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

package api

import "github.com/go-openapi/strfmt"

const (
	ReplicationCommandVersionV0 = iota
)

type ShardReplicationState string

func (s ShardReplicationState) String() string {
	return string(s)
}

const (
	REGISTERED  ShardReplicationState = "REGISTERED"
	HYDRATING   ShardReplicationState = "HYDRATING"
	FINALIZING  ShardReplicationState = "FINALIZING"
	READY       ShardReplicationState = "READY"
	DEHYDRATING ShardReplicationState = "DEHYDRATING"
	ABORTED     ShardReplicationState = "ABORTED"
)

type ShardReplicationTransferType string

func (s ShardReplicationTransferType) String() string {
	return string(s)
}

const (
	COPY ShardReplicationTransferType = "COPY"
	MOVE ShardReplicationTransferType = "MOVE"
)

type ReplicationReplicateShardRequest struct {
	// Version is the version with which this command was generated
	Version int

	Uuid strfmt.UUID

	SourceNode       string
	SourceCollection string
	SourceShard      string
	TargetNode       string

	TransferType string
}

type ReplicationReplicateShardReponse struct{}

type ReplicationUpdateOpStateRequest struct {
	Version int

	Id    uint64
	State ShardReplicationState
}

type ReplicationUpdateOpStateResponse struct{}

type ReplicationRegisterErrorRequest struct {
	Version int

	Id    uint64
	Error string
	Uuid  strfmt.UUID
}

type ReplicationRegisterErrorResponse struct{}

type ReplicationDeleteOpRequest struct {
	Version int

	Id uint64
}

type ReplicationDeleteOpResponse struct{}

type ReplicationDetailsRequest struct {
	Uuid strfmt.UUID
}

type ReplicationDetailsState struct {
	State  string
	Errors []string
}

type ReplicationDetailsResponse struct {
	Uuid         strfmt.UUID
	Id           uint64
	ShardId      string
	Collection   string
	SourceNodeId string
	TargetNodeId string

	Status        ReplicationDetailsState
	StatusHistory []ReplicationDetailsState
	TransferType  string
}
