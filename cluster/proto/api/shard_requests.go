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
	CANCELLED   ShardReplicationState = "CANCELLED" // The operation has been cancelled. It cannot be resumed.
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
}

type ReplicationRegisterErrorResponse struct{}

type ReplicationRemoveOpRequest struct {
	Version int

	Id uint64
}

type ReplicationDeleteOpResponse struct{}

type ReplicationDetailsRequest struct {
	Uuid strfmt.UUID
}

type ReplicationDetailsRequestByCollection struct {
	Collection string
}

type ReplicationDetailsRequestByCollectionAndShard struct {
	Collection string
	Shard      string
}

type ReplicationDetailsRequestByTargetNode struct {
	Node string
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

	Uncancelable       bool
	ScheduledForCancel bool
	ScheduledForDelete bool

	Status        ReplicationDetailsState
	StatusHistory []ReplicationDetailsState
	TransferType  string
}

type ReplicationCancelRequest struct {
	Version int
	Uuid    strfmt.UUID
}

type ReplicationDeleteRequest struct {
	Version int
	Uuid    strfmt.UUID
}

type ReplicationCancellationCompleteRequest struct {
	Version int
	Id      uint64
}

type ReplicationsDeleteByCollectionRequest struct {
	Version    int
	Collection string
}

type ReplicationsDeleteByTenantsRequest struct {
	Version    int
	Collection string
	Tenants    []string
}

type ShardingState struct {
	Collection string
	Shards     map[string][]string
}

type ReplicationQueryShardingStateByCollectionRequest struct {
	Collection string
}

type ReplicationQueryShardingStateByCollectionAndShardRequest struct {
	Collection string
	Shard      string
}

type ReplicationDeleteAllRequest struct {
	Version int
}

type ReplicationPurgeRequest struct {
	Version int
}

type ReplicationOperationStateRequest struct {
	Id uint64
}

type ReplicationOperationStateResponse struct {
	State ShardReplicationState
}

type ReplicationStoreSchemaVersionRequest struct {
	Version       int
	SchemaVersion uint64
	Id            uint64
}

type ReplicationAddReplicaToShard struct {
	OpId                     uint64
	Class, Shard, TargetNode string
	SchemaVersion            uint64
}

type ReplicationForceDeleteAllRequest struct{}

type ReplicationForceDeleteByCollectionRequest struct {
	Collection string
}

type ReplicationForceDeleteByCollectionAndShardRequest struct {
	Collection string
	Shard      string
}

type ReplicationForceDeleteByTargetNodeRequest struct {
	Node string
}

type ReplicationForceDeleteByUuidRequest struct {
	Uuid strfmt.UUID
}
