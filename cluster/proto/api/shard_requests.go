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
	// The operation has been scheduled for cancellation. Cleanup will be performed on the target.
	CANCELLING ShardReplicationState = "CANCELLING"
	// The operation has been cancelled. It cannot be resumed.
	CANCELLED ShardReplicationState = "CANCELLED"
	// The operation has been scheduled for deletion. Cleanup will be performed on the target. Followed by removal from the FSM.
	DELETING ShardReplicationState = "DELETING"
)

type ReplicationReplicateShardRequest struct {
	// Version is the version with which this command was generated
	Version int

	SourceNode       string
	SourceCollection string
	SourceShard      string

	TargetNode string

	Uuid strfmt.UUID
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

type ReplicationRemoveOpRequest struct {
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
}

type ReplicationCancelRequest struct {
	Version int
	Uuid    strfmt.UUID
}

type ReplicationDeleteRequest struct {
	Version int

	Uuid strfmt.UUID
}
