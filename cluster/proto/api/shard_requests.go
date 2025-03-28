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

const (
	ReplicationCommandVersionV0 = iota
)

type ShardReplicationState string

const (
	REGISTERED  ShardReplicationState = "REGISTERED"
	HYDRATING   ShardReplicationState = "HYDRATING"
	FINALIZING  ShardReplicationState = "FINALIZING"
	READY       ShardReplicationState = "READY"
	DEHYDRATING ShardReplicationState = "DEHYDRATING"
	ABORTED     ShardReplicationState = "ABORTED"
)

type ReplicationReplicateShardRequest struct {
	// Version is the version with which this command was generated
	Version int

	SourceNode       string
	SourceCollection string
	SourceShard      string

	TargetNode string
}

type ReplicationReplicateShardReponse struct{}

type ReplicationUpdateOpStateRequest struct {
	Version int

	Id    uint64
	State ShardReplicationState
}

type ReplicationUpdateOpStateResponse struct{}

type ReplicationDeleteOpRequest struct {
	Version int

	Id uint64
}

type ReplicationDeleteOpResponse struct{}
