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

package replica

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

type RemoteIncomingRepo interface {
	// Write endpoints
	ReplicateObject(ctx context.Context, indexName, shardName,
		requestID string, object *storobj.Object) SimpleResponse
	ReplicateObjects(ctx context.Context, indexName,
		shardName, requestID string, objects []*storobj.Object) SimpleResponse
	ReplicateUpdate(ctx context.Context, indexName,
		shardName, requestID string, mergeDoc *objects.MergeDocument) SimpleResponse
	ReplicateDeletion(ctx context.Context, indexName,
		shardName, requestID string, uuid strfmt.UUID) SimpleResponse
	ReplicateDeletions(ctx context.Context, indexName,
		shardName, requestID string, uuids []strfmt.UUID, dryRun bool) SimpleResponse
	ReplicateReferences(ctx context.Context, indexName,
		shardName, requestID string, refs []objects.BatchReference) SimpleResponse
	CommitReplication(indexName,
		shardName, requestID string) interface{}
	AbortReplication(indexName,
		shardName, requestID string) interface{}
	OverwriteObjects(ctx context.Context, index, shard string,
		vobjects []*objects.VObject) ([]RepairResponse, error)
	// Read endpoints
	FetchObject(ctx context.Context, indexName,
		shardName string, id strfmt.UUID) (objects.Replica, error)
	FetchObjects(ctx context.Context, class,
		shardName string, ids []strfmt.UUID) ([]objects.Replica, error)
	DigestObjects(ctx context.Context, class, shardName string,
		ids []strfmt.UUID) (result []RepairResponse, err error)
}

type RemoteReplicaIncoming struct {
	repo RemoteIncomingRepo
}

func NewRemoteReplicaIncoming(repo RemoteIncomingRepo) *RemoteReplicaIncoming {
	return &RemoteReplicaIncoming{
		repo: repo,
	}
}

func (rri *RemoteReplicaIncoming) ReplicateObject(ctx context.Context, indexName,
	shardName, requestID string, object *storobj.Object,
) SimpleResponse {
	return rri.repo.ReplicateObject(ctx, indexName, shardName, requestID, object)
}

func (rri *RemoteReplicaIncoming) ReplicateObjects(ctx context.Context, indexName,
	shardName, requestID string, objects []*storobj.Object,
) SimpleResponse {
	return rri.repo.ReplicateObjects(ctx, indexName, shardName, requestID, objects)
}

func (rri *RemoteReplicaIncoming) ReplicateUpdate(ctx context.Context, indexName,
	shardName, requestID string, mergeDoc *objects.MergeDocument,
) SimpleResponse {
	return rri.repo.ReplicateUpdate(ctx, indexName, shardName, requestID, mergeDoc)
}

func (rri *RemoteReplicaIncoming) ReplicateDeletion(ctx context.Context, indexName,
	shardName, requestID string, uuid strfmt.UUID,
) SimpleResponse {
	return rri.repo.ReplicateDeletion(ctx, indexName, shardName, requestID, uuid)
}

func (rri *RemoteReplicaIncoming) ReplicateDeletions(ctx context.Context, indexName,
	shardName, requestID string, uuids []strfmt.UUID, dryRun bool,
) SimpleResponse {
	return rri.repo.ReplicateDeletions(ctx, indexName, shardName, requestID, uuids, dryRun)
}

func (rri *RemoteReplicaIncoming) ReplicateReferences(ctx context.Context, indexName,
	shardName, requestID string, refs []objects.BatchReference,
) SimpleResponse {
	return rri.repo.ReplicateReferences(ctx, indexName, shardName, requestID, refs)
}

func (rri *RemoteReplicaIncoming) CommitReplication(indexName,
	shardName, requestID string,
) interface{} {
	return rri.repo.CommitReplication(indexName, shardName, requestID)
}

func (rri *RemoteReplicaIncoming) AbortReplication(indexName,
	shardName, requestID string,
) interface{} {
	return rri.repo.AbortReplication(indexName, shardName, requestID)
}

func (rri *RemoteReplicaIncoming) OverwriteObjects(ctx context.Context,
	indexName, shardName string, vobjects []*objects.VObject,
) ([]RepairResponse, error) {
	return rri.repo.OverwriteObjects(ctx, indexName, shardName, vobjects)
}

func (rri *RemoteReplicaIncoming) FetchObject(ctx context.Context,
	indexName, shardName string, id strfmt.UUID,
) (objects.Replica, error) {
	return rri.repo.FetchObject(ctx, indexName, shardName, id)
}

func (rri *RemoteReplicaIncoming) FetchObjects(ctx context.Context,
	indexName, shardName string, ids []strfmt.UUID,
) ([]objects.Replica, error) {
	return rri.repo.FetchObjects(ctx, indexName, shardName, ids)
}

func (rri *RemoteReplicaIncoming) DigestObjects(ctx context.Context,
	indexName, shardName string, ids []strfmt.UUID,
) (result []RepairResponse, err error) {
	return rri.repo.DigestObjects(ctx, indexName, shardName, ids)
}
