//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

type RemoteReplicaIncomingRepo interface {
	ReplicateObject(ctx context.Context, indexName, shardName,
		requestID string, object *storobj.Object) SimpleResponse
	ReplicateObjects(ctx context.Context, indexName,
		shardName, requestID string, objects []*storobj.Object) SimpleResponse
	ReplicateUpdate(ctx context.Context, indexName,
		shardName, requestID string, mergeDoc *objects.MergeDocument) SimpleResponse
	ReplicateDeletion(ctx context.Context, indexName,
		shardName, requestID string, uuid strfmt.UUID) SimpleResponse
	ReplicateDeletions(ctx context.Context, indexName,
		shardName, requestID string, docIDs []uint64, dryRun bool) SimpleResponse
	ReplicateReferences(ctx context.Context, indexName,
		shardName, requestID string, refs []objects.BatchReference) SimpleResponse
	CommitReplication(ctx context.Context, indexName,
		shardName, requestID string) interface{}
	AbortReplication(ctx context.Context, indexName,
		shardName, requestID string) interface{}
}

type RemoteReplicaIncoming struct {
	repo RemoteReplicaIncomingRepo
}

func NewRemoteReplicaIncoming(repo RemoteReplicaIncomingRepo) *RemoteReplicaIncoming {
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
	shardName, requestID string, docIDs []uint64, dryRun bool,
) SimpleResponse {
	return rri.repo.ReplicateDeletions(ctx, indexName, shardName, requestID, docIDs, dryRun)
}

func (rri *RemoteReplicaIncoming) ReplicateReferences(ctx context.Context, indexName,
	shardName, requestID string, refs []objects.BatchReference,
) SimpleResponse {
	return rri.repo.ReplicateReferences(ctx, indexName, shardName, requestID, refs)
}

func (rri *RemoteReplicaIncoming) CommitReplication(ctx context.Context, indexName,
	shardName, requestID string,
) interface{} {
	return rri.repo.CommitReplication(ctx, indexName, shardName, requestID)
}

func (rri *RemoteReplicaIncoming) AbortReplication(ctx context.Context, indexName,
	shardName, requestID string,
) interface{} {
	return rri.repo.AbortReplication(ctx, indexName, shardName, requestID)
}
