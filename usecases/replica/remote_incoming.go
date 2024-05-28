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
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

type RemoteIncomingRepo interface {
	GetIndexForIncomingReplica(className schema.ClassName) RemoteIndexIncomingRepo
}

type RemoteIncomingSchema interface {
	// WaitForUpdate ensures that the local schema has caught up to schemaVersion
	WaitForUpdate(ctx context.Context, schemaVersion uint64) error
}

type RemoteIndexIncomingRepo interface {
	// Write endpoints
	ReplicateObject(ctx context.Context, shardName, requestID string, object *storobj.Object) SimpleResponse
	ReplicateObjects(ctx context.Context, shardName, requestID string, objects []*storobj.Object, schemaVersion uint64) SimpleResponse
	ReplicateUpdate(ctx context.Context, shardName, requestID string, mergeDoc *objects.MergeDocument) SimpleResponse
	ReplicateDeletion(ctx context.Context, shardName, requestID string, uuid strfmt.UUID) SimpleResponse
	ReplicateDeletions(ctx context.Context, shardName, requestID string, uuids []strfmt.UUID, dryRun bool, schemaVersion uint64) SimpleResponse
	ReplicateReferences(ctx context.Context, shardName, requestID string, refs []objects.BatchReference) SimpleResponse
	CommitReplication(shardName, requestID string) interface{}
	AbortReplication(shardName, requestID string) interface{}
	OverwriteObjects(ctx context.Context, shard string, vobjects []*objects.VObject) ([]RepairResponse, error)
	// Read endpoints
	FetchObject(ctx context.Context, shardName string, id strfmt.UUID) (objects.Replica, error)
	FetchObjects(ctx context.Context, shardName string, ids []strfmt.UUID) ([]objects.Replica, error)
	DigestObjects(ctx context.Context, shardName string, ids []strfmt.UUID) (result []RepairResponse, err error)
}

type RemoteReplicaIncoming struct {
	repo   RemoteIncomingRepo
	schema RemoteIncomingSchema
}

func NewRemoteReplicaIncoming(repo RemoteIncomingRepo, schema RemoteIncomingSchema) *RemoteReplicaIncoming {
	return &RemoteReplicaIncoming{
		schema: schema,
		repo:   repo,
	}
}

func (rri *RemoteReplicaIncoming) ReplicateObject(ctx context.Context, indexName,
	shardName, requestID string, object *storobj.Object, schemaVersion uint64,
) SimpleResponse {
	index, simpleResp := rri.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.ReplicateObject(ctx, shardName, requestID, object)
}

func (rri *RemoteReplicaIncoming) ReplicateObjects(ctx context.Context, indexName,
	shardName, requestID string, objects []*storobj.Object, schemaVersion uint64,
) SimpleResponse {
	index, simpleResp := rri.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.ReplicateObjects(ctx, shardName, requestID, objects, schemaVersion)
}

func (rri *RemoteReplicaIncoming) ReplicateUpdate(ctx context.Context, indexName,
	shardName, requestID string, mergeDoc *objects.MergeDocument, schemaVersion uint64,
) SimpleResponse {
	index, simpleResp := rri.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.ReplicateUpdate(ctx, shardName, requestID, mergeDoc)
}

func (rri *RemoteReplicaIncoming) ReplicateDeletion(ctx context.Context, indexName,
	shardName, requestID string, uuid strfmt.UUID, schemaVersion uint64,
) SimpleResponse {
	index, simpleResp := rri.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.ReplicateDeletion(ctx, shardName, requestID, uuid)
}

func (rri *RemoteReplicaIncoming) ReplicateDeletions(ctx context.Context, indexName,
	shardName, requestID string, uuids []strfmt.UUID, dryRun bool, schemaVersion uint64,
) SimpleResponse {
	index, simpleResp := rri.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.ReplicateDeletions(ctx, shardName, requestID, uuids, dryRun, schemaVersion)
}

func (rri *RemoteReplicaIncoming) ReplicateReferences(ctx context.Context, indexName,
	shardName, requestID string, refs []objects.BatchReference, schemaVersion uint64,
) SimpleResponse {
	index, simpleResp := rri.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.ReplicateReferences(ctx, shardName, requestID, refs)
}

func (rri *RemoteReplicaIncoming) CommitReplication(indexName,
	shardName, requestID string,
) interface{} {
	index, simpleResp := rri.indexForIncomingRead(context.Background(), indexName)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.CommitReplication(shardName, requestID)
}

func (rri *RemoteReplicaIncoming) AbortReplication(indexName,
	shardName, requestID string,
) interface{} {
	index, simpleResp := rri.indexForIncomingRead(context.Background(), indexName)
	if simpleResp != nil {
		return *simpleResp
	}
	return index.AbortReplication(shardName, requestID)
}

func (rri *RemoteReplicaIncoming) OverwriteObjects(ctx context.Context,
	indexName, shardName string, vobjects []*objects.VObject,
) ([]RepairResponse, error) {
	index, simpleResp := rri.indexForIncomingRead(ctx, indexName)
	if simpleResp != nil {
		return nil, simpleResp.Errors[0].Err
	}
	return index.OverwriteObjects(ctx, shardName, vobjects)
}

func (rri *RemoteReplicaIncoming) FetchObject(ctx context.Context,
	indexName, shardName string, id strfmt.UUID,
) (objects.Replica, error) {
	index, simpleResp := rri.indexForIncomingRead(ctx, indexName)
	if simpleResp != nil {
		return objects.Replica{}, simpleResp.Errors[0].Err
	}
	return index.FetchObject(ctx, shardName, id)
}

func (rri *RemoteReplicaIncoming) FetchObjects(ctx context.Context,
	indexName, shardName string, ids []strfmt.UUID,
) ([]objects.Replica, error) {
	index, simpleResp := rri.indexForIncomingRead(ctx, indexName)
	if simpleResp != nil {
		return []objects.Replica{}, simpleResp.Errors[0].Err
	}
	return index.FetchObjects(ctx, shardName, ids)
}

func (rri *RemoteReplicaIncoming) DigestObjects(ctx context.Context,
	indexName, shardName string, ids []strfmt.UUID,
) (result []RepairResponse, err error) {
	index, simpleResp := rri.indexForIncomingRead(ctx, indexName)
	if simpleResp != nil {
		return []RepairResponse{}, simpleResp.Errors[0].Err
	}
	return index.DigestObjects(ctx, shardName, ids)
}

func (rri *RemoteReplicaIncoming) indexForIncomingRead(ctx context.Context, indexName string) (RemoteIndexIncomingRepo, *SimpleResponse) {
	index := rri.repo.GetIndexForIncomingReplica(schema.ClassName(indexName))
	if index == nil {
		return nil, &SimpleResponse{Errors: []Error{{Err: fmt.Errorf("local index %q not found", indexName)}}}
	}
	return index, nil
}

func (rri *RemoteReplicaIncoming) indexForIncomingWrite(ctx context.Context, indexName string,
	schemaVersion uint64,
) (RemoteIndexIncomingRepo, *SimpleResponse) {
	if err := rri.schema.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, &SimpleResponse{Errors: []Error{{Err: fmt.Errorf("error waiting for schema version %d: %w", schemaVersion, err)}}}
	}
	index := rri.repo.GetIndexForIncomingReplica(schema.ClassName(indexName))
	if index == nil {
		return nil, &SimpleResponse{Errors: []Error{{Err: fmt.Errorf("local index %q not found", indexName)}}}
	}
	return index, nil
}
