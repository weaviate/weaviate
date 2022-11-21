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

package sharding

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/replica"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type ReplicatedIndexFactory interface {
	GetReplicatedIndex(className schema.ClassName) Replicator
}

type Replicator interface {
	ReplicateObject(ctx context.Context, shardName, requestID string,
		object *storobj.Object) replica.SimpleResponse
	ReplicateObjects(ctx context.Context, shardName, requestID string,
		objects []*storobj.Object) replica.SimpleResponse
	ReplicateDeletion(ctx context.Context, shardName, requestID string,
		uuid strfmt.UUID) replica.SimpleResponse
	CommitReplication(ctx context.Context, shard,
		requestID string) interface{}
	AbortReplication(ctx context.Context, shardName,
		requestID string) interface{}
}

type ReplicatedIndex struct {
	repo ReplicatedIndexFactory
}

func NewReplicatedIndex(repo ReplicatedIndexFactory) *ReplicatedIndex {
	return &ReplicatedIndex{
		repo: repo,
	}
}

func (rii *ReplicatedIndex) ReplicateObject(ctx context.Context, indexName,
	shardName, requestID string, object *storobj.Object,
) replica.SimpleResponse {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return replica.SimpleResponse{
			Errors: []string{fmt.Sprintf("local index %q not found", indexName)},
		}
	}

	return index.ReplicateObject(ctx, shardName, requestID, object)
}

func (rii *ReplicatedIndex) ReplicateObjects(ctx context.Context, indexName,
	shardName, requestID string, objects []*storobj.Object,
) replica.SimpleResponse {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return replica.SimpleResponse{
			Errors: []string{fmt.Sprintf("local index %q not found", indexName)},
		}
	}

	return index.ReplicateObjects(ctx, shardName, requestID, objects)
}

func (rii *ReplicatedIndex) ReplicateDeletion(ctx context.Context, indexName,
	shardName, requestID string, uuid strfmt.UUID,
) replica.SimpleResponse {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return replica.SimpleResponse{
			Errors: []string{fmt.Sprintf("local index %q not found", indexName)},
		}
	}

	return index.ReplicateDeletion(ctx, shardName, requestID, uuid)
}

func (rii *ReplicatedIndex) CommitReplication(ctx context.Context, indexName,
	shardName, requestID string,
) interface{} {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return replica.SimpleResponse{
			Errors: []string{fmt.Sprintf("local index %q not found", indexName)},
		}
	}

	return index.CommitReplication(ctx, shardName, requestID)
}

func (rii *ReplicatedIndex) AbortReplication(ctx context.Context, indexName,
	shardName, requestID string,
) interface{} {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return replica.SimpleResponse{
			Errors: []string{fmt.Sprintf("local index %q not found", indexName)},
		}
	}

	return index.AbortReplication(ctx, shardName, requestID)
}
