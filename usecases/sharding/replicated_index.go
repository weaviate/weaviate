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
	"github.com/pkg/errors"
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
	CommitReplication(ctx context.Context, shard,
		requestID string) replica.SimpleResponse
	PutObject(ctx context.Context, shardName string,
		obj *storobj.Object) error
	DeleteObject(ctx context.Context, shardName string,
		id strfmt.UUID) error
	BatchPutObjects(ctx context.Context, shardName string,
		objs []*storobj.Object) []error
}

type ReplicatedIndex struct {
	repo ReplicatedIndexFactory
}

func NewReplicatedIndex(repo ReplicatedIndexFactory) *ReplicatedIndex {
	return &ReplicatedIndex{
		repo: repo,
	}
}

func (rii *ReplicatedIndex) PutObject(ctx context.Context, indexName,
	shardName string, obj *storobj.Object,
) error {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.PutObject(ctx, shardName, obj)
}

func (rii *ReplicatedIndex) ReplicateObject(ctx context.Context, indexName,
	shardName, requestID string, object *storobj.Object,
) replica.SimpleResponse {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return replica.SimpleResponse{
			Errors: []string{fmt.Sprintf("local index %q not found", indexName)}}
	}

	return index.ReplicateObject(ctx, shardName, requestID, object)
}

func (rii *ReplicatedIndex) CommitReplication(ctx context.Context, indexName,
	shardName, requestID string,
) replica.SimpleResponse {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return replica.SimpleResponse{
			Errors: []string{fmt.Sprintf("local index %q not found", indexName)}}
	}

	return index.CommitReplication(ctx, shardName, requestID)
}

func (rii *ReplicatedIndex) BatchPutObjects(ctx context.Context, indexName,
	shardName string, objs []*storobj.Object,
) []error {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return []error{errors.Errorf("local index %q not found", indexName)}
	}

	return index.BatchPutObjects(ctx, shardName, objs)
}

func (rii *ReplicatedIndex) DeleteObject(ctx context.Context, indexName,
	shardName string, id strfmt.UUID,
) error {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.DeleteObject(ctx, shardName, id)
}
