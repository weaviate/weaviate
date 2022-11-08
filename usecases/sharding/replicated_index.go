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

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type ReplicatedIndexFactory interface {
	GetReplicatedIndex(className schema.ClassName) Replicator
}

type Replicator interface {
	PutObject(ctx context.Context, shardName string,
		obj *storobj.Object) error
	DeleteObject(ctx context.Context, shardName string,
		id strfmt.UUID) error
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

func (rii *ReplicatedIndex) DeleteObject(ctx context.Context, indexName,
	shardName string, id strfmt.UUID,
) error {
	index := rii.repo.GetReplicatedIndex(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.DeleteObject(ctx, shardName, id)
}
