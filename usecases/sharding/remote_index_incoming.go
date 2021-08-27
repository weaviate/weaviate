package sharding

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type RemoteIncomingRepo interface {
	GetIndexForIncoming(className schema.ClassName) RemoteIndexIncomingRepo
}

type RemoteIndexIncomingRepo interface {
	IncomingPutObject(ctx context.Context, shardName string,
		obj *storobj.Object) error
}

type RemoteIndexIncoming struct {
	repo RemoteIncomingRepo
}

func NewRemoteIndexIncoming(repo RemoteIncomingRepo) *RemoteIndexIncoming {
	return &RemoteIndexIncoming{
		repo: repo,
	}
}

func (rii *RemoteIndexIncoming) PutObject(ctx context.Context, indexName,
	shardName string, obj *storobj.Object) error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingPutObject(ctx, shardName, obj)
}
