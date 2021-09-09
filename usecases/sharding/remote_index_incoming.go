package sharding

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

type RemoteIncomingRepo interface {
	GetIndexForIncoming(className schema.ClassName) RemoteIndexIncomingRepo
}

type RemoteIndexIncomingRepo interface {
	IncomingPutObject(ctx context.Context, shardName string,
		obj *storobj.Object) error
	IncomingBatchPutObjects(ctx context.Context, shardName string,
		objs []*storobj.Object) []error
	IncomingBatchAddReferences(ctx context.Context, shardName string,
		refs objects.BatchReferences) []error
	IncomingGetObject(ctx context.Context, shardName string, id strfmt.UUID,
		selectProperties search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	IncomingExists(ctx context.Context, shardName string,
		id strfmt.UUID) (bool, error)
	IncomingMultiGetObjects(ctx context.Context, shardName string,
		ids []strfmt.UUID) ([]*storobj.Object, error)
	IncomingSearch(ctx context.Context, shardName string,
		vector []float32, limit int, filters *filters.LocalFilter,
		additional additional.Properties) ([]*storobj.Object, []float32, error)
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

func (rii *RemoteIndexIncoming) BatchPutObjects(ctx context.Context, indexName,
	shardName string, objs []*storobj.Object) []error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return duplicateErr(errors.Errorf("local index %q not found", indexName),
			len(objs))
	}

	return index.IncomingBatchPutObjects(ctx, shardName, objs)
}

func (rii *RemoteIndexIncoming) BatchAddReferences(ctx context.Context, indexName,
	shardName string, refs objects.BatchReferences) []error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return duplicateErr(errors.Errorf("local index %q not found", indexName),
			len(refs))
	}

	return index.IncomingBatchAddReferences(ctx, shardName, refs)
}

func (rii *RemoteIndexIncoming) GetObject(ctx context.Context, indexName,
	shardName string, id strfmt.UUID, selectProperties search.SelectProperties,
	additional additional.Properties) (*storobj.Object, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingGetObject(ctx, shardName, id, selectProperties, additional)
}

func (rii *RemoteIndexIncoming) Exists(ctx context.Context, indexName,
	shardName string, id strfmt.UUID) (bool, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return false, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingExists(ctx, shardName, id)
}

func (rii *RemoteIndexIncoming) MultiGetObjects(ctx context.Context, indexName,
	shardName string, ids []strfmt.UUID) ([]*storobj.Object, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingMultiGetObjects(ctx, shardName, ids)
}

func (rii *RemoteIndexIncoming) Search(ctx context.Context, indexName, shardName string,
	vector []float32, limit int, filters *filters.LocalFilter,
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingSearch(ctx, shardName, vector, limit, filters, additional)
}
