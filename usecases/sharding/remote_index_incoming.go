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
	"io"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
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
	IncomingDeleteObject(ctx context.Context, shardName string,
		id strfmt.UUID) error
	IncomingMergeObject(ctx context.Context, shardName string,
		mergeDoc objects.MergeDocument) error
	IncomingMultiGetObjects(ctx context.Context, shardName string,
		ids []strfmt.UUID) ([]*storobj.Object, error)
	IncomingSearch(ctx context.Context, shardName string,
		vector []float32, distance float32, limit int, filters *filters.LocalFilter,
		keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
		additional additional.Properties) ([]*storobj.Object, []float32, error)
	IncomingAggregate(ctx context.Context, shardName string,
		params aggregation.Params) (*aggregation.Result, error)
	IncomingFindDocIDs(ctx context.Context, shardName string,
		filters *filters.LocalFilter) ([]uint64, error)
	IncomingDeleteObjectBatch(ctx context.Context, shardName string,
		docIDs []uint64, dryRun bool) objects.BatchSimpleObjects
	IncomingGetShardStatus(ctx context.Context, shardName string) (string, error)
	IncomingUpdateShardStatus(ctx context.Context, shardName, targetStatus string) error

	// Scale-Out Replication POC
	IncomingFilePutter(ctx context.Context, shardName,
		filePath string) (io.WriteCloser, error)
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
	shardName string, obj *storobj.Object,
) error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingPutObject(ctx, shardName, obj)
}

func (rii *RemoteIndexIncoming) BatchPutObjects(ctx context.Context, indexName,
	shardName string, objs []*storobj.Object,
) []error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return duplicateErr(errors.Errorf("local index %q not found", indexName),
			len(objs))
	}

	return index.IncomingBatchPutObjects(ctx, shardName, objs)
}

func (rii *RemoteIndexIncoming) BatchAddReferences(ctx context.Context, indexName,
	shardName string, refs objects.BatchReferences,
) []error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return duplicateErr(errors.Errorf("local index %q not found", indexName),
			len(refs))
	}

	return index.IncomingBatchAddReferences(ctx, shardName, refs)
}

func (rii *RemoteIndexIncoming) GetObject(ctx context.Context, indexName,
	shardName string, id strfmt.UUID, selectProperties search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingGetObject(ctx, shardName, id, selectProperties, additional)
}

func (rii *RemoteIndexIncoming) Exists(ctx context.Context, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return false, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingExists(ctx, shardName, id)
}

func (rii *RemoteIndexIncoming) DeleteObject(ctx context.Context, indexName,
	shardName string, id strfmt.UUID,
) error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingDeleteObject(ctx, shardName, id)
}

func (rii *RemoteIndexIncoming) MergeObject(ctx context.Context, indexName,
	shardName string, mergeDoc objects.MergeDocument,
) error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingMergeObject(ctx, shardName, mergeDoc)
}

func (rii *RemoteIndexIncoming) MultiGetObjects(ctx context.Context, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingMultiGetObjects(ctx, shardName, ids)
}

func (rii *RemoteIndexIncoming) Search(ctx context.Context, indexName, shardName string,
	vector []float32, distance float32, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingSearch(
		ctx, shardName, vector, distance, limit, filters, keywordRanking, sort, additional)
}

func (rii *RemoteIndexIncoming) Aggregate(ctx context.Context, indexName, shardName string,
	params aggregation.Params,
) (*aggregation.Result, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingAggregate(ctx, shardName, params)
}

func (rii *RemoteIndexIncoming) FindDocIDs(ctx context.Context, indexName, shardName string,
	filters *filters.LocalFilter,
) ([]uint64, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingFindDocIDs(ctx, shardName, filters)
}

func (rii *RemoteIndexIncoming) DeleteObjectBatch(ctx context.Context, indexName, shardName string,
	docIDs []uint64, dryRun bool,
) objects.BatchSimpleObjects {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		err := errors.Errorf("local index %q not found", indexName)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	return index.IncomingDeleteObjectBatch(ctx, shardName, docIDs, dryRun)
}

func (rii *RemoteIndexIncoming) GetShardStatus(ctx context.Context,
	indexName, shardName string,
) (string, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return "", errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingGetShardStatus(ctx, shardName)
}

func (rii *RemoteIndexIncoming) UpdateShardStatus(ctx context.Context,
	indexName, shardName, targetStatus string,
) error {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingUpdateShardStatus(ctx, shardName, targetStatus)
}

func (rii *RemoteIndexIncoming) FilePutter(ctx context.Context,
	indexName, shardName, filePath string,
) (io.WriteCloser, error) {
	index := rii.repo.GetIndexForIncoming(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingFilePutter(ctx, shardName, filePath)
}
