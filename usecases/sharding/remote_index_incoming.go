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

package sharding

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/weaviate/weaviate/entities/dto"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type RemoteIncomingRepo interface {
	GetIndexForIncomingSharding(className schema.ClassName) RemoteIndexIncomingRepo
}

type RemoteIncomingSchema interface {
	ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (*models.Class, error)
}

type RemoteIndexIncomingRepo interface {
	IncomingPutObject(ctx context.Context, shardName string,
		obj *storobj.Object, schemaVersion uint64) error
	IncomingBatchPutObjects(ctx context.Context, shardName string,
		objs []*storobj.Object, schemaVersion uint64) []error
	IncomingBatchAddReferences(ctx context.Context, shardName string,
		refs objects.BatchReferences, schemaVersion uint64) []error
	IncomingGetObject(ctx context.Context, shardName string, id strfmt.UUID,
		selectProperties search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	IncomingExists(ctx context.Context, shardName string,
		id strfmt.UUID) (bool, error)
	IncomingDeleteObject(ctx context.Context, shardName string,
		id strfmt.UUID, deletionTime time.Time, schemaVersion uint64) error
	IncomingMergeObject(ctx context.Context, shardName string,
		mergeDoc objects.MergeDocument, schemaVersion uint64) error
	IncomingMultiGetObjects(ctx context.Context, shardName string,
		ids []strfmt.UUID) ([]*storobj.Object, error)
	IncomingSearch(ctx context.Context, shardName string,
		vectors []models.Vector, targetVectors []string, distance float32, limit int,
		filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
		sort []filters.Sort, cursor *filters.Cursor, groupBy *searchparams.GroupBy,
		additional additional.Properties, targetCombination *dto.TargetCombination, properties []string,
	) ([]*storobj.Object, []float32, error)
	IncomingAggregate(ctx context.Context, shardName string,
		params aggregation.Params, modules interface{}) (*aggregation.Result, error)

	IncomingFindUUIDs(ctx context.Context, shardName string,
		filters *filters.LocalFilter) ([]strfmt.UUID, error)
	IncomingDeleteObjectBatch(ctx context.Context, shardName string,
		uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64) objects.BatchSimpleObjects
	IncomingGetShardQueueSize(ctx context.Context, shardName string) (int64, error)
	IncomingGetShardStatus(ctx context.Context, shardName string) (string, error)
	IncomingUpdateShardStatus(ctx context.Context, shardName, targetStatus string, schemaVersion uint64) error
	IncomingOverwriteObjects(ctx context.Context, shard string,
		vobjects []*objects.VObject) ([]types.RepairResponse, error)
	IncomingDigestObjects(ctx context.Context, shardName string,
		ids []strfmt.UUID) (result []types.RepairResponse, err error)
	IncomingDigestObjectsInRange(ctx context.Context, shardName string,
		initialUUID, finalUUID strfmt.UUID, limit int) (result []types.RepairResponse, err error)
	IncomingHashTreeLevel(ctx context.Context, shardName string,
		level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)

	// Scale-Out Replication POC
	IncomingFilePutter(ctx context.Context, shardName,
		filePath string) (io.WriteCloser, error)
	IncomingCreateShard(ctx context.Context, className string, shardName string) error
	IncomingReinitShard(ctx context.Context, shardName string) error
	// IncomingPauseAndListFiles See adapters/clients.RemoteIndex.PauseAndListFiles
	IncomingPauseAndListFiles(ctx context.Context, shardName string) ([]string, error)
	// IncomingGetFile See adapters/clients.RemoteIndex.GetFile
	IncomingGetFile(ctx context.Context, shardName, relativeFilePath string) (io.ReadCloser, error)
}

type RemoteIndexIncoming struct {
	repo    RemoteIncomingRepo
	schema  RemoteIncomingSchema
	modules interface{}
}

func NewRemoteIndexIncoming(repo RemoteIncomingRepo, schema RemoteIncomingSchema, modules interface{}) *RemoteIndexIncoming {
	return &RemoteIndexIncoming{
		repo:    repo,
		schema:  schema,
		modules: modules,
	}
}

func (rii *RemoteIndexIncoming) PutObject(ctx context.Context, indexName,
	shardName string, obj *storobj.Object, schemaVersion uint64,
) error {
	index, err := rii.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return err
	}

	return index.IncomingPutObject(ctx, shardName, obj, schemaVersion)
}

func (rii *RemoteIndexIncoming) BatchPutObjects(ctx context.Context, indexName,
	shardName string, objs []*storobj.Object, schemaVersion uint64,
) []error {
	index, err := rii.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return duplicateErr(err, len(objs))
	}

	return index.IncomingBatchPutObjects(ctx, shardName, objs, schemaVersion)
}

func (rii *RemoteIndexIncoming) BatchAddReferences(ctx context.Context, indexName,
	shardName string, refs objects.BatchReferences, schemaVersion uint64,
) []error {
	index, err := rii.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return duplicateErr(err, len(refs))
	}

	return index.IncomingBatchAddReferences(ctx, shardName, refs, schemaVersion)
}

func (rii *RemoteIndexIncoming) GetObject(ctx context.Context, indexName,
	shardName string, id strfmt.UUID, selectProperties search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingGetObject(ctx, shardName, id, selectProperties, additional)
}

func (rii *RemoteIndexIncoming) Exists(ctx context.Context, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return false, enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingExists(ctx, shardName, id)
}

func (rii *RemoteIndexIncoming) DeleteObject(ctx context.Context, indexName,
	shardName string, id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	index, err := rii.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return err
	}

	return index.IncomingDeleteObject(ctx, shardName, id, deletionTime, schemaVersion)
}

func (rii *RemoteIndexIncoming) MergeObject(ctx context.Context, indexName,
	shardName string, mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	index, err := rii.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return err
	}

	return index.IncomingMergeObject(ctx, shardName, mergeDoc, schemaVersion)
}

func (rii *RemoteIndexIncoming) MultiGetObjects(ctx context.Context, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingMultiGetObjects(ctx, shardName, ids)
}

func (rii *RemoteIndexIncoming) Search(ctx context.Context, indexName, shardName string,
	vectors []models.Vector, targetVectors []string, distance float32, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	groupBy *searchparams.GroupBy, additional additional.Properties, targetCombination *dto.TargetCombination,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, nil, enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingSearch(
		ctx, shardName, vectors, targetVectors, distance, limit, filters, keywordRanking, sort, cursor, groupBy, additional, targetCombination, properties)
}

func (rii *RemoteIndexIncoming) Aggregate(ctx context.Context, indexName, shardName string,
	params aggregation.Params,
) (*aggregation.Result, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingAggregate(ctx, shardName, params, rii.modules)
}

func (rii *RemoteIndexIncoming) FindUUIDs(ctx context.Context, indexName, shardName string,
	filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingFindUUIDs(ctx, shardName, filters)
}

func (rii *RemoteIndexIncoming) DeleteObjectBatch(ctx context.Context, indexName, shardName string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) objects.BatchSimpleObjects {
	index, err := rii.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	return index.IncomingDeleteObjectBatch(ctx, shardName, uuids, deletionTime, dryRun, schemaVersion)
}

func (rii *RemoteIndexIncoming) GetShardQueueSize(ctx context.Context,
	indexName, shardName string,
) (int64, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return 0, enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingGetShardQueueSize(ctx, shardName)
}

func (rii *RemoteIndexIncoming) GetShardStatus(ctx context.Context,
	indexName, shardName string,
) (string, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return "", enterrors.NewErrUnprocessable(errors.Errorf("local index %q not found", indexName))
	}

	return index.IncomingGetShardStatus(ctx, shardName)
}

func (rii *RemoteIndexIncoming) UpdateShardStatus(ctx context.Context,
	indexName, shardName, targetStatus string, schemaVersion uint64,
) error {
	index, err := rii.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return err
	}

	return index.IncomingUpdateShardStatus(ctx, shardName, targetStatus, schemaVersion)
}

func (rii *RemoteIndexIncoming) FilePutter(ctx context.Context,
	indexName, shardName, filePath string,
) (io.WriteCloser, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingFilePutter(ctx, shardName, filePath)
}

func (rii *RemoteIndexIncoming) CreateShard(ctx context.Context,
	indexName, shardName string,
) error {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingCreateShard(ctx, indexName, shardName)
}

func (rii *RemoteIndexIncoming) ReInitShard(ctx context.Context,
	indexName, shardName string,
) error {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingReinitShard(ctx, shardName)
}

// PauseAndListFiles see adapters/clients.RemoteIndex.PauseAndListFiles
func (rii *RemoteIndexIncoming) PauseAndListFiles(ctx context.Context,
	indexName, shardName string,
) ([]string, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingPauseAndListFiles(ctx, shardName)
}

// GetFile see adapters/clients.RemoteIndex.GetFile
func (rii *RemoteIndexIncoming) GetFile(ctx context.Context,
	indexName, shardName, relativeFilePath string,
) (io.ReadCloser, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, errors.Errorf("local index %q not found", indexName)
	}

	return index.IncomingGetFile(ctx, shardName, relativeFilePath)
}

func (rii *RemoteIndexIncoming) OverwriteObjects(ctx context.Context,
	indexName, shardName string, vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, fmt.Errorf("local index %q not found", indexName)
	}

	return index.IncomingOverwriteObjects(ctx, shardName, vobjects)
}

func (rii *RemoteIndexIncoming) DigestObjects(ctx context.Context,
	indexName, shardName string, ids []strfmt.UUID,
) ([]types.RepairResponse, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local index %q not found", indexName))
	}

	return index.IncomingDigestObjects(ctx, shardName, ids)
}

func (rii *RemoteIndexIncoming) indexForIncomingWrite(ctx context.Context, indexName string,
	schemaVersion uint64,
) (RemoteIndexIncomingRepo, error) {
	// wait for schema and store to reach version >= schemaVersion
	if _, err := rii.schema.ReadOnlyClassWithVersion(ctx, indexName, schemaVersion); err != nil {
		return nil, fmt.Errorf("local index %q not found: %w", indexName, err)
	}
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, fmt.Errorf("local index %q not found", indexName)
	}

	return index, nil
}

func (rii *RemoteIndexIncoming) DigestObjectsInRange(ctx context.Context,
	indexName, shardName string, initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, fmt.Errorf("local index %q not found", indexName)
	}

	return index.IncomingDigestObjectsInRange(ctx, shardName, initialUUID, finalUUID, limit)
}

func (rii *RemoteIndexIncoming) HashTreeLevel(ctx context.Context,
	indexName, shardName string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	index := rii.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, fmt.Errorf("local index %q not found", indexName)
	}

	return index.IncomingHashTreeLevel(ctx, shardName, level, discriminant)
}
