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

package db

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/dto"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

type LazyLoadShard struct {
	shardOpts        *deferredShardOpts
	shard            *Shard
	loaded           bool
	mutex            sync.Mutex
	memMonitor       memwatch.AllocChecker
	shardLoadLimiter ShardLoadLimiter
	lazyLoadSegments bool
}

func NewLazyLoadShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	indexCheckpoints *indexcheckpoint.Checkpoints, memMonitor memwatch.AllocChecker,
	shardLoadLimiter ShardLoadLimiter, shardReindexer ShardReindexerV3, lazyLoadSegments bool,
) *LazyLoadShard {
	if memMonitor == nil {
		memMonitor = memwatch.NewDummyMonitor()
	}
	promMetrics.NewUnloadedshard()
	return &LazyLoadShard{
		shardOpts: &deferredShardOpts{
			promMetrics:      promMetrics,
			name:             shardName,
			index:            index,
			class:            class,
			jobQueueCh:       jobQueueCh,
			scheduler:        index.scheduler,
			indexCheckpoints: indexCheckpoints,
			shardReindexer:   shardReindexer,
		},
		memMonitor:       memMonitor,
		shardLoadLimiter: shardLoadLimiter,
		lazyLoadSegments: lazyLoadSegments,
	}
}

type deferredShardOpts struct {
	promMetrics      *monitoring.PrometheusMetrics
	name             string
	index            *Index
	class            *models.Class
	jobQueueCh       chan job
	scheduler        *queue.Scheduler
	indexCheckpoints *indexcheckpoint.Checkpoints
	shardReindexer   ShardReindexerV3
}

func (l *LazyLoadShard) mustLoad() {
	l.mustLoadCtx(context.Background())
}

func (l *LazyLoadShard) mustLoadCtx(ctx context.Context) {
	if err := l.Load(ctx); err != nil {
		panic(err.Error())
	}
}

func (l *LazyLoadShard) Load(ctx context.Context) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.loaded {
		return nil
	}

	if err := l.memMonitor.CheckMappingAndReserve(3, int(lsmkv.FlushAfterDirtyDefault.Seconds())); err != nil {
		return errors.Wrap(err, "memory pressure: cannot load shard")
	}

	if err := l.shardLoadLimiter.Acquire(ctx); err != nil {
		return fmt.Errorf("acquiring permit to load shard: %w", err)
	}
	defer l.shardLoadLimiter.Release()

	shard, err := NewShard(ctx, l.shardOpts.promMetrics, l.shardOpts.name, l.shardOpts.index,
		l.shardOpts.class, l.shardOpts.jobQueueCh, l.shardOpts.scheduler,
		l.shardOpts.indexCheckpoints, l.shardOpts.shardReindexer, l.lazyLoadSegments)
	if err != nil {
		msg := fmt.Sprintf("Unable to load shard %s: %v", l.shardOpts.name, err)
		l.shardOpts.index.logger.WithField("error", "shard_load").WithError(err).Error(msg)
		return errors.New(msg)
	}

	l.shard = shard
	l.loaded = true

	return nil
}

func (l *LazyLoadShard) Index() *Index {
	return l.shardOpts.index
}

func (l *LazyLoadShard) Name() string {
	return l.shardOpts.name
}

func (l *LazyLoadShard) Store() *lsmkv.Store {
	l.mustLoad()
	return l.shard.Store()
}

func (l *LazyLoadShard) NotifyReady() {
	l.mustLoad()
	l.shard.NotifyReady()
}

func (l *LazyLoadShard) GetStatusNoLoad() storagestate.Status {
	if l.loaded {
		return l.shard.GetStatus()
	}
	return storagestate.StatusLoading
}

func (l *LazyLoadShard) GetStatus() storagestate.Status {
	l.mustLoad()
	return l.shard.GetStatus()
}

func (l *LazyLoadShard) UpdateStatus(status string) error {
	l.mustLoad()
	return l.shard.UpdateStatus(status)
}

func (l *LazyLoadShard) SetStatusReadonly(reason string) error {
	l.mustLoad()
	return l.shard.SetStatusReadonly(reason)
}

func (l *LazyLoadShard) FindUUIDs(ctx context.Context, filters *filters.LocalFilter) ([]strfmt.UUID, error) {
	if err := l.Load(ctx); err != nil {
		return []strfmt.UUID{}, err
	}
	return l.shard.FindUUIDs(ctx, filters)
}

func (l *LazyLoadShard) Counter() *indexcounter.Counter {
	l.mustLoad()
	return l.shard.Counter()
}

func (l *LazyLoadShard) ObjectCount() int {
	l.mustLoad()
	return l.shard.ObjectCount()
}

func (l *LazyLoadShard) ObjectCountAsync() int {
	l.mutex.Lock()
	if !l.loaded {
		l.mutex.Unlock()
		return 0
	}
	l.mutex.Unlock()
	return l.shard.ObjectCountAsync()
}

func (l *LazyLoadShard) GetPropertyLengthTracker() *inverted.JsonShardMetaData {
	l.mustLoad()
	return l.shard.GetPropertyLengthTracker()
}

func (l *LazyLoadShard) PutObject(ctx context.Context, object *storobj.Object) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.PutObject(ctx, object)
}

func (l *LazyLoadShard) PutObjectBatch(ctx context.Context, objects []*storobj.Object) []error {
	if err := l.Load(ctx); err != nil {
		return []error{err}
	} // TODO check
	return l.shard.PutObjectBatch(ctx, objects)
}

func (l *LazyLoadShard) ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.ObjectByID(ctx, id, props, additional)
}

func (l *LazyLoadShard) ObjectByIDErrDeleted(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.ObjectByIDErrDeleted(ctx, id, props, additional)
}

func (l *LazyLoadShard) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	if err := l.Load(ctx); err != nil {
		return false, err
	}
	return l.shard.Exists(ctx, id)
}

func (l *LazyLoadShard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, properties []string) ([]*storobj.Object, []float32, error) {
	if err := l.Load(ctx); err != nil {
		return nil, nil, err
	}
	return l.shard.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, additional, properties)
}

func (l *LazyLoadShard) ObjectVectorSearch(ctx context.Context, searchVectors []models.Vector, targetVectors []string, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties, targetCombination *dto.TargetCombination, properties []string) ([]*storobj.Object, []float32, error) {
	if err := l.Load(ctx); err != nil {
		return nil, nil, err
	}
	return l.shard.ObjectVectorSearch(ctx, searchVectors, targetVectors, targetDist, limit, filters, sort, groupBy, additional, targetCombination, properties)
}

func (l *LazyLoadShard) UpdateVectorIndexConfig(ctx context.Context, updated schemaConfig.VectorIndexConfig) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.UpdateVectorIndexConfig(ctx, updated)
}

func (l *LazyLoadShard) UpdateVectorIndexConfigs(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.UpdateVectorIndexConfigs(ctx, updated)
}

func (l *LazyLoadShard) SetAsyncReplicationEnabled(ctx context.Context, enabled bool) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.SetAsyncReplicationEnabled(ctx, enabled)
}

func (l *LazyLoadShard) addTargetNodeOverride(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	l.shard.addTargetNodeOverride(ctx, targetNodeOverride)
	return nil
}

func (l *LazyLoadShard) removeTargetNodeOverride(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	l.shard.removeTargetNodeOverride(ctx, targetNodeOverride)
	return nil
}

func (l *LazyLoadShard) removeAllTargetNodeOverrides(ctx context.Context) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.removeAllTargetNodeOverrides(ctx)
}

func (l *LazyLoadShard) getAsyncReplicationStats(ctx context.Context) []*models.AsyncReplicationStatus {
	if err := l.Load(ctx); err != nil {
		return nil
	}
	return l.shard.getAsyncReplicationStats(ctx)
}

func (l *LazyLoadShard) AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error {
	if err := l.Load(ctx); err != nil {
		return []error{err}
	} // TODO check
	return l.shard.AddReferencesBatch(ctx, refs)
}

func (l *LazyLoadShard) DeleteObjectBatch(ctx context.Context, ids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects {
	l.mustLoadCtx(ctx)
	return l.shard.DeleteObjectBatch(ctx, ids, deletionTime, dryRun)
}

func (l *LazyLoadShard) DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.DeleteObject(ctx, id, deletionTime)
}

func (l *LazyLoadShard) MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.MultiObjectByID(ctx, query)
}

func (l *LazyLoadShard) ObjectDigestsInRange(ctx context.Context,
	initialUUID, finalUUID strfmt.UUID, limit int,
) (objs []types.RepairResponse, err error) {
	if !l.isLoaded() {
		return nil, err
	}

	return l.shard.ObjectDigestsInRange(ctx, initialUUID, finalUUID, limit)
}

func (l *LazyLoadShard) ID() string {
	return shardId(l.shardOpts.index.ID(), l.shardOpts.name)
}

func (l *LazyLoadShard) drop() error {
	// if not loaded, execute simplified drop without loading shard:
	// - perform required actions
	// - remove entire shard directory
	// use lock to prevent eventual concurrent droping and loading
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.loaded {
		idx := l.shardOpts.index
		className := idx.Config.ClassName.String()
		shardName := l.shardOpts.name

		// cleanup metrics
		NewMetrics(idx.logger, l.shardOpts.promMetrics, className, shardName).
			DeleteShardLabels(className, shardName)

		// cleanup dimensions
		if idx.Config.TrackVectorDimensions {
			clearDimensionMetrics(l.shardOpts.promMetrics, className, shardName)
		}

		// cleanup index checkpoints
		if l.shardOpts.indexCheckpoints != nil {
			if err := l.shardOpts.index.indexCheckpoints.DeleteShard(l.ID()); err != nil {
				return fmt.Errorf("delete shard index checkpoints: %w", err)
			}
		}

		// remove shard dir
		if err := os.RemoveAll(shardPath(idx.path(), shardName)); err != nil {
			return fmt.Errorf("delete shard dir: %w", err)
		}

		return nil
	}

	return l.shard.drop()
}

func (l *LazyLoadShard) DebugResetVectorIndex(ctx context.Context, targetVector string) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.DebugResetVectorIndex(ctx, targetVector)
}

func (l *LazyLoadShard) initPropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper, lazyLoadSegments bool, props ...*models.Property) {
	l.mustLoad()
	l.shard.initPropertyBuckets(ctx, eg, lazyLoadSegments, props...)
}

func (l *LazyLoadShard) HaltForTransfer(ctx context.Context, offloading bool, inactivityTimeout time.Duration) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.HaltForTransfer(ctx, offloading, inactivityTimeout)
}

func (l *LazyLoadShard) ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.ListBackupFiles(ctx, ret)
}

func (l *LazyLoadShard) resumeMaintenanceCycles(ctx context.Context) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.resumeMaintenanceCycles(ctx)
}

func (l *LazyLoadShard) GetFileMetadata(ctx context.Context, relativeFilePath string) (file.FileMetadata, error) {
	if err := l.Load(ctx); err != nil {
		return file.FileMetadata{}, err
	}
	return l.shard.GetFileMetadata(ctx, relativeFilePath)
}

func (l *LazyLoadShard) GetFile(ctx context.Context, relativeFilePath string) (io.ReadCloser, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.GetFile(ctx, relativeFilePath)
}

func (l *LazyLoadShard) SetPropertyLengths(props []inverted.Property) error {
	l.mustLoad()
	return l.shard.SetPropertyLengths(props)
}

func (l *LazyLoadShard) AnalyzeObject(object *storobj.Object) ([]inverted.Property, []inverted.NilProperty, error) {
	l.mustLoad()
	return l.shard.AnalyzeObject(object)
}

func (l *LazyLoadShard) Dimensions(ctx context.Context, targetVector string) int {
	l.mustLoad()
	return l.shard.Dimensions(ctx, targetVector)
}

func (l *LazyLoadShard) QuantizedDimensions(ctx context.Context, targetVector string, segments int) int {
	l.mustLoad()
	return l.shard.QuantizedDimensions(ctx, targetVector, segments)
}

func (l *LazyLoadShard) publishDimensionMetrics(ctx context.Context) {
	l.mustLoad()
	l.shard.publishDimensionMetrics(ctx)
}

func (l *LazyLoadShard) resetDimensionsLSM() error {
	l.mustLoad()
	return l.shard.resetDimensionsLSM()
}

func (l *LazyLoadShard) Aggregate(ctx context.Context, params aggregation.Params, modules *modules.Provider) (*aggregation.Result, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.Aggregate(ctx, params, modules)
}

func (l *LazyLoadShard) MergeObject(ctx context.Context, object objects.MergeDocument) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.MergeObject(ctx, object)
}

func (l *LazyLoadShard) GetVectorIndexQueue(targetVector string) (*VectorIndexQueue, bool) {
	l.mustLoad()
	return l.shard.GetVectorIndexQueue(targetVector)
}

func (l *LazyLoadShard) GetVectorIndex(targetVector string) (VectorIndex, bool) {
	l.mustLoad()
	return l.shard.GetVectorIndex(targetVector)
}

func (l *LazyLoadShard) ForEachVectorIndex(f func(targetVector string, index VectorIndex) error) error {
	l.mustLoad()
	return l.shard.ForEachVectorIndex(f)
}

func (l *LazyLoadShard) ForEachVectorQueue(f func(targetVector string, queue *VectorIndexQueue) error) error {
	l.mustLoad()
	return l.shard.ForEachVectorQueue(f)
}

func (l *LazyLoadShard) VectorDistanceForQuery(ctx context.Context, id uint64, searchVectors []models.Vector, targets []string) ([]float32, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.VectorDistanceForQuery(ctx, id, searchVectors, targets)
}

func (l *LazyLoadShard) ConvertQueue(targetVector string) error {
	l.mustLoad()
	return l.shard.ConvertQueue(targetVector)
}

func (l *LazyLoadShard) FillQueue(targetVector string, from uint64) error {
	l.mustLoad()
	return l.shard.FillQueue(targetVector, from)
}

func (l *LazyLoadShard) RepairIndex(ctx context.Context, targetVector string) error {
	l.mustLoad()
	return l.shard.RepairIndex(ctx, targetVector)
}

func (l *LazyLoadShard) Shutdown(ctx context.Context) error {
	if !l.isLoaded() {
		return nil
	}
	return l.shard.Shutdown(ctx)
}

func (l *LazyLoadShard) preventShutdown() (release func(), err error) {
	if err := l.Load(context.Background()); err != nil {
		return nil, fmt.Errorf("LazyLoadShard::preventShutdown: %w", err)
	}
	return l.shard.preventShutdown()
}

func (l *LazyLoadShard) HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	if !l.isLoaded() {
		return []hashtree.Digest{}, nil
	}
	return l.shard.HashTreeLevel(ctx, level, discriminant)
}

func (l *LazyLoadShard) ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.ObjectList(ctx, limit, sort, cursor, additional, className)
}

func (l *LazyLoadShard) WasDeleted(ctx context.Context, id strfmt.UUID) (bool, time.Time, error) {
	if err := l.Load(ctx); err != nil {
		return false, time.Time{}, err
	}
	return l.shard.WasDeleted(ctx, id)
}

func (l *LazyLoadShard) Versioner() *shardVersioner {
	l.mustLoad()
	return l.shard.Versioner()
}

func (l *LazyLoadShard) isReadOnly() error {
	l.mustLoad()
	return l.shard.isReadOnly()
}

func (l *LazyLoadShard) preparePutObject(ctx context.Context, shardID string, object *storobj.Object) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.preparePutObject(ctx, shardID, object)
}

func (l *LazyLoadShard) preparePutObjects(ctx context.Context, shardID string, objects []*storobj.Object) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.preparePutObjects(ctx, shardID, objects)
}

func (l *LazyLoadShard) prepareMergeObject(ctx context.Context, shardID string, object *objects.MergeDocument) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.prepareMergeObject(ctx, shardID, object)
}

func (l *LazyLoadShard) prepareDeleteObject(ctx context.Context, shardID string, id strfmt.UUID, deletionTime time.Time) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.prepareDeleteObject(ctx, shardID, id, deletionTime)
}

func (l *LazyLoadShard) prepareDeleteObjects(ctx context.Context, shardID string,
	ids []strfmt.UUID, deletionTime time.Time, dryRun bool,
) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.prepareDeleteObjects(ctx, shardID, ids, deletionTime, dryRun)
}

func (l *LazyLoadShard) prepareAddReferences(ctx context.Context, shardID string, refs []objects.BatchReference) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.prepareAddReferences(ctx, shardID, refs)
}

func (l *LazyLoadShard) commitReplication(ctx context.Context, shardID string, mutex *shardTransfer) interface{} {
	l.mustLoad()
	return l.shard.commitReplication(ctx, shardID, mutex)
}

func (l *LazyLoadShard) abortReplication(ctx context.Context, shardID string) replica.SimpleResponse {
	l.mustLoad()
	return l.shard.abortReplication(ctx, shardID)
}

func (l *LazyLoadShard) filePutter(ctx context.Context, shardID string) (io.WriteCloser, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.filePutter(ctx, shardID)
}

func (l *LazyLoadShard) extendDimensionTrackerLSM(dimLength int, docID uint64, targetVector string) error {
	if err := l.Load(context.Background()); err != nil {
		return err
	}
	return l.shard.extendDimensionTrackerLSM(dimLength, docID, targetVector)
}

func (l *LazyLoadShard) addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.addToPropertySetBucket(bucket, docID, key)
}

func (l *LazyLoadShard) addToPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.addToPropertyRangeBucket(bucket, docID, key)
}

func (l *LazyLoadShard) addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error {
	l.mustLoad()
	return l.shard.addToPropertyMapBucket(bucket, pair, key)
}

func (l *LazyLoadShard) pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair {
	l.mustLoad()
	return l.shard.pairPropertyWithFrequency(docID, freq, propLen)
}

func (l *LazyLoadShard) setFallbackToSearchable(fallback bool) {
	l.mustLoad()
	l.shard.setFallbackToSearchable(fallback)
}

func (l *LazyLoadShard) addJobToQueue(job job) {
	l.mustLoad()
	l.shard.addJobToQueue(job)
}

func (l *LazyLoadShard) uuidFromDocID(docID uint64) (strfmt.UUID, error) {
	l.mustLoad()
	return l.shard.uuidFromDocID(docID)
}

func (l *LazyLoadShard) batchDeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.batchDeleteObject(ctx, id, deletionTime)
}

func (l *LazyLoadShard) putObjectLSM(object *storobj.Object, idBytes []byte) (objectInsertStatus, error) {
	l.mustLoad()
	return l.shard.putObjectLSM(object, idBytes)
}

func (l *LazyLoadShard) mayUpsertObjectHashTree(object *storobj.Object, idBytes []byte, status objectInsertStatus) error {
	l.mustLoad()
	return l.shard.mayUpsertObjectHashTree(object, idBytes, status)
}

func (l *LazyLoadShard) mutableMergeObjectLSM(merge objects.MergeDocument, idBytes []byte) (mutableMergeResult, error) {
	l.mustLoad()
	return l.shard.mutableMergeObjectLSM(merge, idBytes)
}

func (l *LazyLoadShard) deleteFromPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.deleteFromPropertySetBucket(bucket, docID, key)
}

func (l *LazyLoadShard) deleteFromPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.deleteFromPropertyRangeBucket(bucket, docID, key)
}

func (l *LazyLoadShard) batchExtendInvertedIndexItemsLSMNoFrequency(b *lsmkv.Bucket, item inverted.MergeItem) error {
	l.mustLoad()
	return l.shard.batchExtendInvertedIndexItemsLSMNoFrequency(b, item)
}

func (l *LazyLoadShard) updatePropertySpecificIndices(ctx context.Context, object *storobj.Object, status objectInsertStatus) error {
	l.mustLoad()
	return l.shard.updatePropertySpecificIndices(ctx, object, status)
}

func (l *LazyLoadShard) updateVectorIndexIgnoreDelete(ctx context.Context, vector []float32, status objectInsertStatus) error {
	l.mustLoad()
	return l.shard.updateVectorIndexIgnoreDelete(ctx, vector, status)
}

func (l *LazyLoadShard) updateVectorIndexesIgnoreDelete(ctx context.Context, vectors map[string][]float32, status objectInsertStatus) error {
	l.mustLoad()
	return l.shard.updateVectorIndexesIgnoreDelete(ctx, vectors, status)
}

func (l *LazyLoadShard) updateMultiVectorIndexesIgnoreDelete(ctx context.Context, multiVectors map[string][][]float32, status objectInsertStatus) error {
	l.mustLoad()
	return l.shard.updateMultiVectorIndexesIgnoreDelete(ctx, multiVectors, status)
}

func (l *LazyLoadShard) hasGeoIndex() bool {
	l.mustLoad()
	return l.shard.hasGeoIndex()
}

func (l *LazyLoadShard) Metrics() *Metrics {
	l.mustLoad()
	return l.shard.Metrics()
}

func (l *LazyLoadShard) isLoaded() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.loaded
}

func (l *LazyLoadShard) Activity() (int32, int32) {
	var loaded bool
	l.mutex.Lock()
	loaded = l.loaded
	l.mutex.Unlock()

	if !loaded {
		// don't force-load the shard, just report the same number every time, so
		// the caller can figure out there was no activity
		return 0, 0
	}

	return l.shard.Activity()
}

func (l *LazyLoadShard) pathLSM() string {
	return shardPathLSM(l.shardOpts.index.path(), l.shardOpts.name)
}
