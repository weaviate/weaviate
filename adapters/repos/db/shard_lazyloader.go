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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"golang.org/x/sync/errgroup"
)

type LazyLoadShard struct {
	shardOpts      *deferredShardOpts
	propLenTracker *inverted.JsonShardMetaData
	shard          *Shard
	loaded         bool
	mutex          sync.Mutex
}

func NewLazyLoadShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	indexCheckpoints *indexcheckpoint.Checkpoints,
) *LazyLoadShard {
	promMetrics.NewUnloadedshard(class.Class)
	return &LazyLoadShard{
		shardOpts: &deferredShardOpts{
			promMetrics: promMetrics,

			name:             shardName,
			index:            index,
			class:            class,
			jobQueueCh:       jobQueueCh,
			indexCheckpoints: indexCheckpoints,
		},
	}
}

type deferredShardOpts struct {
	promMetrics      *monitoring.PrometheusMetrics
	name             string
	index            *Index
	class            *models.Class
	jobQueueCh       chan job
	indexCheckpoints *indexcheckpoint.Checkpoints
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

	l.propLenTracker = nil

	if l.loaded {
		return nil
	}
	if l.shardOpts.class == nil {
		l.shardOpts.promMetrics.StartLoadingShard("unknown class")
	} else {
		l.shardOpts.promMetrics.StartLoadingShard(l.shardOpts.class.Class)
	}
	shard, err := NewShard(ctx, l.shardOpts.promMetrics, l.shardOpts.name, l.shardOpts.index,
		l.shardOpts.class, l.shardOpts.jobQueueCh, l.shardOpts.indexCheckpoints, l.propLenTracker)
	if err != nil {
		msg := fmt.Sprintf("Unable to load shard %s: %v", l.shardOpts.name, err)
		l.shardOpts.index.logger.WithField("error", "shard_load").WithError(err).Error(msg)
		return errors.New(msg)
	}
	l.shard = shard
	l.loaded = true
	if l.shardOpts.class == nil {
		l.shardOpts.promMetrics.FinishLoadingShard("unknown class")
	} else {
		l.shardOpts.promMetrics.FinishLoadingShard(l.shardOpts.class.Class)
	}
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

func (l *LazyLoadShard) GetStatus() storagestate.Status {
	l.mustLoad()
	return l.shard.GetStatus()
}

func (l *LazyLoadShard) UpdateStatus(status string) error {
	l.mustLoad()
	return l.shard.UpdateStatus(status)
}

func (l *LazyLoadShard) ChangeObjectCountBy(delta int) error {
	if err := l.Load(context.Background()); err != nil {
		return err
	}
	return l.shard.ChangeObjectCountBy(delta)
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
	return l.GetPropertyLengthTracker().ObjectTally()
}

func (l *LazyLoadShard) GetPropertyLengthTracker() *inverted.JsonShardMetaData {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.loaded {
		return l.shard.GetPropertyLengthTracker()
	}

	if l.propLenTracker != nil {
		return l.propLenTracker
	}

	var tracker *inverted.JsonShardMetaData

	// FIXME add method for tracker path
	plPath := path.Join(l.shardOpts.index.path(), "proplengths")
	tracker, err := inverted.NewJsonShardMetaData(plPath, l.shardOpts.index.logger)
	l.propLenTracker = tracker
	if err != nil {
		panic(fmt.Sprintf("could not create property length tracker at %v: %v", plPath, err))
	}

	return l.propLenTracker
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

func (l *LazyLoadShard) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	if err := l.Load(ctx); err != nil {
		return false, err
	}
	return l.shard.Exists(ctx, id)
}

func (l *LazyLoadShard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	if err := l.Load(ctx); err != nil {
		return nil, nil, err
	}
	return l.shard.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, additional)
}

func (l *LazyLoadShard) ObjectVectorSearch(ctx context.Context, searchVector []float32, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	if err := l.Load(ctx); err != nil {
		return nil, nil, err
	}
	return l.shard.ObjectVectorSearch(ctx, searchVector, targetDist, limit, filters, sort, groupBy, additional)
}

func (l *LazyLoadShard) UpdateVectorIndexConfig(ctx context.Context, updated schema.VectorIndexConfig) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.UpdateVectorIndexConfig(ctx, updated)
}

func (l *LazyLoadShard) AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error {
	if err := l.Load(ctx); err != nil {
		return []error{err}
	} // TODO check
	return l.shard.AddReferencesBatch(ctx, refs)
}

func (l *LazyLoadShard) DeleteObjectBatch(ctx context.Context, ids []strfmt.UUID, dryRun bool) objects.BatchSimpleObjects {
	l.mustLoadCtx(ctx)
	return l.shard.DeleteObjectBatch(ctx, ids, dryRun)
}

func (l *LazyLoadShard) DeleteObject(ctx context.Context, id strfmt.UUID) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.DeleteObject(ctx, id)
}

func (l *LazyLoadShard) MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.MultiObjectByID(ctx, query)
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
	if !l.loaded {
		defer l.mutex.Unlock()

		idx := l.shardOpts.index
		className := idx.Config.ClassName.String()
		shardName := l.shardOpts.name

		// cleanup metrics
		NewMetrics(idx.logger, l.shardOpts.promMetrics, className, shardName).
			DeleteShardLabels(className, shardName)

		// cleanup dimensions
		if idx.Config.TrackVectorDimensions {
			clearDimensionMetrics(l.shardOpts.promMetrics, className, shardName, idx.vectorIndexUserConfig)
		}

		// cleanup queue
		if l.shardOpts.indexCheckpoints != nil {
			if err := l.shardOpts.indexCheckpoints.Delete(shardId(idx.ID(), shardName)); err != nil {
				return fmt.Errorf("delete checkpoint: %w", err)
			}
		}

		// remove shard dir
		if err := os.RemoveAll(shardPath(idx.path(), shardName)); err != nil {
			return fmt.Errorf("delete shard dir: %w", err)
		}

		return nil
	}
	l.mutex.Unlock()

	return l.shard.drop()
}

func (l *LazyLoadShard) addIDProperty(ctx context.Context) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.addIDProperty(ctx)
}

func (l *LazyLoadShard) addDimensionsProperty(ctx context.Context) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.addDimensionsProperty(ctx)
}

func (l *LazyLoadShard) addTimestampProperties(ctx context.Context) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.addTimestampProperties(ctx)
}

func (l *LazyLoadShard) createPropertyIndex(ctx context.Context, prop *models.Property, eg *errgroup.Group) {
	l.mustLoad()
	l.shard.createPropertyIndex(ctx, prop, eg)
}

func (l *LazyLoadShard) BeginBackup(ctx context.Context) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.BeginBackup(ctx)
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

func (l *LazyLoadShard) SetPropertyLengths(props []inverted.Property) error {
	l.mustLoad()
	return l.shard.SetPropertyLengths(props)
}

func (l *LazyLoadShard) AnalyzeObject(object *storobj.Object) ([]inverted.Property, []nilProp, error) {
	l.mustLoad()
	return l.shard.AnalyzeObject(object)
}

func (l *LazyLoadShard) Dimensions() int {
	l.mustLoad()
	return l.shard.Dimensions()
}

func (l *LazyLoadShard) QuantizedDimensions(segments int) int {
	l.mustLoad()
	return l.shard.QuantizedDimensions(segments)
}

func (l *LazyLoadShard) Aggregate(ctx context.Context, params aggregation.Params) (*aggregation.Result, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.Aggregate(ctx, params)
}

func (l *LazyLoadShard) MergeObject(ctx context.Context, object objects.MergeDocument) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.MergeObject(ctx, object)
}

func (l *LazyLoadShard) Queue() *IndexQueue {
	l.mustLoad()
	return l.shard.Queue()
}

func (l *LazyLoadShard) Shutdown(ctx context.Context) error {
	if !l.isLoaded() {
		return nil
	}
	return l.shard.Shutdown(ctx)
}

func (l *LazyLoadShard) ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.ObjectList(ctx, limit, sort, cursor, additional, className)
}

func (l *LazyLoadShard) WasDeleted(ctx context.Context, id strfmt.UUID) (bool, error) {
	if err := l.Load(ctx); err != nil {
		return false, err
	}
	return l.shard.WasDeleted(ctx, id)
}

func (l *LazyLoadShard) VectorIndex() VectorIndex {
	l.mustLoad()
	return l.shard.VectorIndex()
}

func (l *LazyLoadShard) Versioner() *shardVersioner {
	l.mustLoad()
	return l.shard.Versioner()
}

func (l *LazyLoadShard) isReadOnly() bool {
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

func (l *LazyLoadShard) prepareDeleteObject(ctx context.Context, shardID string, id strfmt.UUID) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.prepareDeleteObject(ctx, shardID, id)
}

func (l *LazyLoadShard) prepareDeleteObjects(ctx context.Context, shardID string, ids []strfmt.UUID, dryRun bool) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.prepareDeleteObjects(ctx, shardID, ids, dryRun)
}

func (l *LazyLoadShard) prepareAddReferences(ctx context.Context, shardID string, refs []objects.BatchReference) replica.SimpleResponse {
	l.mustLoadCtx(ctx)
	return l.shard.prepareAddReferences(ctx, shardID, refs)
}

func (l *LazyLoadShard) commitReplication(ctx context.Context, shardID string, mutex *backupMutex) interface{} {
	l.mustLoad()
	return l.shard.commitReplication(ctx, shardID, mutex)
}

func (l *LazyLoadShard) abortReplication(ctx context.Context, shardID string) replica.SimpleResponse {
	l.mustLoad()
	return l.shard.abortReplication(ctx, shardID)
}

func (l *LazyLoadShard) reinit(ctx context.Context) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.reinit(ctx)
}

func (l *LazyLoadShard) filePutter(ctx context.Context, shardID string) (io.WriteCloser, error) {
	if err := l.Load(ctx); err != nil {
		return nil, err
	}
	return l.shard.filePutter(ctx, shardID)
}

func (l *LazyLoadShard) extendDimensionTrackerLSM(dimensions int, docID uint64) error {
	l.mustLoad()
	return l.shard.extendDimensionTrackerLSM(dimensions, docID)
}

func (l *LazyLoadShard) addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	l.mustLoad()
	return l.shard.addToPropertySetBucket(bucket, docID, key)
}

func (l *LazyLoadShard) addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error {
	l.mustLoad()
	return l.shard.addToPropertyMapBucket(bucket, pair, key)
}

func (l *LazyLoadShard) pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair {
	l.mustLoad()
	return l.shard.pairPropertyWithFrequency(docID, freq, propLen)
}

func (l *LazyLoadShard) keyPropertyNull(isNull bool) ([]byte, error) {
	l.mustLoad()
	return l.shard.keyPropertyNull(isNull)
}

func (l *LazyLoadShard) keyPropertyLength(length int) ([]byte, error) {
	l.mustLoad()
	return l.shard.keyPropertyLength(length)
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

func (l *LazyLoadShard) batchDeleteObject(ctx context.Context, id strfmt.UUID) error {
	if err := l.Load(ctx); err != nil {
		return err
	}
	return l.shard.batchDeleteObject(ctx, id)
}

func (l *LazyLoadShard) putObjectLSM(object *storobj.Object, idBytes []byte) (objectInsertStatus, error) {
	l.mustLoad()
	return l.shard.putObjectLSM(object, idBytes)
}

func (l *LazyLoadShard) mutableMergeObjectLSM(merge objects.MergeDocument, idBytes []byte) (mutableMergeResult, error) {
	l.mustLoad()
	return l.shard.mutableMergeObjectLSM(merge, idBytes)
}

func (l *LazyLoadShard) deleteInvertedIndexItemLSM(bucket *lsmkv.Bucket, item inverted.Countable, docID uint64) error {
	l.mustLoad()
	return l.shard.deleteInvertedIndexItemLSM(bucket, item, docID)
}

func (l *LazyLoadShard) batchExtendInvertedIndexItemsLSMNoFrequency(b *lsmkv.Bucket, item inverted.MergeItem) error {
	l.mustLoad()
	return l.shard.batchExtendInvertedIndexItemsLSMNoFrequency(b, item)
}

func (l *LazyLoadShard) updatePropertySpecificIndices(object *storobj.Object, status objectInsertStatus) error {
	l.mustLoad()
	return l.shard.updatePropertySpecificIndices(object, status)
}

func (l *LazyLoadShard) updateVectorIndexIgnoreDelete(vector []float32, status objectInsertStatus) error {
	l.mustLoad()
	return l.shard.updateVectorIndexIgnoreDelete(vector, status)
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
