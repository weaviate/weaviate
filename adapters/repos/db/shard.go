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
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/cluster/router/types"
	"go.etcd.io/bbolt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const IdLockPoolSize = 128

var (
	errAlreadyShutdown    = errors.New("already shut or dropped")
	errShutdownInProgress = errors.New("shard shutdown in progress")
)

type ShardLike interface {
	Index() *Index                  // Get the parent index
	Name() string                   // Get the shard name
	Store() *lsmkv.Store            // Get the underlying store
	NotifyReady()                   // Set shard status to ready
	GetStatus() storagestate.Status // Return the shard status
	GetStatusNoLoad() storagestate.Status
	UpdateStatus(status string) error                                                   // Set shard status
	SetStatusReadonly(reason string) error                                              // Set shard status to readonly with reason
	FindUUIDs(ctx context.Context, filters *filters.LocalFilter) ([]strfmt.UUID, error) // Search and return document ids

	Counter() *indexcounter.Counter
	ObjectCount() int
	ObjectCountAsync() int
	GetPropertyLengthTracker() *inverted.JsonShardMetaData

	PutObject(context.Context, *storobj.Object) error
	PutObjectBatch(context.Context, []*storobj.Object) []error
	ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error)
	ObjectByIDErrDeleted(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, id strfmt.UUID) (bool, error)
	ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, properties []string) ([]*storobj.Object, []float32, error)
	ObjectVectorSearch(ctx context.Context, searchVectors []models.Vector, targetVectors []string, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties, targetCombination *dto.TargetCombination, properties []string) ([]*storobj.Object, []float32, error)
	UpdateVectorIndexConfig(ctx context.Context, updated schemaConfig.VectorIndexConfig) error
	UpdateVectorIndexConfigs(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error
	AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error
	DeleteObjectBatch(ctx context.Context, ids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects // Delete many objects by id
	DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error                                           // Delete object by id
	MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error)
	ObjectDigestsInRange(ctx context.Context, initialUUID, finalUUID strfmt.UUID, limit int) (objs []types.RepairResponse, err error)
	ID() string // Get the shard id
	drop() error
	HaltForTransfer(ctx context.Context, offloading bool, inactivityTimeout time.Duration) error
	initPropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper, lazyLoadSegments bool, props ...*models.Property)
	ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error
	resumeMaintenanceCycles(ctx context.Context) error
	GetFileMetadata(ctx context.Context, relativeFilePath string) (file.FileMetadata, error)
	GetFile(ctx context.Context, relativeFilePath string) (io.ReadCloser, error)
	SetPropertyLengths(props []inverted.Property) error
	AnalyzeObject(*storobj.Object) ([]inverted.Property, []inverted.NilProperty, error)
	Aggregate(ctx context.Context, params aggregation.Params, modules *modules.Provider) (*aggregation.Result, error)
	HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)
	MergeObject(ctx context.Context, object objects.MergeDocument) error
	VectorDistanceForQuery(ctx context.Context, id uint64, searchVectors []models.Vector, targets []string) ([]float32, error)
	ConvertQueue(targetVector string) error
	FillQueue(targetVector string, from uint64) error
	Shutdown(context.Context) error // Shutdown the shard
	preventShutdown() (release func(), err error)

	// TODO tests only
	ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor,
		additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) // Search and return objects
	WasDeleted(ctx context.Context, id strfmt.UUID) (bool, time.Time, error) // Check if an object was deleted
	GetVectorIndexQueue(targetVector string) (*VectorIndexQueue, bool)
	GetVectorIndex(targetVector string) (VectorIndex, bool)
	ForEachVectorIndex(f func(targetVector string, index VectorIndex) error) error
	ForEachVectorQueue(f func(targetVector string, queue *VectorIndexQueue) error) error
	// TODO tests only
	Versioner() *shardVersioner // Get the shard versioner

	SetAsyncReplicationEnabled(ctx context.Context, enabled bool) error

	isReadOnly() error
	pathLSM() string

	preparePutObject(context.Context, string, *storobj.Object) replica.SimpleResponse
	preparePutObjects(context.Context, string, []*storobj.Object) replica.SimpleResponse
	prepareMergeObject(context.Context, string, *objects.MergeDocument) replica.SimpleResponse
	prepareDeleteObject(context.Context, string, strfmt.UUID, time.Time) replica.SimpleResponse
	prepareDeleteObjects(context.Context, string, []strfmt.UUID, time.Time, bool) replica.SimpleResponse
	prepareAddReferences(context.Context, string, []objects.BatchReference) replica.SimpleResponse

	commitReplication(context.Context, string, *shardTransfer) interface{}
	abortReplication(context.Context, string) replica.SimpleResponse
	filePutter(context.Context, string) (io.WriteCloser, error)

	// TODO tests only
	Dimensions(ctx context.Context, targetVector string) int // dim(vector)*number vectors
	QuantizedDimensions(ctx context.Context, targetVector string, segments int) int

	extendDimensionTrackerLSM(dimLength int, docID uint64, targetVector string) error
	publishDimensionMetrics(ctx context.Context)
	resetDimensionsLSM() error

	addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	deleteFromPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error
	addToPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	deleteFromPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair

	setFallbackToSearchable(fallback bool)
	addJobToQueue(job job)
	uuidFromDocID(docID uint64) (strfmt.UUID, error)
	batchDeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error
	putObjectLSM(object *storobj.Object, idBytes []byte) (objectInsertStatus, error)
	mayUpsertObjectHashTree(object *storobj.Object, idBytes []byte, status objectInsertStatus) error
	mutableMergeObjectLSM(merge objects.MergeDocument, idBytes []byte) (mutableMergeResult, error)
	batchExtendInvertedIndexItemsLSMNoFrequency(b *lsmkv.Bucket, item inverted.MergeItem) error
	updatePropertySpecificIndices(ctx context.Context, object *storobj.Object, status objectInsertStatus) error
	updateVectorIndexIgnoreDelete(ctx context.Context, vector []float32, status objectInsertStatus) error
	updateVectorIndexesIgnoreDelete(ctx context.Context, vectors map[string][]float32, status objectInsertStatus) error
	updateMultiVectorIndexesIgnoreDelete(ctx context.Context, multiVectors map[string][][]float32, status objectInsertStatus) error
	hasGeoIndex() bool
	// addTargetNodeOverride adds a target node override to the shard.
	addTargetNodeOverride(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error
	// removeTargetNodeOverride removes a target node override from the shard.
	removeTargetNodeOverride(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error
	// removeAllTargetNodeOverrides removes all target node overrides from the shard
	// and resets the async replication config.
	removeAllTargetNodeOverrides(ctx context.Context) error

	// getAsyncReplicationStats returns all current sync replication stats for this node/shard
	getAsyncReplicationStats(ctx context.Context) []*models.AsyncReplicationStatus

	Metrics() *Metrics

	// A thread-safe counter that goes up any time there is activity on this
	// shard. The absolute value has no meaning, it's only purpose is to compare
	// the previous value to the current value. First value is for reads, second
	// for writes.
	Activity() (int32, int32)
	// Debug methods
	DebugResetVectorIndex(ctx context.Context, targetVector string) error
	RepairIndex(ctx context.Context, targetVector string) error
}

type onAddToPropertyValueIndex func(shard *Shard, docID uint64, property *inverted.Property) error

type onDeleteFromPropertyValueIndex func(shard *Shard, docID uint64, property *inverted.Property) error

// Shard is the smallest completely-contained index unit. A shard manages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	index             *Index // a reference to the underlying index, which in turn contains schema information
	class             *models.Class
	scheduler         *queue.Scheduler
	name              string
	store             *lsmkv.Store
	counter           *indexcounter.Counter
	indexCheckpoints  *indexcheckpoint.Checkpoints
	metrics           *Metrics
	promMetrics       *monitoring.PrometheusMetrics
	slowQueryReporter helpers.SlowQueryReporter
	propertyIndices   propertyspecific.Indices
	propLenTracker    *inverted.JsonShardMetaData
	versioner         *shardVersioner

	vectorIndexMu sync.RWMutex
	vectorIndex   VectorIndex
	queue         *VectorIndexQueue
	vectorIndexes map[string]VectorIndex
	queues        map[string]*VectorIndexQueue

	// async replication
	asyncReplicationRWMux      sync.RWMutex
	asyncReplicationConfig     asyncReplicationConfig
	hashtree                   hashtree.AggregatedHashTree
	hashtreeFullyInitialized   bool
	asyncReplicationCancelFunc context.CancelFunc

	lastComparedHosts                 []string
	lastComparedHostsMux              sync.RWMutex
	asyncReplicationStatsByTargetNode map[string]*hashBeatHostStats
	//

	haltForTransferMux               sync.Mutex
	haltForTransferInactivityTimeout time.Duration
	haltForTransferInactivityTimer   *time.Timer
	haltForTransferCount             int
	haltForTransferCancel            func()

	status              ShardStatus
	statusLock          sync.RWMutex
	propertyIndicesLock sync.RWMutex

	stopDimensionTracking        chan struct{}
	dimensionTrackingInitialized atomic.Bool

	centralJobQueue chan job // reference to queue used by all shards

	docIdLock []sync.Mutex
	// replication
	replicationMap pendingReplicaTasks

	// Indicates whether searchable buckets should be used
	// when filterable buckets are missing for text/text[] properties
	// This can happen for db created before v1.19, where
	// only map (now called searchable) buckets were created as inverted
	// indexes for text/text[] props.
	// Now roaring set (filterable) and map (searchable) buckets can
	// coexists for text/text[] props, and by default both are enabled.
	// So despite property's IndexFilterable and IndexSearchable settings
	// being enabled, only searchable bucket exists
	fallbackToSearchable bool

	cycleCallbacks *shardCycleCallbacks
	bitmapFactory  *roaringset.BitmapFactory
	bitmapBufPool  roaringset.BitmapBufPool

	activityTrackerRead  atomic.Int32
	activityTrackerWrite atomic.Int32

	// shared bolt database for dynamic vector indexes.
	// nil if there is no configured dynamic vector index
	dynamicVectorIndexDB *bbolt.DB

	// indicates whether shard is shut down or dropped (or ongoing)
	shut atomic.Bool
	// indicates whether shard in being used at the moment (e.g. write request)
	inUseCounter atomic.Int64
	// allows concurrent shut read/write
	shutdownLock *sync.RWMutex

	reindexer                             ShardReindexerV3
	callbacksAddToPropertyValueIndex      []onAddToPropertyValueIndex
	callbacksRemoveFromPropertyValueIndex []onDeleteFromPropertyValueIndex
	// stores names of properties that are searchable and use buckets of
	// inverted strategy. for such properties delta analyzer should avoid
	// computing delta between previous and current values of properties
	searchableBlockmaxPropNames     []string
	searchableBlockmaxPropNamesLock *sync.Mutex

	usingBlockMaxWAND bool

	// shutdownRequested marks shard as requested for shutdown
	shutdownRequested atomic.Bool
}

func (s *Shard) ID() string {
	return shardId(s.index.ID(), s.name)
}

func (s *Shard) path() string {
	return shardPath(s.index.path(), s.name)
}

func (s *Shard) pathLSM() string {
	return shardPathLSM(s.index.path(), s.name)
}

func (s *Shard) pathHashTree() string {
	return path.Join(s.path(), "hashtree_uuid")
}

func (s *Shard) vectorIndexID(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("vectors_%s", targetVector)
	}
	return "main"
}

func (s *Shard) uuidToIdLockPoolId(idBytes []byte) uint8 {
	// use the last byte of the uuid to determine which locking-pool a given object should use. The last byte is used
	// as uuids probably often have some kind of order and the last byte will in general be the one that changes the most
	return idBytes[15] % IdLockPoolSize
}

func (s *Shard) memtableDirtyConfig() lsmkv.BucketOption {
	return lsmkv.WithDirtyThreshold(
		time.Duration(s.index.Config.MemtablesFlushDirtyAfter) * time.Second)
}

func (s *Shard) dynamicMemtableSizing() lsmkv.BucketOption {
	return lsmkv.WithDynamicMemtableSizing(
		s.index.Config.MemtablesInitialSizeMB,
		s.index.Config.MemtablesMaxSizeMB,
		s.index.Config.MemtablesMinActiveSeconds,
		s.index.Config.MemtablesMaxActiveSeconds,
	)
}

func (s *Shard) segmentCleanupConfig() lsmkv.BucketOption {
	return lsmkv.WithSegmentsCleanupInterval(
		time.Duration(s.index.Config.SegmentsCleanupIntervalSeconds) * time.Second)
}

func (s *Shard) UpdateVectorIndexConfig(ctx context.Context, updated schemaConfig.VectorIndexConfig) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	err := s.SetStatusReadonly("UpdateVectorIndexConfig")
	if err != nil {
		return fmt.Errorf("attempt to mark read-only: %w", err)
	}

	index, ok := s.GetVectorIndex("")
	if !ok {
		return fmt.Errorf("vector index does not exist")
	}

	return index.UpdateUserConfig(updated, func() {
		s.UpdateStatus(storagestate.StatusReady.String())
	})
}

func (s *Shard) UpdateVectorIndexConfigs(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}
	if err := s.SetStatusReadonly("UpdateVectorIndexConfig"); err != nil {
		return fmt.Errorf("attempt to mark read-only: %w", err)
	}

	wg := new(sync.WaitGroup)
	var err error
	for targetVector, targetCfg := range updated {
		if index, ok := s.GetVectorIndex(targetVector); ok {
			wg.Add(1)
			if err = index.UpdateUserConfig(targetCfg, wg.Done); err != nil {
				break
			}
		} else {
			// dont lazy load segments on config update
			if err = s.initTargetVector(ctx, targetVector, targetCfg, false); err != nil {
				return fmt.Errorf("creating new vector index: %w", err)
			}
		}
	}

	f := func() {
		wg.Wait()
		s.UpdateStatus(storagestate.StatusReady.String())
	}
	enterrors.GoWrapper(f, s.index.logger)

	return err
}

// ObjectCount returns the exact count at any moment
func (s *Shard) ObjectCount() int {
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return 0
	}

	return b.Count()
}

// ObjectCountAsync returns the eventually consistent "async" count which is
// much cheaper to obtain
func (s *Shard) ObjectCountAsync() int {
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return 0
	}

	return b.CountAsync()
}

func (s *Shard) isFallbackToSearchable() bool {
	return s.fallbackToSearchable
}

func (s *Shard) tenant() string {
	// TODO provide better impl
	if s.index.partitioningEnabled {
		return s.name
	}
	return ""
}

func shardId(indexId, shardName string) string {
	return fmt.Sprintf("%s_%s", indexId, shardName)
}

func shardPath(indexPath, shardName string) string {
	return path.Join(indexPath, shardName)
}

func shardPathLSM(indexPath, shardName string) string {
	return path.Join(indexPath, shardName, "lsm")
}

func bucketKeyPropertyLength(length int) ([]byte, error) {
	return entinverted.LexicographicallySortableInt64(int64(length))
}

func bucketKeyPropertyNull(isNull bool) ([]byte, error) {
	if isNull {
		return []byte{uint8(filters.InternalNullState)}, nil
	}
	return []byte{uint8(filters.InternalNotNullState)}, nil
}

// Activity score for read and write
func (s *Shard) Activity() (int32, int32) {
	return s.activityTrackerRead.Load(), s.activityTrackerWrite.Load()
}

func (s *Shard) registerAddToPropertyValueIndex(callback onAddToPropertyValueIndex) {
	s.callbacksAddToPropertyValueIndex = append(s.callbacksAddToPropertyValueIndex, callback)
}

func (s *Shard) registerDeleteFromPropertyValueIndex(callback onDeleteFromPropertyValueIndex) {
	s.callbacksRemoveFromPropertyValueIndex = append(s.callbacksRemoveFromPropertyValueIndex, callback)
}
