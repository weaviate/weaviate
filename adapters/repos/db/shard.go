//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/cluster/router/types"
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

const IdLockPoolSize uint64 = 1024

var (
	errAlreadyShutdown    = errors.New("already shut or dropped")
	errShutdownInProgress = errors.New("shard shutdown in progress")
)

type ShardLike interface {
	Index() *Index                                                                                 // Get the parent index
	Name() string                                                                                  // Get the shard name
	Store() *lsmkv.Store                                                                           // Get the underlying store
	NotifyReady()                                                                                  // Set shard status to ready
	GetStatus() storagestate.Status                                                                // Return the shard status
	GetStatusReason() string                                                                       // Return the reason for the current status
	UpdateStatus(status, reason string) error                                                      // Set shard status
	SetStatusReadonly(reason string) error                                                         // Set shard status to readonly with reason
	FindUUIDs(ctx context.Context, filters *filters.LocalFilter, limit int) ([]strfmt.UUID, error) // Search and return document ids

	Counter() *indexcounter.Counter
	ObjectCount(ctx context.Context) (int, error)
	ObjectCountAsync(ctx context.Context) (int64, error)
	GetPropertyLengthTracker() *inverted.JsonShardMetaData

	PutObject(context.Context, *storobj.Object) error
	PutObjectBatch(context.Context, []*storobj.Object) []error
	ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error)
	ObjectDigestErrDeleted(ctx context.Context, id strfmt.UUID) (types.RepairResponse, error)
	Exists(ctx context.Context, id strfmt.UUID) (bool, error)
	ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, properties []string) ([]*storobj.Object, []float32, error)
	ObjectVectorSearch(ctx context.Context, searchVectors []models.Vector, targetVectors []string, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties, targetCombination *dto.TargetCombination, properties []string, selection *searchparams.Selection) ([]*storobj.Object, []float32, error)
	UpdateVectorIndexConfig(ctx context.Context, updated schemaConfig.VectorIndexConfig) error
	UpdateVectorIndexConfigs(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error
	DropVectorIndex(ctx context.Context, targetVector string) error
	AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error
	DeleteObjectBatch(ctx context.Context, ids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects // Delete many objects by id
	DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error                                           // Delete object by id
	MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error)
	ObjectDigests(ctx context.Context, query []multi.Identifier) ([]types.RepairResponse, error)
	ObjectDigestsInRange(ctx context.Context, initialUUID, finalUUID strfmt.UUID, limit int) (objs []types.RepairResponse, err error)
	ID() string // Get the shard id
	drop(keepFiles bool) error
	HaltForTransfer(ctx context.Context, offloading bool, inactivityTimeout time.Duration) error
	initPropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper, lazyLoadSegments bool, props ...*models.Property)
	updatePropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper, property *models.Property)
	CreateBackupSnapshot(ctx context.Context, sd *backup.ShardDescriptor, stagingRoot string) ([]string, error)
	ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) ([]string, error)
	resumeMaintenanceCycles(ctx context.Context) error
	GetFileMetadata(ctx context.Context, relativeFilePath string) (file.FileMetadata, error)
	GetFile(ctx context.Context, relativeFilePath string) (io.ReadCloser, error)
	SetPropertyLengths(props []inverted.Property) error
	AnalyzeObject(*storobj.Object) ([]inverted.Property, []inverted.NilProperty, []inverted.NestedProperty, error)
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
	ForEachGeoQueue(f func(propName string, queue *VectorIndexQueue) error) error
	// TODO tests only
	Versioner() *shardVersioner // Get the shard versioner

	isReadOnly() error
	pathLSM() string

	preparePutObject(context.Context, string, *storobj.Object) replica.SimpleResponse
	preparePutObjects(context.Context, string, []*storobj.Object) replica.SimpleResponse
	prepareMergeObject(context.Context, string, *objects.MergeDocument) replica.SimpleResponse
	prepareDeleteObject(context.Context, string, strfmt.UUID, time.Time) replica.SimpleResponse
	prepareDeleteObjects(context.Context, string, []strfmt.UUID, time.Time, bool) replica.SimpleResponse
	prepareAddReferences(context.Context, string, []objects.BatchReference) replica.SimpleResponse

	commitReplication(context.Context, string) interface{}
	abortReplication(context.Context, string) replica.SimpleResponse
	filePutter(context.Context, string) (io.WriteCloser, error)

	// Dimensions returns the total number of dimensions for a given vector
	Dimensions(ctx context.Context, targetVector string) (int, error)
	QuantizedDimensions(ctx context.Context, targetVector string, segments int) (int, error)

	extendDimensionTrackerLSM(dimLength int, docID uint64, targetVector string) error
	resetDimensionsLSM(ctx context.Context) error

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
	putObjectLSM(ctx context.Context, object *storobj.Object, idBytes []byte) (objectInsertStatus, error)
	mayUpsertObjectHashTree(object *storobj.Object, idBytes []byte, status objectInsertStatus) error
	mutableMergeObjectLSM(ctx context.Context, merge objects.MergeDocument, idBytes []byte) (mutableMergeResult, error)
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
	RequantizeIndex(ctx context.Context, targetVector string) error

	// Debug method for docID lock debugging and contention detection and simulation
	DebugGetDocIdLockStatus() (bool, error)
}

// asyncReplicationController is a package-internal interface implemented by
// both *Shard and *LazyLoadShard. It exists so that index-level code can call
// the private enable/disable methods without exposing them on ShardLike.
type asyncReplicationController interface {
	enableAsyncReplication(ctx context.Context, config AsyncReplicationConfig) error
	disableAsyncReplication(ctx context.Context) error
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

	geoQueues map[string]*VectorIndexQueue

	// async replication
	asyncReplicationRWMux           sync.RWMutex
	targetNodeOverrides             additional.AsyncReplicationTargetNodeOverrides
	asyncReplicationConfig          AsyncReplicationConfig
	hashtree                        hashtree.AggregatedHashTree
	hashtreeFullyInitialized        bool
	minimalHashtreeInitializationCh chan struct{}
	asyncReplicationCancelFunc      context.CancelFunc

	// asyncRepCtx is the per-shard context for the hashbeat cycle. It is
	// derived from context.Background() and cancelled by asyncReplicationCancelFunc
	// when async replication is stopped. Workers receive this context so that
	// in-flight cycles terminate promptly when the shard is deregistered.
	asyncRepCtx context.Context

	// asyncRepWg tracks all async replication goroutines that may access shard
	// resources: in-flight hashbeat cycles, hashtree init goroutines, and
	// scheduler-register goroutines. The counter is usually 0 or 1 but may
	// briefly exceed 1 when an init goroutine overlaps with a dispatch.
	// Done() for hashbeat cycles is called before the result is sent back to
	// the dispatcher, so the scheduler cannot re-dispatch until Done() fires.
	// Callers that need a strict happens-before guarantee call asyncRepWg.Wait()
	// after Deregister to ensure all goroutines have fully exited.
	asyncRepWg sync.WaitGroup

	// asyncRepNeedsRebuild is set by runEntry when the effective hashtree height
	// (after applying runtime-config overrides) differs from the current hashtree
	// height. The scheduler spawns a rebuild goroutine after asyncRepWg.Done()
	// so that DisableAsyncReplication can safely call Deregister+Wait.
	asyncRepNeedsRebuild atomic.Bool

	// asyncRepRebuildInFlight prevents concurrent rebuildHashtree goroutines.
	// CAS false→true before spawning a rebuild; the goroutine clears it on exit.
	// If a rebuild is already in-flight, asyncRepNeedsRebuild is re-armed so
	// the next completed cycle will try again.
	asyncRepRebuildInFlight atomic.Bool

	// asyncRepRebuildFailures counts consecutive hashtree rebuild failures.
	// Reset to zero on success. Used by runEntry to compute exponential backoff.
	asyncRepRebuildFailures atomic.Uint32

	// asyncRepRebuildBackoffUntil stores the Unix-nanosecond timestamp after
	// which the next rebuild is permitted. Zero means no backoff is active.
	// Set by rebuildHashtree on failure; cleared on success.
	asyncRepRebuildBackoffUntil atomic.Int64

	// asyncRepLastLog throttles per-shard log messages independently of the
	// global loggingFrequency config. Stored as Unix seconds (0 = never logged,
	// treated as "epoch = very long ago"). Atomic to satisfy the race detector:
	// initAsyncReplication resets it while runHashbeatCycle reads/writes it.
	asyncRepLastLog atomic.Int64

	lastComparedHosts                 []string
	lastComparedHostsMux              sync.RWMutex
	asyncReplicationStatsByTargetNode map[string]*hashBeatHostStats
	// asyncReplicationStatsMux guards asyncReplicationStatsByTargetNode
	// independently of asyncReplicationRWMux. This prevents the per-iteration
	// stats write (in handleHashbeatWakeup) from write-locking asyncReplicationRWMux,
	// which would stall every concurrent object write and query via writer-preference.
	// Lock ordering when both are needed: asyncReplicationRWMux before asyncReplicationStatsMux.
	asyncReplicationStatsMux sync.RWMutex

	haltForTransferMux               sync.Mutex
	haltForTransferInactivityTimeout time.Duration
	haltForTransferInactivityTimer   *time.Timer
	haltForTransferCount             int
	haltForTransferCancel            func()

	status              ShardStatus
	statusLock          sync.RWMutex
	propertyIndicesLock sync.RWMutex

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

	shutCtx       context.Context
	shutCtxCancel context.CancelCauseFunc

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

	HFreshEnabled bool

	lazySegmentLoadingEnabled bool

	// metricsRegistered tracks whether this shard was registered with shard lifecycle metrics
	// (e.g., NewLoadedShard or FinishLoadingShard was called). This prevents double-counting
	// or incorrect metric updates during partial initialization cleanup.
	metricsRegistered atomic.Bool
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
		return fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, targetVector)
	}
	return "main"
}

// uuidToIdLockPoolId computes a lock pool id for a given uuid. The lock pool
// is used to synchronize access to the same uuid. The pool size is fixed and
// defined by IdLockPoolSize.
// The function uses the XOR of the two halves of the UUID to ensure a good
// distribution of UUIDs to lock pool ids. This helps to minimize lock
// contention when multiple goroutines are accessing different UUIDs.
// The result is then taken modulo the pool size to ensure it fits within the
// bounds of the lock pool array.
func (s *Shard) uuidToIdLockPoolId(uuidBytes []byte) uint {
	lo := binary.LittleEndian.Uint64(uuidBytes[:8])
	hi := binary.LittleEndian.Uint64(uuidBytes[8:16])

	return uint((lo ^ hi) % IdLockPoolSize)
}

func (s *Shard) UpdateVectorIndexConfig(ctx context.Context, updated schemaConfig.VectorIndexConfig) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	reason := statusReasonVectorIndexUpdate
	err := s.SetStatusReadonly(reason)
	if err != nil {
		return fmt.Errorf("attempt to mark read-only: %w", err)
	}

	index, ok := s.GetVectorIndex("")
	if !ok {
		return fmt.Errorf("vector index does not exist")
	}

	return index.UpdateUserConfig(updated, func() {
		s.UpdateStatus(storagestate.StatusReady.String(), reason)
	})
}

func (s *Shard) UpdateVectorIndexConfigs(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	if err := newCompressedVectorsMigrator(s.index.logger).doUpdate(s, updated); err != nil {
		s.index.logger.WithFields(logrus.Fields{
			"action":   "init_target_vectors",
			"shard_id": s.ID(),
		}).Errorf("failed to migrate vectors compressed folder: %v", err)
	}

	i := 0
	targetVecs := make([]string, len(updated))
	for targetVec := range updated {
		targetVecs[i] = targetVec
		i++
	}
	reason := fmt.Sprintf("UpdateVectorIndexConfigs: %v", targetVecs)

	// If every vector index is already compressed, UpdateUserConfig calls its
	// callback synchronously — there is no async compression work to protect.
	// Skip the READONLY/READY cycle entirely to avoid a RAFT log replay trap:
	// when UpdateClass commands are replayed rapidly after a restart, the first
	// call correctly sets READONLY and spawns a goroutine to restore READY, but
	// all subsequent calls hit the isReadOnly() guard above and exit early
	// without spawning their own goroutine.  If the goroutine from the first
	// call has not been scheduled by the time the last replay fires, the shard
	// is left permanently READONLY until the goroutine eventually runs — during
	// which any concurrent search fails with "store is read-only".
	allCompressed := true
	for targetVector := range updated {
		index, ok := s.GetVectorIndex(targetVector)
		if !ok || !index.Compressed() {
			allCompressed = false
			break
		}
	}

	if !allCompressed {
		if err := s.SetStatusReadonly(reason); err != nil {
			return fmt.Errorf("attempt to mark read-only: %w", err)
		}
	}

	wg := new(sync.WaitGroup)
	var err error
	for targetVector, targetCfg := range updated {
		if index, ok := s.GetVectorIndex(targetVector); ok {
			wg.Add(1)
			if err = index.UpdateUserConfig(targetCfg, wg.Done); err != nil {
				s.index.logger.WithFields(logrus.Fields{
					"action":        "update_vector_index_config",
					"shard_id":      s.ID(),
					"target_vector": targetVector,
				}).Errorf("failed to update vector index config: %v", err)
				break
			}
		} else {
			// dont lazy load segments on config update
			if err = s.initTargetVector(ctx, targetVector, targetCfg, false); err != nil {
				err = fmt.Errorf("creating new vector index: %w, %s", err, targetVector)
				s.index.logger.WithFields(logrus.Fields{
					"action":        "init_target_vector",
					"shard_id":      s.ID(),
					"target_vector": targetVector,
				}).Errorf("failed to init target vector index: %v", err)
				break
			}
		}
	}

	if allCompressed {
		// All callbacks were synchronous; wg is already at zero, nothing to wait for.
		return err
	}

	f := func() {
		wg.Wait()
		s.UpdateStatus(storagestate.StatusReady.String(), reason)
	}
	enterrors.GoWrapper(f, s.index.logger)

	return err
}

// ObjectCount returns the exact count at any moment
func (s *Shard) ObjectCount(ctx context.Context) (int, error) {
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return 0, errors.New("object bucket does not exist")
	}

	return b.Count(ctx)
}

// ObjectCountAsync returns the eventually consistent "async" count which is
// much cheaper to obtain
func (s *Shard) ObjectCountAsync(_ context.Context) (int64, error) {
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		// we return no error, because we could have shards without the objects bucket
		// the error is needed to satisfy the interface for lazy loaded shards possible errors
		return 0, nil
	}

	return int64(b.CountAsync()), nil
}

func (s *Shard) ObjectStorageSize(ctx context.Context) (int64, error) {
	metrics, err := shardusage.CalculateUnloadedObjectsMetrics(s.index.logger, s.index.path(), s.name, false)
	if err != nil {
		return 0, err
	}
	return metrics.StorageBytes, nil
}

// VectorStorageSize calculates the total storage size of all vector indexes in the shard
func (s *Shard) VectorStorageSize(ctx context.Context, lsmPath string, directories []string) (int64, int64, error) {
	vectorSize, err := shardusage.CalculateUnloadedVectorsMetrics(lsmPath, directories)
	if err != nil {
		return 0, 0, err
	}

	uncompressedSize := int64(0)
	if err := s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		// Get dimensions and object count from the dimensions bucket for this specific target vector
		dimensionality, err := s.calcTargetVectorDimensions(ctx, targetVector)
		if err != nil {
			return err
		}
		if dimensionality.Count == 0 || dimensionality.Dimensions == 0 {
			return nil
		}

		uncompressedSize += int64(dimensionality.Count) * int64(dimensionality.Dimensions) * 4
		return nil
	}); err != nil {
		return 0, 0, err
	}

	return vectorSize, uncompressedSize, nil
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
