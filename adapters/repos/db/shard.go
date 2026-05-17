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
	CompareDigests(ctx context.Context, sourceDigests []types.RepairResponse) ([]types.RepairResponse, error)
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
	AnalyzeObjectForMigration(*storobj.Object) ([]inverted.Property, []inverted.NilProperty, error)
	AnalyzeObjectForMigrationWithOverlay(*storobj.Object, map[string]inverted.PropertyOverlay) ([]inverted.Property, []inverted.NilProperty, error)
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

	// rangeableLocalReadyMu guards rangeableLocalReady. Holds the per-prop
	// "is this shard's rangeable bucket fully populated and safe to
	// query?" answer used to fix GH 0-weaviate-issues#212 Issue C.
	//
	// True means the local rangeable bucket has all the data for this
	// property — either the property was created with
	// IndexRangeFilters=true (no migration ever ran) or an
	// enable-rangeable / repair-rangeable migration completed locally
	// (markTidied fired in [runtimeSwap]).
	//
	// False means the rangeable bucket is mid-migration on THIS replica:
	// a PreReindexHook created an empty main bucket but the per-shard
	// runtimeSwap that prepends ingest+reindex segments into it hasn't
	// run yet on this node. During this window the cluster-wide schema
	// flag may already be true (the first replica to swap fires
	// strategy.OnMigrationComplete which RAFTs the flip cluster-wide),
	// so the inverted query path would otherwise route range queries to
	// the empty bucket and return partial / zero counts. The
	// IsRangeableLocallyReady callback wired into the Searcher
	// overrides hasRangeableIndex=false for this prop on this shard,
	// forcing a fallback to the filterable bucket walk until our local
	// swap catches up.
	//
	// Read on every range-filter query plan, so kept under a fast
	// RWMutex rather than a sync.Map. Default value (missing key)
	// returns true via IsRangeableLocallyReady — at shard init we
	// pessimistically set false for any in-flight migration tracker
	// found on disk, and the post-tidy hook flips it back to true.
	rangeableLocalReadyMu sync.RWMutex
	rangeableLocalReady   map[string]bool

	// tokenizationOverlayMu guards tokenizationOverlay. Holds the per-prop
	// "what tokenization should query input use on this shard?" override
	// used to close the Gap B race in 0-weaviate-issues#216.
	//
	// Mechanism: a change-tokenization (or change-tokenization-filterable)
	// migration's per-shard runtimeSwap flips the canonical bucket pointer
	// to NEW-tokenization data BEFORE the cluster-wide schema flip in
	// OnTaskCompleted.flipSemanticMigrationSchema commits via RAFT. During
	// that seconds-long window on each replica:
	//   - Bucket content on this shard: NEW tokenization (post-swap)
	//   - Live schema as seen by the analyzer: OLD tokenization (pre-flip)
	// Query input gets tokenized against OLD; lookup hits NEW bucket; counts
	// don't match. Frontend Claude's `7→4→1→7→3→2` flap on the live demo.
	//
	// Lifecycle (see reindex_provider.go's OnGroupCompleted +
	// OnTaskCompleted):
	//   1. SET: pre-swap, just BEFORE the per-task RunSwapOnShard loop
	//      kicks off on this shard. Setting pre-swap means the brief
	//      in-flight window between bucket-pointer flip and overlay
	//      visibility is bounded by the in-memory swap latency
	//      (microseconds) rather than the cluster-wide cutover spread.
	//   2. CLEAR (defensive, all-failed path): if every per-task swap on
	//      this shard fails before flipping its bucket pointer (e.g.
	//      ctx.Canceled during graceful shutdown), the post-loop branch
	//      clears the overlay. Without this, an all-failed swap path
	//      would leave overlay=NEW against unchanged OLD buckets —
	//      permanent misalignment because the FAILED transition skips
	//      the cluster-wide schema flip and the explicit clear hook
	//      never runs.
	//   3. CLEAR (success path): once flipSemanticMigrationSchema
	//      commits the cluster-wide schema flip, OnTaskCompleted clears
	//      the overlay per-shard so the steady-state map is empty.
	//   4. CLEAR (self-clear backstop): TokenizationFor below clears
	//      the entry on the next read where the live schema has caught
	//      up to the overlay value — defensive against any callback-
	//      ordering edge case.
	//
	// Read on every query that touches the affected property, so kept
	// under a fast RWMutex rather than a sync.Map (consistent with
	// rangeableLocalReady above). Per-shard, in-memory only — the
	// cluster-wide schema flip is the authoritative cross-replica
	// signal; this overlay just bridges the local seconds-long gap
	// between bucket-swap-here and schema-flip-observed-here.
	tokenizationOverlayMu sync.RWMutex
	tokenizationOverlay   map[string]string

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

	reindexer ShardReindexerV3

	// Copy-on-write callback slices stored in atomic.Value for lock-free reads
	// on the hot write path. Registration (rare) copies the slice behind
	// propertyValueIndexCallbacksMu; iteration (every object write) loads the
	// current snapshot without locking.
	callbacksAddToPropertyValueIndex      atomic.Value // []onAddToPropertyValueIndex
	callbacksRemoveFromPropertyValueIndex atomic.Value // []onDeleteFromPropertyValueIndex
	propertyValueIndexCallbacksMu         sync.Mutex
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
	if err := s.SetStatusReadonly(reason); err != nil {
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

// IsRangeableLocallyReady reports whether this shard's local rangeable
// bucket for the given property is fully populated and safe to query.
// See [rangeableLocalReady] for the full rationale (GH
// 0-weaviate-issues#212 Issue C).
//
// Returns true when:
//   - The per-shard map has an explicit `true` entry. Set by
//     [setRangeableLocallyReady] after a local
//     enable-rangeable / repair-rangeable migration's swap completes
//     (markTidied + OnMigrationComplete), OR
//   - There is no explicit entry in the map AND the rangeable bucket
//     for this prop exists in the LSM store. This covers native
//     rangeable props (created with IndexRangeFilters=true, bucket
//     populated on initial import) and props whose migrations
//     completed before this shard restarted (the per-shard map is
//     in-memory only and starts empty).
//
// Returns false when:
//   - The per-shard map has an explicit `false` entry (set by the
//     migration's PreReindexHook), OR
//   - There is no explicit entry AND the rangeable bucket does not
//     exist in the LSM store yet. This catches the narrow window where
//     another replica's runtimeSwap has already flipped the
//     cluster-wide schema flag to `IndexRangeFilters=true` but THIS
//     replica's PreReindexHook hasn't fired yet — without this
//     bucket-existence default-false, the inverted query path would
//     try to look up a bucket that isn't there and return
//     "bucket for prop %s not found - is it indexed?" to the LB.
func (s *Shard) IsRangeableLocallyReady(propName string) bool {
	s.rangeableLocalReadyMu.RLock()
	if s.rangeableLocalReady != nil {
		if ready, set := s.rangeableLocalReady[propName]; set {
			s.rangeableLocalReadyMu.RUnlock()
			return ready
		}
	}
	s.rangeableLocalReadyMu.RUnlock()

	// Default: ready iff the rangeable bucket physically exists in the
	// store. Cheap (a map lookup under bucketAccessLock.RLock in
	// lsmkv.Store.Bucket). We avoid materializing the explicit entry
	// here on purpose — the migration hooks own the lifecycle of the
	// map, and adding a "shadow" entry from a query goroutine would
	// race the hook's clear-then-set sequence.
	return s.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName)) != nil
}

// setRangeableLocallyReady is invoked by the rangeable migration's
// lifecycle hooks. It is intentionally NOT exported beyond the package
// because outside callers should observe the state via
// [IsRangeableLocallyReady] only; mutating it from outside the
// migration code paths would corrupt the per-replica routing invariant.
func (s *Shard) setRangeableLocallyReady(propName string, ready bool) {
	s.rangeableLocalReadyMu.Lock()
	defer s.rangeableLocalReadyMu.Unlock()
	if s.rangeableLocalReady == nil {
		s.rangeableLocalReady = map[string]bool{}
	}
	s.rangeableLocalReady[propName] = ready
}

// SetTokenizationOverlay records that propName's query-time tokenization on
// this shard should be `target` instead of the schema-stored value until
// the live schema catches up. Set by the change-tokenization migration's
// reindex hook (reindex_provider.OnGroupCompleted) just BEFORE the
// per-task RunSwapOnShard loop kicks off — setting pre-swap means the
// brief window between bucket-pointer flip and overlay visibility is
// bounded by the in-memory swap latency (microseconds), not by the
// cluster-wide cutover spread. The same caller clears the overlay if
// every per-task swap failed (defensive: no bucket flipped → overlay
// must not stay set against unchanged buckets). On success, the overlay
// is cleared explicitly by OnTaskCompleted after the cluster-wide
// schema flip commits, with [TokenizationFor]'s self-clear-on-catchup
// branch as a backstop. See the [tokenizationOverlay] field godoc for
// the full rationale and lifecycle.
//
// Empty target is a no-op — used by the migration cleanup path to avoid
// having every caller guard the call.
func (s *Shard) SetTokenizationOverlay(propName, target string) {
	if propName == "" || target == "" {
		return
	}
	s.tokenizationOverlayMu.Lock()
	defer s.tokenizationOverlayMu.Unlock()
	if s.tokenizationOverlay == nil {
		s.tokenizationOverlay = map[string]string{}
	}
	s.tokenizationOverlay[propName] = target
}

// ClearTokenizationOverlay removes any tokenization-overlay entry for
// propName. Idempotent — called by the schema-update callback when the
// live schema's tokenization for propName matches the overlay's target,
// indicating OnTaskCompleted's flipSemanticMigrationSchema has applied
// on this node and the overlay is no longer needed.
func (s *Shard) ClearTokenizationOverlay(propName string) {
	if propName == "" {
		return
	}
	s.tokenizationOverlayMu.Lock()
	defer s.tokenizationOverlayMu.Unlock()
	if s.tokenizationOverlay == nil {
		return
	}
	delete(s.tokenizationOverlay, propName)
}

// TokenizationFor returns the active query-time tokenization for propName
// on this shard. Consults the overlay first; if the overlay's value
// matches the live schema's `liveTokenization` the overlay is self-
// cleared (defensive against schema-update-callback ordering) and the
// live value is returned. Otherwise the overlay value is returned if
// present, else liveTokenization.
//
// liveTokenization is the value the caller would have used in the
// absence of any overlay — typically `prop.Tokenization`. Passing the
// live value as a parameter (rather than re-reading the schema here)
// keeps this helper cheap and avoids a schema-manager dependency in the
// query hot path.
func (s *Shard) TokenizationFor(propName, liveTokenization string) string {
	if propName == "" {
		return liveTokenization
	}
	s.tokenizationOverlayMu.RLock()
	if s.tokenizationOverlay == nil {
		s.tokenizationOverlayMu.RUnlock()
		return liveTokenization
	}
	overlay, ok := s.tokenizationOverlay[propName]
	s.tokenizationOverlayMu.RUnlock()
	if !ok {
		return liveTokenization
	}
	if overlay == liveTokenization {
		// Live schema has caught up. Self-clear so future calls take the
		// fast path — write under the write lock so concurrent self-
		// clears don't race the migration's explicit clear.
		s.tokenizationOverlayMu.Lock()
		if s.tokenizationOverlay != nil {
			if current, ok := s.tokenizationOverlay[propName]; ok && current == liveTokenization {
				delete(s.tokenizationOverlay, propName)
			}
		}
		s.tokenizationOverlayMu.Unlock()
		return liveTokenization
	}
	return overlay
}

// SnapshotTokenizationOverlay returns a fixed-allocation map of
// {propName → target} entries for the supplied propNames, restricted
// to entries that currently exist in the overlay. Used by query setup
// paths that need to populate inverted.PropertyOverlay values for the
// analyzer's WithSchemaOverlay mechanism (see
// adapters/repos/db/inverted/analyzer.go).
//
// Per QA Claude's micro-note on #216: this avoids cloning the entire
// underlying map at every query — only the requested props are
// snapshotted, and an empty result returns nil so the analyzer can
// take its fast path.
//
// The returned map is owned by the caller.
func (s *Shard) SnapshotTokenizationOverlay(propNames []string) map[string]string {
	if len(propNames) == 0 {
		return nil
	}
	s.tokenizationOverlayMu.RLock()
	defer s.tokenizationOverlayMu.RUnlock()
	if len(s.tokenizationOverlay) == 0 {
		return nil
	}
	var out map[string]string
	for _, name := range propNames {
		if v, ok := s.tokenizationOverlay[name]; ok {
			if out == nil {
				out = make(map[string]string, len(propNames))
			}
			out[name] = v
		}
	}
	return out
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

func (s *Shard) registerAddToPropertyValueIndex(callback onAddToPropertyValueIndex) func() {
	disabled := &atomic.Bool{}
	wrapped := func(shard *Shard, docID uint64, property *inverted.Property) error {
		if disabled.Load() {
			return nil
		}
		return callback(shard, docID, property)
	}

	s.propertyValueIndexCallbacksMu.Lock()
	var current []onAddToPropertyValueIndex
	if v := s.callbacksAddToPropertyValueIndex.Load(); v != nil {
		current = v.([]onAddToPropertyValueIndex)
	}
	updated := make([]onAddToPropertyValueIndex, len(current)+1)
	copy(updated, current)
	updated[len(current)] = wrapped
	s.callbacksAddToPropertyValueIndex.Store(updated)
	s.propertyValueIndexCallbacksMu.Unlock()

	return func() { disabled.Store(true) }
}

func (s *Shard) registerDeleteFromPropertyValueIndex(callback onDeleteFromPropertyValueIndex) func() {
	disabled := &atomic.Bool{}
	wrapped := func(shard *Shard, docID uint64, property *inverted.Property) error {
		if disabled.Load() {
			return nil
		}
		return callback(shard, docID, property)
	}

	s.propertyValueIndexCallbacksMu.Lock()
	var current []onDeleteFromPropertyValueIndex
	if v := s.callbacksRemoveFromPropertyValueIndex.Load(); v != nil {
		current = v.([]onDeleteFromPropertyValueIndex)
	}
	updated := make([]onDeleteFromPropertyValueIndex, len(current)+1)
	copy(updated, current)
	updated[len(current)] = wrapped
	s.callbacksRemoveFromPropertyValueIndex.Store(updated)
	s.propertyValueIndexCallbacksMu.Unlock()

	return func() { disabled.Store(true) }
}
