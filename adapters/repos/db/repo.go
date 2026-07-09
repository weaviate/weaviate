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
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/entities/loadlimiter"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	clusterReplication "github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

type DB struct {
	logger                    logrus.FieldLogger
	localNodeName             string
	schemaGetter              schemaUC.SchemaGetter
	config                    Config
	indices                   map[string]*Index
	remoteIndex               sharding.RemoteIndexClient
	asyncReplicationScheduler *AsyncReplicationScheduler
	replicaClient             replica.Client
	nodeResolver              cluster.NodeResolver
	remoteNode                *sharding.RemoteNode
	promMetrics               *monitoring.PrometheusMetrics
	indexCheckpoints          *indexcheckpoint.Checkpoints
	shutdown                  chan struct{}
	startupComplete           atomic.Bool
	startupProgress           atomic.Pointer[StartupProgressSnapshot]
	resourceScanState         *resourceScanState
	memMonitor                *memwatch.Monitor

	// indexLock is an RWMutex which allows concurrent access to various indexes,
	// but only one modification at a time. R/W can be a bit confusing here,
	// because it does not refer to write or read requests from a user's
	// perspective, but rather:
	//
	// - Read -> The array containing all indexes is read-only. In other words
	// there will never be a race condition from doing something like index :=
	// indexes[0]. What you do with the Index after retrieving it from the array
	// does not matter. Assuming that it is thread-safe (it is) you can
	// read/write from the index itself. Therefore from a user's perspective
	// something like a parallel import batch and a read-query can happen without
	// any problems.
	//
	// - Write -> The index array is being modified, for example, because a new
	// index is added. This is mutually exclusive with the other case (but
	// hopefully very short).
	//
	//
	// See also: https://github.com/weaviate/weaviate/issues/2351
	//
	// This lock should be used to avoid that the indices-map is changed while iterating over it. To
	// mark a given index in use, lock that index directly.
	indexLock sync.RWMutex

	jobQueueCh          chan job
	scheduler           *queue.Scheduler
	shutDownWg          sync.WaitGroup
	maxNumberGoroutines int
	ratePerSecond       atomic.Int64

	// in the case of metrics grouping we need to observe some metrics
	// node-centric, rather than shard-centric
	metricsObserver *nodeWideMetricsObserver

	shardLoadLimiter  *loadlimiter.LoadLimiter
	bucketLoadLimiter *loadlimiter.LoadLimiter

	reindexer      ShardReindexerV3
	nodeSelector   cluster.NodeSelector
	schemaReader   schemaUC.SchemaReader
	replicationFSM types.ReplicationFSMReader

	// reindexAuditMu guards the audit deps installed by
	// [DB.SetReindexAuditDeps] and the backup-gate activity lookup
	// installed by [DB.SetShardReindexActivityLookup] so they are
	// safely visible from any post-restore goroutine.
	//
	// reindexAuditDeferredRequests counts the number of times
	// [DB.AuditOrphanReindexTrackersIfReady] was called BEFORE deps
	// were installed (typically from the per-class-dir restore hook
	// firing during RAFT replay while the SetReindexAuditDeps
	// goroutine is still waiting on metaStoreReady). On the first
	// SetReindexAuditDeps call, if the counter is non-zero, the
	// install path runs a single replay sweep so the deferred
	// per-class audits are not silently lost. Closes B2.
	reindexAuditMu                     sync.RWMutex
	reindexAuditLookupBuilder          KnownReindexTaskLookupBuilder
	reindexAuditLogger                 logrus.FieldLogger
	reindexAuditDeferredRequests       int
	shardReindexActivityLookupBuilder  ShardReindexActivityLookupBuilder
	reindexCleanupInProgressLookupBldr CleanupInProgressLookupBuilder

	bitmapBufPool      roaringset.BitmapBufPool
	bitmapBufPoolClose func()

	AsyncIndexingEnabled bool

	tenantsManager schemaUC.TenantsActivityManager

	// usageLimits is propagated to each Index when it is created, so
	// Shard.PutObject{,Batch} can call CheckObjects on the write path.
	// nil disables the check. See docs/usage_limits.md.
	usageLimits *usagelimits.Manager
}

// SetUsageLimits installs the usage-limits Manager on the DB. Must be
// called before WaitForStartup so that indices created during startup
// inherit the manager. See docs/usage_limits.md.
func (db *DB) SetUsageLimits(m *usagelimits.Manager) {
	db.usageLimits = m
}

func (db *DB) GetSchemaGetter() schemaUC.SchemaGetter {
	return db.schemaGetter
}

func (db *DB) GetSchema() schema.Schema {
	return db.schemaGetter.GetSchemaSkipAuth()
}

func (db *DB) GetConfig() Config {
	return db.config
}

func (db *DB) GetRemoteIndex() sharding.RemoteIndexClient {
	return db.remoteIndex
}

func (db *DB) SetSchemaGetter(sg schemaUC.SchemaGetter) {
	db.schemaGetter = sg
}

func (db *DB) GetScheduler() *queue.Scheduler {
	return db.scheduler
}

func (db *DB) WaitForStartup(ctx context.Context) error {
	stop := db.trackStartupProgress(ctx)
	defer stop()

	err := db.init(ctx)
	if err != nil {
		return err
	}

	db.startupComplete.Store(true)
	db.scanResourceUsage()

	return nil
}

func (db *DB) StartupComplete() bool { return db.startupComplete.Load() }

const startupProgressUpdateInterval = 5 * time.Second

// StartupProgressSnapshot is a consistent reading of eager shard-loading progress
type StartupProgressSnapshot struct {
	Loaded int64
	Total  int64
}

// StartupLoadingProgress reports the most recently computed eager shard-loading
// progress: how many eagerly-loaded local shards have finished loading (loaded)
// against how many are expected to load eagerly (total).
func (db *DB) StartupLoadingProgress() *StartupProgressSnapshot {
	return db.startupProgress.Load()
}

// trackStartupProgress recomputes eager shard-loading progress on a ticker and
// caches it. It returns a stop function that halts the ticker and pins the
// final value; callers should defer it. The goroutine also exits if ctx is done.
func (db *DB) trackStartupProgress(ctx context.Context) func() {
	classNames := db.startupClassNames()

	// Publish immediately so a value is available before the first log tick.
	db.updateStartupProgress(classNames)

	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		ticker := time.NewTicker(startupProgressUpdateInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				db.updateStartupProgress(classNames)
			}
		}
	}, db.logger)

	return func() {
		close(done)
		// Pin the final value now that loading has finished.
		db.updateStartupProgress(classNames)
	}
}

// startupClassNames snapshots the current class names for the startup progress scan
func (db *DB) startupClassNames() []string {
	s := db.schemaGetter.GetSchemaSkipAuth()
	if s.Objects == nil {
		return nil
	}
	names := make([]string, 0, len(s.Objects.Classes))
	for _, class := range s.Objects.Classes {
		names = append(names, class.Class)
	}
	return names
}

// updateStartupProgress recomputes progress once and publishes it to the cached
// snapshot and the Prometheus gauges.
func (db *DB) updateStartupProgress(classNames []string) {
	loaded, total := db.scanStartupProgress(classNames)
	db.startupProgress.Store(&StartupProgressSnapshot{Loaded: loaded, Total: total})
	db.promMetrics.SetStartupShardProgress(loaded, total)
}

// scanStartupProgress computes eager shard-loading progress from scratch for the
// given classes.
//
// total starts from every HOT local shard known to the schema (eager and lazy);
// lazy shards are then discounted as they are encountered in the index maps,
// leaving the eager total. Safe to call while the DB is still loading.
func (db *DB) scanStartupProgress(classNames []string) (loaded, total int64) {
	for _, className := range classNames {
		total += db.localShardsToLoad(className)
	}

	db.indexLock.RLock()
	indices := make([]*Index, 0, len(db.indices))
	for _, idx := range db.indices {
		indices = append(indices, idx)
	}
	db.indexLock.RUnlock()

	for _, idx := range indices {
		_ = idx.ForEachShard(func(_ string, shard ShardLike) error {
			if _, ok := shard.(*LazyLoadShard); ok {
				total--
				return nil
			}
			loaded++
			return nil
		})
	}

	return loaded, total
}

// localShardsToLoad returns the number of local shards that count toward eager
// startup loading for the given class: local physical shards whose activity
// status is HOT (empty status counts as HOT)
func (db *DB) localShardsToLoad(className string) int64 {
	var count int64
	_ = db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return nil
		}
		for name, physical := range state.Physical {
			if state.IsLocalShard(name) && physical.ActivityStatus() == models.TenantActivityStatusHOT {
				count++
			}
		}
		return nil
	})
	return count
}

// IndexGetter interface defines the methods that the service uses from db.IndexGetter
// This allows for better testability by using interfaces instead of concrete types
type IndexGetter interface {
	GetIndexLike(className schema.ClassName) IndexLike
}

// IndexLike interface defines the methods that the service uses from db.Index
// This allows for better testability by using interfaces instead of concrete types
type IndexLike interface {
	ForEachShard(f func(name string, shard ShardLike) error) error
	CalculateUnloadedObjectsMetrics(ctx context.Context, tenantName string) (usagetypes.ObjectUsage, error)
	CalculateUnloadedVectorsMetrics(ctx context.Context, tenantName string) (int64, error)
}

func New(logger logrus.FieldLogger, localNodeName string, config Config,
	remoteIndex sharding.RemoteIndexClient, nodeResolver cluster.NodeResolver,
	remoteNodesClient sharding.RemoteNodeClient, replicaClient replica.Client,
	promMetrics *monitoring.PrometheusMetrics, memMonitor *memwatch.Monitor,
	nodeSelector cluster.NodeSelector, schemaReader schemaUC.SchemaReader, replicationFSM types.ReplicationFSMReader,
) (*DB, error) {
	if memMonitor == nil {
		memMonitor = memwatch.NewDummyMonitor()
	}
	metricsRegisterer := monitoring.NoopRegisterer
	if promMetrics != nil && promMetrics.Registerer != nil {
		metricsRegisterer = promMetrics.Registerer
	}

	// delete any leftover indices that were kept for backup purposes. This should only happen after a crash.
	// Dont return errors here for missing files etc, as we just want to do a best-effort cleanup.
	if err := cleanupRootPathOnStartup(config.RootPath, logger); err != nil {
		return nil, err
	}

	// resume any .deleteme cleanup that didn't finish before the last shutdown
	scanAndAsyncDeletePending(config.RootPath, logger)

	asyncReplicationScheduler, err := NewAsyncReplicationScheduler(
		context.Background(),
		config.Replication,
		promMetrics,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create async replication scheduler: %w", err)
	}

	db := &DB{
		logger:                    logger,
		localNodeName:             localNodeName,
		config:                    config,
		indices:                   map[string]*Index{},
		remoteIndex:               remoteIndex,
		nodeResolver:              nodeResolver,
		remoteNode:                sharding.NewRemoteNode(nodeResolver, remoteNodesClient),
		replicaClient:             replicaClient,
		asyncReplicationScheduler: asyncReplicationScheduler,
		promMetrics:               promMetrics,
		shutdown:                  make(chan struct{}),
		maxNumberGoroutines:       int(math.Round(config.MaxImportGoroutinesFactor * float64(runtime.GOMAXPROCS(0)))),
		resourceScanState:         newResourceScanState(),
		memMonitor:                memMonitor,
		shardLoadLimiter:          loadlimiter.NewLoadLimiter(metricsRegisterer, "database_shards", config.MaximumConcurrentShardLoads),
		bucketLoadLimiter:         loadlimiter.NewLoadLimiter(metricsRegisterer, "database_buckets", config.MaximumConcurrentBucketLoads),
		reindexer:                 NewShardReindexerV3Noop(),
		nodeSelector:              nodeSelector,
		schemaReader:              schemaReader,
		replicationFSM:            replicationFSM,
		bitmapBufPool:             roaringset.NewBitmapBufPoolNoop(),
		bitmapBufPoolClose:        func() {},
		AsyncIndexingEnabled:      config.AsyncIndexingEnabled,
	}

	if db.maxNumberGoroutines == 0 {
		return db, errors.New("no workers to add batch-jobs configured.")
	}

	schedulerOK := false
	defer func() {
		if !schedulerOK {
			db.asyncReplicationScheduler.Close()
		}
	}()

	// scheduler used by async indexing and hfresh background queues
	db.shutDownWg.Add(1)
	db.scheduler = queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		OnClose: db.shutDownWg.Done,
		Metrics: promMetrics,
	})
	db.scheduler.Start()

	if !db.AsyncIndexingEnabled {
		db.jobQueueCh = make(chan job, 100000)
		db.shutDownWg.Add(db.maxNumberGoroutines)
		for i := 0; i < db.maxNumberGoroutines; i++ {
			i := i
			enterrors.GoWrapper(func() { db.batchWorker(i == 0) }, db.logger)
		}
	}

	schedulerOK = true
	return db, nil
}

type Config struct {
	RootPath                       string
	QueryLimit                     int64
	QueryMaximumResults            int64
	QueryHybridMaximumResults      int64
	QueryNestedRefLimit            int64
	ResourceUsage                  config.ResourceUsage
	MaxImportGoroutinesFactor      float64
	LazySegmentsDisabled           bool
	SegmentInfoIntoFileNameEnabled bool
	WriteMetadataFilesEnabled      bool
	MemtablesFlushDirtyAfter       int
	MemtablesInitialSizeMB         int
	MemtablesMaxSizeMB             int
	MemtablesMinActiveSeconds      int
	MemtablesMaxActiveSeconds      int
	MinMMapSize                    int64
	MaxReuseWalSize                int64
	SegmentsCleanupIntervalSeconds int
	SeparateObjectsCompactions     bool
	MaxSegmentSize                 int64
	TrackVectorDimensions          bool
	TrackVectorDimensionsInterval  time.Duration
	UsageEnabled                   bool
	ServerVersion                  string
	GitHash                        string
	AvoidMMap                      bool
	// EnableLazyLoadShards controls lazy shard loading.
	// nil = auto-detect based on thresholds, true = always lazy-load, false = always eager-load.
	EnableLazyLoadShards                *bool
	LazyLoadShardCountThreshold         int
	LazyLoadShardSizeThresholdGB        float64
	ForceFullReplicasSearch             bool
	TransferInactivityTimeout           time.Duration
	HaltForTransferTimeout              time.Duration
	LSMEnableSegmentsChecksumValidation bool
	LSMSkipWriteClassNameEnabled        bool
	NamespacesEnabled                   bool
	Replication                         replication.GlobalConfig
	MaximumConcurrentShardLoads         int
	MaximumConcurrentBucketLoads        int
	CycleManagerRoutinesFactor          int
	IndexRangeableInMemory              bool
	ObjectsTTLBatchSize                 *configRuntime.DynamicValue[int]
	ObjectsTTLPauseEveryNoBatches       *configRuntime.DynamicValue[int]
	ObjectsTTLPauseDuration             *configRuntime.DynamicValue[time.Duration]
	ObjectsTTLConcurrencyFactor         *configRuntime.DynamicValue[float64]

	HNSWMaxLogSize                               int64
	HNSWDisableSnapshots                         bool
	HNSWSnapshotIntervalSeconds                  int
	HNSWSnapshotOnStartup                        bool
	HNSWSnapshotMinDeltaCommitlogsNumber         int
	HNSWSnapshotMinDeltaCommitlogsSizePercentage int
	HNSWWaitForCachePrefill                      bool
	HNSWFlatSearchConcurrency                    int
	HNSWAcornFilterRatio                         float64
	HNSWGeoIndexEF                               int
	VisitedListPoolMaxSize                       int

	TenantActivityReadLogLevel  *configRuntime.DynamicValue[string]
	TenantActivityWriteLogLevel *configRuntime.DynamicValue[string]
	QuerySlowLogEnabled         *configRuntime.DynamicValue[bool]
	QuerySlowLogThreshold       *configRuntime.DynamicValue[time.Duration]
	InvertedSorterDisabled      *configRuntime.DynamicValue[bool]
	LazyPropertyLengthsEnabled  *configRuntime.DynamicValue[bool]
	MaintenanceModeEnabled      func() bool
	AsyncIndexingEnabled        bool

	HFreshEnabled   bool
	OperationalMode *configRuntime.DynamicValue[string]

	DisableDimensionMetrics *configRuntime.DynamicValue[bool]
}

// GetIndex returns the index if it exists or nil if it doesn't
// by default it will retry 3 times between 0-150 ms to get the index
// to handle the eventual consistency.
func (db *DB) GetIndex(className schema.ClassName) *Index {
	var (
		index  *Index
		exists bool
	)
	// TODO-RAFT remove backoff. Eventual consistency handled by versioning
	backoff.Retry(func() error {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		index, exists = db.indices[indexID(className)]
		if !exists {
			return fmt.Errorf("index for class %v not found locally", index)
		}
		return nil
	}, utils.NewBackoff())

	return index
}

// WaitForLocalInflightWrites blocks until this node's in-flight coordinated
// writes to the given shard have drained, or ctx is done.
//
// If the index is not found, or if the index has no replicator (i.e. this node is not a replica for the given shard), this method returns immediately without error.
func (db *DB) WaitForLocalInflightWrites(ctx context.Context, class, shard string) error {
	var index *Index
	if ok := func() bool {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()
		index = db.indices[indexID(schema.ClassName(class))]
		if index == nil || index.replicator == nil {
			return false
		}
		index.dropIndex.RLock()
		return true
	}(); !ok {
		return fmt.Errorf("index for class %v not found locally or has no replicator", class)
	}
	defer index.dropIndex.RUnlock()
	return index.replicator.WaitForDrain(ctx, shard)
}

// GetLocalShardNames returns the names of all shards local to this node for
// the given collection. Returns an error if the collection is not found or has
// no local shards.
func (db *DB) GetLocalShardNames(collection string) ([]string, error) {
	index := db.GetIndex(schema.ClassName(collection))
	if index == nil {
		return nil, fmt.Errorf("collection %q not found", collection)
	}
	var names []string
	if err := index.ForEachShard(func(name string, _ ShardLike) error {
		names = append(names, name)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("collection %q has no local shards", collection)
	}
	return names, nil
}

// IndexExists returns if an index exists
func (db *DB) IndexExists(className schema.ClassName) bool {
	return db.GetIndex(className) != nil
}

// TODO-RAFT: Because of interfaces and import order we can't have this function just return the same index interface
// for both sharding and replica usage. With a refactor of the interfaces this can be done and we can remove the
// deduplication

// GetIndexForIncomingSharding returns the index if it exists or nil if it doesn't
// by default it will retry 3 times between 0-150 ms to get the index
// to handle the eventual consistency.
func (db *DB) GetIndexForIncomingSharding(className schema.ClassName) sharding.RemoteIndexIncomingRepo {
	index := db.GetIndex(className)
	if index == nil {
		return nil
	}

	return index
}

// DeleteIndex deletes the index
func (db *DB) DeleteIndex(className schema.ClassName) error {
	index := db.GetIndex(className)
	if index == nil {
		return nil
	}

	// drop index
	db.indexLock.Lock()
	defer db.indexLock.Unlock()

	index.dropIndex.Lock()
	defer index.dropIndex.Unlock()
	if err := index.drop(); err != nil {
		db.logger.WithField("action", "delete_index").WithField("class", className).Error(err)
	}

	delete(db.indices, indexID(className))

	if err := db.promMetrics.DeleteClass(className.String()); err != nil {
		db.logger.Error("can't delete prometheus metrics", err)
	}
	return nil
}

func (db *DB) Shutdown(ctx context.Context) error {
	db.shutdown <- struct{}{}
	db.bitmapBufPoolClose()

	if !db.AsyncIndexingEnabled {
		// shut down the workers that add objects to
		for i := 0; i < db.maxNumberGoroutines; i++ {
			db.jobQueueCh <- job{
				index: -1,
			}
		}
	}

	// shut down the async workers
	err := db.scheduler.Close(ctx)
	if err != nil {
		return errors.Wrap(err, "close scheduler")
	}

	if db.metricsObserver != nil {
		db.metricsObserver.Shutdown()
	}

	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	defer db.asyncReplicationScheduler.Close()
	for id, index := range db.indices {
		if err := index.Shutdown(ctx); err != nil {
			return errors.Wrapf(err, "shutdown index %q", id)
		}
	}

	db.shutDownWg.Wait() // wait until job queue shutdown is completed

	if db.AsyncIndexingEnabled {
		db.indexCheckpoints.Close()
	}

	return nil
}

type job struct {
	object  *storobj.Object
	status  objectInsertStatus
	index   int
	ctx     context.Context
	batcher *objectsBatcher
}

func (db *DB) batchWorker(first bool) {
	objectCounter := 0
	checkTime := time.Now().Add(time.Second)
	for jobToAdd := range db.jobQueueCh {
		if jobToAdd.index < 0 {
			db.shutDownWg.Done()
			return
		}
		func() {
			defer jobToAdd.batcher.wg.Done()
			jobToAdd.batcher.storeSingleObjectInAdditionalStorage(jobToAdd.ctx, jobToAdd.object, jobToAdd.status, jobToAdd.index)
		}()

		objectCounter += 1
		if first && time.Now().After(checkTime) { // only have one worker report the rate per second
			db.ratePerSecond.Store(int64(objectCounter * db.maxNumberGoroutines))

			objectCounter = 0
			checkTime = time.Now().Add(time.Second)
		}
	}
}

func (db *DB) SetReindexer(reindexer ShardReindexerV3) {
	db.reindexer = reindexer
}

func (db *DB) SetNodeSelector(nodeSelector cluster.NodeSelector) {
	db.nodeSelector = nodeSelector
}

func (db *DB) SetSchemaReader(schemaReader schemaUC.SchemaReader) {
	db.schemaReader = schemaReader
}

func (db *DB) SetReplicationFSM(replicationFsm *clusterReplication.ShardReplicationFSM) {
	db.replicationFSM = replicationFsm
}

func (db *DB) SetBitmapBufPool(bufPool roaringset.BitmapBufPool, close func()) {
	db.bitmapBufPool = bufPool
	db.bitmapBufPoolClose = close
}

func (db *DB) SetTenantsActivityManager(tenantsManager schemaUC.TenantsActivityManager) {
	db.tenantsManager = tenantsManager
}
