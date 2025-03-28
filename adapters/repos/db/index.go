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
	"os"
	"path"
	"runtime"
	"runtime/debug"
	golangSort "sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/autocut"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
	runtimeconfig "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (

	// Use runtime.GOMAXPROCS instead of runtime.NumCPU because NumCPU returns
	// the physical CPU cores. However, in a containerization context, that might
	// not be what we want. The physical node could have 128 cores, but we could
	// be cgroup-limited to 2 cores. In that case, we want 2 to be our limit, not
	// 128. It isn't guaranteed that MAXPROCS reflects the cgroup limit, but at
	// least there is a chance that it was set correctly. If not, it defaults to
	// NumCPU anyway, so we're not any worse off.
	_NUMCPU          = runtime.GOMAXPROCS(0)
	ErrShardNotFound = errors.New("shard not found")
)

// shardMap is a syn.Map which specialized in storing shards
type shardMap sync.Map

// Range calls f sequentially for each key and value present in the map.
// If f returns an error, range stops the iteration
func (m *shardMap) Range(f func(name string, shard ShardLike) error) (err error) {
	(*sync.Map)(m).Range(func(key, value any) bool {
		err = f(key.(string), value.(ShardLike))
		return err == nil
	})
	return err
}

// RangeConcurrently calls f for each key and value present in the map with at
// most _NUMCPU executors running in parallel. As opposed to [Range] it does
// not guarantee an exit on the first error.
func (m *shardMap) RangeConcurrently(logger logrus.FieldLogger, f func(name string, shard ShardLike) error) (err error) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(_NUMCPU)
	(*sync.Map)(m).Range(func(key, value any) bool {
		name, shard := key.(string), value.(ShardLike)
		eg.Go(func() error {
			return f(name, shard)
		}, name, shard)
		return true
	})

	return eg.Wait()
}

// Load returns the shard or nil if no shard is present.
func (m *shardMap) Load(name string) ShardLike {
	v, ok := (*sync.Map)(m).Load(name)
	if !ok {
		return nil
	}

	shard, ok := v.(ShardLike)
	if !ok {
		return nil
	}
	return shard
}

// Store sets a shard giving its name and value
func (m *shardMap) Store(name string, shard ShardLike) {
	(*sync.Map)(m).Store(name, shard)
}

// Swap swaps the shard for a key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (m *shardMap) Swap(name string, shard ShardLike) (previous ShardLike, loaded bool) {
	v, ok := (*sync.Map)(m).Swap(name, shard)
	if v == nil || !ok {
		return nil, ok
	}
	return v.(ShardLike), ok
}

// CompareAndSwap swaps the old and new values for key if the value stored in the map is equal to old.
func (m *shardMap) CompareAndSwap(name string, old, new ShardLike) bool {
	return (*sync.Map)(m).CompareAndSwap(name, old, new)
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (m *shardMap) LoadAndDelete(name string) (ShardLike, bool) {
	v, ok := (*sync.Map)(m).LoadAndDelete(name)
	if v == nil || !ok {
		return nil, ok
	}
	return v.(ShardLike), ok
}

// Index is the logical unit which contains all the data for one particular
// class. An index can be further broken up into self-contained units, called
// Shards, to allow for easy distribution across Nodes
type Index struct {
	classSearcher           inverted.ClassSearcher // to allow for nested by-references searches
	shards                  shardMap
	Config                  IndexConfig
	globalreplicationConfig *replication.GlobalConfig

	getSchema  schemaUC.SchemaGetter
	logger     logrus.FieldLogger
	remote     *sharding.RemoteIndex
	stopwords  *stopwords.Detector
	replicator *replica.Replicator

	vectorIndexUserConfigLock sync.Mutex
	vectorIndexUserConfig     schemaConfig.VectorIndexConfig
	vectorIndexUserConfigs    map[string]schemaConfig.VectorIndexConfig

	partitioningEnabled bool

	invertedIndexConfig     schema.InvertedIndexConfig
	invertedIndexConfigLock sync.Mutex

	// This lock should be used together with the db indexLock.
	//
	// The db indexlock locks the map that contains all indices against changes and should be used while iterating.
	// This lock protects this specific index form being deleted while in use. Use Rlock to signal that it is in use.
	// This way many goroutines can use a specific index in parallel. The delete-routine will try to acquire a RWlock.
	//
	// Usage:
	// Lock the whole db using db.indexLock
	// pick the indices you want and Rlock them
	// unlock db.indexLock
	// Use the indices
	// RUnlock all picked indices
	dropIndex sync.RWMutex

	metrics          *Metrics
	centralJobQueue  chan job
	scheduler        *queue.Scheduler
	indexCheckpoints *indexcheckpoint.Checkpoints

	cycleCallbacks *indexCycleCallbacks

	shardTransferMutex shardTransfer
	lastBackup         atomic.Pointer[BackupState]

	// canceled when either Shutdown or Drop called
	closingCtx    context.Context
	closingCancel context.CancelFunc

	// always true if lazy shard loading is off, in the case of lazy shard
	// loading will be set to true once the last shard was loaded.
	allShardsReady   atomic.Bool
	allocChecker     memwatch.AllocChecker
	shardCreateLocks *esync.KeyLocker

	replicationConfigLock sync.RWMutex

	shardLoadLimiter ShardLoadLimiter

	closeLock sync.RWMutex
	closed    bool
}

func (i *Index) ID() string {
	return indexID(i.Config.ClassName)
}

func (i *Index) path() string {
	return path.Join(i.Config.RootPath, i.ID())
}

type nodeResolver interface {
	AllHostnames() []string
	NodeHostname(nodeName string) (string, bool)
}

// NewIndex creates an index with the specified amount of shards, using only
// the shards that are local to a node
func NewIndex(ctx context.Context, cfg IndexConfig,
	shardState *sharding.State, invertedIndexConfig schema.InvertedIndexConfig,
	vectorIndexUserConfig schemaConfig.VectorIndexConfig,
	vectorIndexUserConfigs map[string]schemaConfig.VectorIndexConfig,
	router *router.Router, sg schemaUC.SchemaGetter,
	cs inverted.ClassSearcher, logger logrus.FieldLogger,
	nodeResolver nodeResolver, remoteClient sharding.RemoteIndexClient,
	replicaClient replica.Client,
	globalReplicationConfig *replication.GlobalConfig,
	promMetrics *monitoring.PrometheusMetrics, class *models.Class, jobQueueCh chan job,
	scheduler *queue.Scheduler,
	indexCheckpoints *indexcheckpoint.Checkpoints,
	allocChecker memwatch.AllocChecker,
) (*Index, error) {
	sd, err := stopwords.NewDetectorFromConfig(invertedIndexConfig.Stopwords)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new index")
	}

	if cfg.QueryNestedRefLimit == 0 {
		cfg.QueryNestedRefLimit = config.DefaultQueryNestedCrossReferenceLimit
	}

	if vectorIndexUserConfigs == nil {
		vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{}
	}

	index := &Index{
		Config:                  cfg,
		globalreplicationConfig: globalReplicationConfig,
		getSchema:               sg,
		logger:                  logger,
		classSearcher:           cs,
		vectorIndexUserConfig:   vectorIndexUserConfig,
		invertedIndexConfig:     invertedIndexConfig,
		vectorIndexUserConfigs:  vectorIndexUserConfigs,
		stopwords:               sd,
		partitioningEnabled:     shardState.PartitioningEnabled,
		remote:                  sharding.NewRemoteIndex(cfg.ClassName.String(), sg, nodeResolver, remoteClient),
		metrics:                 NewMetrics(logger, promMetrics, cfg.ClassName.String(), "n/a"),
		centralJobQueue:         jobQueueCh,
		shardTransferMutex:      shardTransfer{log: logger, retryDuration: mutexRetryDuration, notifyDuration: mutexNotifyDuration},
		scheduler:               scheduler,
		indexCheckpoints:        indexCheckpoints,
		allocChecker:            allocChecker,
		shardCreateLocks:        esync.NewKeyLocker(),
		shardLoadLimiter:        cfg.ShardLoadLimiter,
	}

	getDeletionStrategy := func() string {
		return index.DeletionStrategy()
	}

	// TODO: Fix replica router instantiation to be at the top level
	index.replicator = replica.NewReplicator(cfg.ClassName.String(), router, sg.NodeName(), getDeletionStrategy, replicaClient, logger)

	index.closingCtx, index.closingCancel = context.WithCancel(context.Background())

	index.initCycleCallbacks()

	if err := index.checkSingleShardMigration(); err != nil {
		return nil, errors.Wrap(err, "migrating sharding state from previous version")
	}

	if err := os.MkdirAll(index.path(), os.ModePerm); err != nil {
		return nil, fmt.Errorf("init index %q: %w", index.ID(), err)
	}

	if err := index.initAndStoreShards(ctx, class, shardState, promMetrics); err != nil {
		return nil, err
	}

	index.cycleCallbacks.compactionCycle.Start()
	index.cycleCallbacks.compactionAuxCycle.Start()
	index.cycleCallbacks.flushCycle.Start()

	return index, nil
}

// since called in Index's constructor there is no risk same shard will be inited/created in parallel,
// therefore shardCreateLocks are not used here
func (i *Index) initAndStoreShards(ctx context.Context, class *models.Class,
	shardState *sharding.State, promMetrics *monitoring.PrometheusMetrics,
) error {
	if i.Config.DisableLazyLoadShards {
		eg := enterrors.NewErrorGroupWrapper(i.logger)
		eg.SetLimit(_NUMCPU)

		for _, shardName := range shardState.AllLocalPhysicalShards() {
			physical := shardState.Physical[shardName]
			if physical.ActivityStatus() != models.TenantActivityStatusHOT {
				// do not instantiate inactive shard
				continue
			}

			shardName := shardName // prevent loop variable capture
			eg.Go(func() error {
				if err := i.shardLoadLimiter.Acquire(ctx); err != nil {
					return fmt.Errorf("acquiring permit to load shard: %w", err)
				}
				defer i.shardLoadLimiter.Release()

				shard, err := NewShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.scheduler, i.indexCheckpoints)
				if err != nil {
					return fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
				}

				i.shards.Store(shardName, shard)
				return nil
			}, shardName)
		}

		if err := eg.Wait(); err != nil {
			return err
		}

		i.allShardsReady.Store(true)
		return nil
	}

	// shards to lazily initialize are fetch once and in the same goroutine the index is initialized
	// so to avoid races if shards are deleted before being initialized
	shards := shardState.AllLocalPhysicalShards()

	for _, shardName := range shards {
		physical := shardState.Physical[shardName]
		if physical.ActivityStatus() != models.TenantActivityStatusHOT {
			// do not instantiate inactive shard
			continue
		}

		shard := NewLazyLoadShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.indexCheckpoints, i.allocChecker, i.shardLoadLimiter)
		i.shards.Store(shardName, shard)
	}

	initLazyShardsInBackground := func() {
		defer i.allShardsReady.Store(true)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		now := time.Now()

		for _, shardName := range shards {
			// prioritize closingCtx over ticker:
			// check closing again in case of ticker was selected when both
			// cases where available
			select {
			case <-i.closingCtx.Done():
				// break loop by returning error
				i.logger.
					WithField("action", "load_all_shards").
					Errorf("failed to load all shards due to ctx close: %v", i.closingCtx.Err())
				return
			case <-ticker.C:
				select {
				case <-i.closingCtx.Done():
					// break loop by returning error
					i.logger.
						WithField("action", "load_all_shards").
						Errorf("failed to load all shards due to ticker ctx close: %v", i.closingCtx.Err())
					return
				default:
					err := i.loadLocalShardIfActive(shardName)
					if err != nil {
						i.logger.
							WithField("action", "load_shard").
							WithField("shard_name", shardName).
							Errorf("failed to load shard: %v", err)
						return
					}
				}
			}
		}

		i.logger.
			WithField("action", "load_all_shards").
			WithField("took", time.Since(now).String()).
			Debug("finished loading all shards")
	}

	enterrors.GoWrapper(initLazyShardsInBackground, i.logger)

	return nil
}

func (i *Index) loadLocalShardIfActive(shardName string) error {
	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	// check if set to inactive in the meantime by concurrent call
	shard := i.shards.Load(shardName)
	if shard == nil {
		return nil
	}

	lazyShard, ok := shard.(*LazyLoadShard)
	if ok {
		return lazyShard.Load(context.Background())
	}

	return nil
}

// used to init/create shard in different moments of index's lifecycle, therefore it needs to be called
// within shardCreateLocks to prevent parallel create/init of the same shard
func (i *Index) initShard(ctx context.Context, shardName string, class *models.Class,
	promMetrics *monitoring.PrometheusMetrics, disableLazyLoad bool,
) (ShardLike, error) {
	if disableLazyLoad {
		if err := i.allocChecker.CheckMappingAndReserve(3, int(lsmkv.FlushAfterDirtyDefault.Seconds())); err != nil {
			return nil, errors.Wrap(err, "memory pressure: cannot init shard")
		}

		if err := i.shardLoadLimiter.Acquire(ctx); err != nil {
			return nil, fmt.Errorf("acquiring permit to load shard: %w", err)
		}
		defer i.shardLoadLimiter.Release()

		shard, err := NewShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.scheduler, i.indexCheckpoints)
		if err != nil {
			return nil, fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
		}

		return shard, nil
	}

	shard := NewLazyLoadShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.indexCheckpoints, i.allocChecker, i.shardLoadLimiter)
	return shard, nil
}

// Iterate over all objects in the index, applying the callback function to each one.  Adding or removing objects during iteration is not supported.
func (i *Index) IterateObjects(ctx context.Context, cb func(index *Index, shard ShardLike, object *storobj.Object) error) (err error) {
	return i.ForEachShard(func(_ string, shard ShardLike) error {
		wrapper := func(object *storobj.Object) error {
			return cb(i, shard, object)
		}
		bucket := shard.Store().Bucket(helpers.ObjectsBucketLSM)
		return bucket.IterateObjects(ctx, wrapper)
	})
}

// ForEachShard applies func f on each shard in the index.
//
// WARNING: only use this if you expect all LazyLoadShards to be loaded!
// Calling this method may lead to shards being force-loaded, causing
// unexpected CPU spikes. If you only want to apply f on loaded shards,
// call ForEachLoadedShard instead.
func (i *Index) ForEachShard(f func(name string, shard ShardLike) error) error {
	return i.shards.Range(f)
}

func (i *Index) ForEachLoadedShard(f func(name string, shard ShardLike) error) error {
	return i.shards.Range(func(name string, shard ShardLike) error {
		// Skip lazy loaded shard which are not loaded
		if asLazyLoadShard, ok := shard.(*LazyLoadShard); ok {
			if !asLazyLoadShard.isLoaded() {
				return nil
			}
		}
		return f(name, shard)
	})
}

func (i *Index) ForEachShardConcurrently(f func(name string, shard ShardLike) error) error {
	return i.shards.RangeConcurrently(i.logger, f)
}

// Iterate over all objects in the shard, applying the callback function to each one.  Adding or removing objects during iteration is not supported.
func (i *Index) IterateShards(ctx context.Context, cb func(index *Index, shard ShardLike) error) (err error) {
	return i.ForEachShard(func(key string, shard ShardLike) error {
		return cb(i, shard)
	})
}

func (i *Index) addProperty(ctx context.Context, props ...*models.Property) error {
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU)

	i.ForEachShard(func(key string, shard ShardLike) error {
		shard.initPropertyBuckets(ctx, eg, props...)
		return nil
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrapf(err, "extend idx '%s' with properties '%v", i.ID(), props)
	}
	return nil
}

func (i *Index) updateVectorIndexConfig(ctx context.Context,
	updated schemaConfig.VectorIndexConfig,
) error {
	// an updated is not specific to one shard, but rather all
	err := i.ForEachShard(func(name string, shard ShardLike) error {
		// At the moment, we don't do anything in an update that could fail, but
		// technically this should be part of some sort of a two-phase commit  or
		// have another way to rollback if we have updates that could potentially
		// fail in the future. For now that's not a realistic risk.
		if err := shard.UpdateVectorIndexConfig(ctx, updated); err != nil {
			return errors.Wrapf(err, "shard %s", name)
		}
		return nil
	})
	if err != nil {
		return err
	}
	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	i.vectorIndexUserConfig = updated

	return nil
}

func (i *Index) updateVectorIndexConfigs(ctx context.Context,
	updated map[string]schemaConfig.VectorIndexConfig,
) error {
	err := i.ForEachShard(func(name string, shard ShardLike) error {
		if err := shard.UpdateVectorIndexConfigs(ctx, updated); err != nil {
			return fmt.Errorf("shard %q: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	for targetName, targetCfg := range updated {
		i.vectorIndexUserConfigs[targetName] = targetCfg
	}

	return nil
}

func (i *Index) getInvertedIndexConfig() schema.InvertedIndexConfig {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	return i.invertedIndexConfig
}

func (i *Index) updateInvertedIndexConfig(ctx context.Context,
	updated schema.InvertedIndexConfig,
) error {
	i.invertedIndexConfigLock.Lock()
	defer i.invertedIndexConfigLock.Unlock()

	i.invertedIndexConfig = updated

	return nil
}

func (i *Index) asyncReplicationGloballyDisabled() bool {
	return runtimeconfig.GetOverrides(i.globalreplicationConfig.AsyncReplicationDisabled, i.globalreplicationConfig.AsyncReplicationDisabledFn)
}

func (i *Index) updateReplicationConfig(ctx context.Context, cfg *models.ReplicationConfig) error {
	i.replicationConfigLock.Lock()
	defer i.replicationConfigLock.Unlock()

	i.Config.ReplicationFactor = cfg.Factor
	i.Config.DeletionStrategy = cfg.DeletionStrategy
	i.Config.AsyncReplicationEnabled = cfg.AsyncEnabled && i.Config.ReplicationFactor > 1 && !i.asyncReplicationGloballyDisabled()

	err := i.ForEachLoadedShard(func(name string, shard ShardLike) error {
		if err := shard.updateAsyncReplicationConfig(ctx, i.Config.AsyncReplicationEnabled); err != nil {
			return fmt.Errorf("updating async replication on shard %q: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (i *Index) ReplicationFactor() int64 {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.ReplicationFactor
}

func (i *Index) DeletionStrategy() string {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.DeletionStrategy
}

type IndexConfig struct {
	RootPath                            string
	ClassName                           schema.ClassName
	QueryMaximumResults                 int64
	QueryNestedRefLimit                 int64
	ResourceUsage                       config.ResourceUsage
	MemtablesFlushDirtyAfter            int
	MemtablesInitialSizeMB              int
	MemtablesMaxSizeMB                  int
	MemtablesMinActiveSeconds           int
	MemtablesMaxActiveSeconds           int
	SegmentsCleanupIntervalSeconds      int
	SeparateObjectsCompactions          bool
	CycleManagerRoutinesFactor          int
	MaxSegmentSize                      int64
	HNSWMaxLogSize                      int64
	HNSWWaitForCachePrefill             bool
	HNSWFlatSearchConcurrency           int
	HNSWAcornFilterRatio                float64
	VisitedListPoolMaxSize              int
	ReplicationFactor                   int64
	DeletionStrategy                    string
	AsyncReplicationEnabled             bool
	AvoidMMap                           bool
	DisableLazyLoadShards               bool
	ForceFullReplicasSearch             bool
	LSMEnableSegmentsChecksumValidation bool
	TrackVectorDimensions               bool
	ShardLoadLimiter                    ShardLoadLimiter
}

func indexID(class schema.ClassName) string {
	return strings.ToLower(string(class))
}

func (i *Index) determineObjectShard(ctx context.Context, id strfmt.UUID, tenant string) (string, error) {
	return i.determineObjectShardByStatus(ctx, id, tenant, nil)
}

func (i *Index) determineObjectShardByStatus(ctx context.Context, id strfmt.UUID, tenant string, shardsStatus map[string]string) (string, error) {
	if tenant == "" {
		uuid, err := uuid.Parse(id.String())
		if err != nil {
			return "", fmt.Errorf("parse uuid: %q", id.String())
		}

		uuidBytes, err := uuid.MarshalBinary() // cannot error
		if err != nil {
			return "", fmt.Errorf("marshal uuid: %q", id.String())
		}
		return i.getSchema.ShardFromUUID(i.Config.ClassName.String(), uuidBytes), nil
	}

	var err error
	if len(shardsStatus) == 0 {
		shardsStatus, err = i.getSchema.TenantsShards(ctx, i.Config.ClassName.String(), tenant)
		if err != nil {
			return "", err
		}
	}

	if status := shardsStatus[tenant]; status != "" {
		if status == models.TenantActivityStatusHOT {
			return tenant, nil
		}
		return "", objects.NewErrMultiTenancy(fmt.Errorf("%w: '%s'", enterrors.ErrTenantNotActive, tenant))
	}
	class := i.getSchema.ReadOnlyClass(i.Config.ClassName.String())
	if class == nil {
		return "", fmt.Errorf("class %q not found in schema", i.Config.ClassName)
	}
	return "", objects.NewErrMultiTenancy(
		fmt.Errorf("%w: %q", enterrors.ErrTenantNotFound, tenant))
}

func (i *Index) putObject(ctx context.Context, object *storobj.Object,
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) error {
	if err := i.validateMultiTenancy(object.Object.Tenant); err != nil {
		return err
	}

	if i.Config.ClassName != object.Class() {
		return fmt.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	shardName, err := i.determineObjectShard(ctx, object.ID(), object.Object.Tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return fmt.Errorf("determine shard: %w", err)
		default:
			return objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := types.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.PutObject(ctx, shardName, object, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate insertion: shard=%q: %w", shardName, err)
		}
		return nil
	}

	// no replication, remote shard (or local not yet inited)
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}

	if shard == nil {
		if err := i.remote.PutObject(ctx, shardName, object, schemaVersion); err != nil {
			return fmt.Errorf("put remote object: shard=%q: %w", shardName, err)
		}
		return nil
	}
	defer release()

	// no replication, local shard
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	err = shard.PutObject(ctx, object)
	if err != nil {
		return fmt.Errorf("put local object: shard=%q: %w", shardName, err)
	}

	return nil
}

func (i *Index) IncomingPutObject(ctx context.Context, shardName string,
	object *storobj.Object, schemaVersion uint64,
) error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/weaviate/weaviate/issues/1775
	if err := i.parseDateFieldsInProps(object.Object.Properties); err != nil {
		return err
	}

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.PutObject(ctx, object)
}

func (i *Index) replicationEnabled() bool {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.ReplicationFactor > 1
}

func (i *Index) asyncReplicationEnabled() bool {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.ReplicationFactor > 1 && i.Config.AsyncReplicationEnabled && !i.asyncReplicationGloballyDisabled()
}

// parseDateFieldsInProps checks the schema for the current class for which
// fields are date fields, then - if they are set - parses them accordingly.
// Works for both date and date[].
func (i *Index) parseDateFieldsInProps(props interface{}) error {
	if props == nil {
		return nil
	}

	propMap, ok := props.(map[string]interface{})
	if !ok {
		// don't know what to do with this
		return nil
	}

	c := i.getSchema.ReadOnlyClass(i.Config.ClassName.String())
	if c == nil {
		return fmt.Errorf("class %s not found in schema", i.Config.ClassName)
	}

	for _, prop := range c.Properties {
		if prop.DataType[0] == string(schema.DataTypeDate) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			parsed, err := parseAsStringToTime(raw)
			if err != nil {
				return errors.Wrapf(err, "time prop %q", prop.Name)
			}

			propMap[prop.Name] = parsed
		}

		if prop.DataType[0] == string(schema.DataTypeDateArray) {
			raw, ok := propMap[prop.Name]
			if !ok {
				// prop is not set, nothing to do
				continue
			}

			asSlice, ok := raw.([]string)
			if !ok {
				return errors.Errorf("parse as time array, expected []interface{} got %T",
					raw)
			}
			parsedSlice := make([]interface{}, len(asSlice))
			for j := range asSlice {
				parsed, err := parseAsStringToTime(interface{}(asSlice[j]))
				if err != nil {
					return errors.Wrapf(err, "time array prop %q at pos %d", prop.Name, j)
				}

				parsedSlice[j] = parsed
			}
			propMap[prop.Name] = parsedSlice

		}
	}

	return nil
}

func parseAsStringToTime(in interface{}) (time.Time, error) {
	var parsed time.Time
	var err error

	asString, ok := in.(string)
	if !ok {
		return parsed, errors.Errorf("parse as time, expected string got %T", in)
	}

	parsed, err = time.Parse(time.RFC3339, asString)
	if err != nil {
		return parsed, err
	}

	return parsed, nil
}

// return value []error gives the error for the index with the positions
// matching the inputs
func (i *Index) putObjectBatch(ctx context.Context, objects []*storobj.Object,
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) []error {
	type objsAndPos struct {
		objects []*storobj.Object
		pos     []int
	}
	out := make([]error, len(objects))
	if i.replicationEnabled() && replProps == nil {
		replProps = defaultConsistency()
	}

	byShard := map[string]objsAndPos{}
	// get all tenants shards
	tenants := make([]string, len(objects))
	tenantsStatus := map[string]string{}
	var err error
	for _, obj := range objects {
		if obj.Object.Tenant == "" {
			continue
		}
		tenants = append(tenants, obj.Object.Tenant)
	}

	if len(tenants) > 0 {
		tenantsStatus, err = i.getSchema.TenantsShards(ctx, i.Config.ClassName.String(), tenants...)
		if err != nil {
			return []error{err}
		}
	}

	for pos, obj := range objects {
		if err := i.validateMultiTenancy(obj.Object.Tenant); err != nil {
			out[pos] = err
			continue
		}
		shardName, err := i.determineObjectShardByStatus(ctx, obj.ID(), obj.Object.Tenant, tenantsStatus)
		if err != nil {
			out[pos] = err
			continue
		}

		group := byShard[shardName]
		group.objects = append(group.objects, obj)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	wg := &sync.WaitGroup{}
	for shardName, group := range byShard {
		shardName := shardName
		group := group
		wg.Add(1)
		f := func() {
			defer wg.Done()

			defer func() {
				err := recover()
				if err != nil {
					for pos := range group.pos {
						out[pos] = fmt.Errorf("an unexpected error occurred: %s", err)
					}
					fmt.Fprintf(os.Stderr, "panic: %s\n", err)
					entsentry.Recover(err)
					debug.PrintStack()
				}
			}()
			var errs []error
			if replProps != nil {
				errs = i.replicator.PutObjects(ctx, shardName, group.objects,
					types.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
			} else {
				shard, release, err := i.GetShard(ctx, shardName)
				if err != nil {
					errs = []error{err}
				} else if shard != nil {
					i.shardTransferMutex.RLockGuard(func() error {
						defer release()
						errs = shard.PutObjectBatch(ctx, group.objects)
						return nil
					})
				} else {
					errs = i.remote.BatchPutObjects(ctx, shardName, group.objects, schemaVersion)
				}
			}

			for i, err := range errs {
				desiredPos := group.pos[i]
				out[desiredPos] = err
			}
		}
		enterrors.GoWrapper(f, i.logger)
	}

	wg.Wait()

	return out
}

func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}

	return out
}

func (i *Index) IncomingBatchPutObjects(ctx context.Context, shardName string,
	objects []*storobj.Object, schemaVersion uint64,
) []error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	// This is a bit hacky, the problem here is that storobj.Parse() currently
	// misses date fields as it has no way of knowing that a date-formatted
	// string was actually a date type. However, adding this functionality to
	// Parse() would break a lot of code, because it currently
	// schema-independent. To find out if a field is a date or date[], we need to
	// involve the schema, thus why we are doing it here. This was discovered as
	// part of https://github.com/weaviate/weaviate/issues/1775
	for j := range objects {
		if err := i.parseDateFieldsInProps(objects[j].Object.Properties); err != nil {
			return duplicateErr(err, len(objects))
		}
	}

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return duplicateErr(err, len(objects))
	}
	defer release()

	return shard.PutObjectBatch(ctx, objects)
}

// return value map[int]error gives the error for the index as it received it
func (i *Index) AddReferencesBatch(ctx context.Context, refs objects.BatchReferences,
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) []error {
	type refsAndPos struct {
		refs objects.BatchReferences
		pos  []int
	}
	if i.replicationEnabled() && replProps == nil {
		replProps = defaultConsistency()
	}

	byShard := map[string]refsAndPos{}
	out := make([]error, len(refs))

	for pos, ref := range refs {
		if err := i.validateMultiTenancy(ref.Tenant); err != nil {
			out[pos] = err
			continue
		}
		shardName, err := i.determineObjectShard(ctx, ref.From.TargetID, ref.Tenant)
		if err != nil {
			out[pos] = err
			continue
		}

		group := byShard[shardName]
		group.refs = append(group.refs, ref)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	for shardName, group := range byShard {
		var errs []error
		if i.replicationEnabled() {
			errs = i.replicator.AddReferences(ctx, shardName, group.refs, types.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
		} else {
			shard, release, err := i.GetShard(ctx, shardName)
			if err != nil {
				errs = duplicateErr(err, len(group.refs))
			} else if shard != nil {
				i.shardTransferMutex.RLockGuard(func() error {
					defer release()
					errs = shard.AddReferencesBatch(ctx, group.refs)
					return nil
				})
			} else {
				errs = i.remote.BatchAddReferences(ctx, shardName, group.refs, schemaVersion)
			}
		}

		for i, err := range errs {
			desiredPos := group.pos[i]
			out[desiredPos] = err
		}
	}

	return out
}

func (i *Index) IncomingBatchAddReferences(ctx context.Context, shardName string,
	refs objects.BatchReferences, schemaVersion uint64,
) []error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return duplicateErr(err, len(refs))
	}
	defer release()

	return shard.AddReferencesBatch(ctx, refs)
}

func (i *Index) objectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties,
	replProps *additional.ReplicationProperties, tenant string,
) (*storobj.Object, error) {
	if err := i.validateMultiTenancy(tenant); err != nil {
		return nil, err
	}

	shardName, err := i.determineObjectShard(ctx, id, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return nil, objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return nil, fmt.Errorf("determine shard: %w", err)
		default:
			return nil, objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	var obj *storobj.Object

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		if replProps.NodeName != "" {
			obj, err = i.replicator.NodeObject(ctx, replProps.NodeName, shardName, id, props, addl)
		} else {
			obj, err = i.replicator.GetOne(ctx, types.ConsistencyLevel(replProps.ConsistencyLevel), shardName, id, props, addl)
		}
		return obj, err
	}

	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return obj, err
	}

	if shard != nil {
		defer release()
		if obj, err = shard.ObjectByID(ctx, id, props, addl); err != nil {
			return obj, fmt.Errorf("get local object: shard=%s: %w", shardName, err)
		}
	} else {
		if obj, err = i.remote.GetObject(ctx, shardName, id, props, addl); err != nil {
			return obj, fmt.Errorf("get remote object: shard=%s: %w", shardName, err)
		}
	}

	return obj, nil
}

func (i *Index) IncomingGetObject(ctx context.Context, shardName string,
	id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.ObjectByID(ctx, id, props, additional)
}

func (i *Index) IncomingMultiGetObjects(ctx context.Context, shardName string,
	ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.MultiObjectByID(ctx, wrapIDsInMulti(ids))
}

func (i *Index) multiObjectByID(ctx context.Context,
	query []multi.Identifier, tenant string,
) ([]*storobj.Object, error) {
	if err := i.validateMultiTenancy(tenant); err != nil {
		return nil, err
	}

	type idsAndPos struct {
		ids []multi.Identifier
		pos []int
	}

	byShard := map[string]idsAndPos{}
	for pos, id := range query {
		shardName, err := i.determineObjectShard(ctx, strfmt.UUID(id.ID), tenant)
		if err != nil {
			switch {
			case errors.As(err, &objects.ErrMultiTenancy{}):
				return nil, objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
			case errors.As(err, &authzerrors.Forbidden{}):
				return nil, fmt.Errorf("determine shard: %w", err)
			default:
				return nil, objects.NewErrInvalidUserInput("determine shard: %v", err)
			}
		}

		group := byShard[shardName]
		group.ids = append(group.ids, id)
		group.pos = append(group.pos, pos)
		byShard[shardName] = group
	}

	out := make([]*storobj.Object, len(query))

	for shardName, group := range byShard {
		var objects []*storobj.Object
		var err error

		shard, release, err := i.GetShard(ctx, shardName)
		if err != nil {
			return nil, err
		} else if shard != nil {
			defer release()
			objects, err = shard.MultiObjectByID(ctx, group.ids)
			if err != nil {
				return nil, errors.Wrapf(err, "local shard %s", shardId(i.ID(), shardName))
			}
		} else {
			objects, err = i.remote.MultiGetObjects(ctx, shardName, extractIDsFromMulti(group.ids))
			if err != nil {
				return nil, errors.Wrapf(err, "remote shard %s", shardName)
			}
		}

		for i, obj := range objects {
			desiredPos := group.pos[i]
			out[desiredPos] = obj
		}
	}

	return out, nil
}

func extractIDsFromMulti(in []multi.Identifier) []strfmt.UUID {
	out := make([]strfmt.UUID, len(in))

	for i, id := range in {
		out[i] = strfmt.UUID(id.ID)
	}

	return out
}

func wrapIDsInMulti(in []strfmt.UUID) []multi.Identifier {
	out := make([]multi.Identifier, len(in))

	for i, id := range in {
		out[i] = multi.Identifier{ID: string(id)}
	}

	return out
}

func (i *Index) exists(ctx context.Context, id strfmt.UUID,
	replProps *additional.ReplicationProperties, tenant string,
) (bool, error) {
	if err := i.validateMultiTenancy(tenant); err != nil {
		return false, err
	}

	shardName, err := i.determineObjectShard(ctx, id, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return false, objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return false, fmt.Errorf("determine shard: %w", err)
		default:
			return false, objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	var exists bool
	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := types.ConsistencyLevel(replProps.ConsistencyLevel)
		return i.replicator.Exists(ctx, cl, shardName, id)
	}

	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return exists, err
	}

	if shard != nil {
		defer release()
		exists, err = shard.Exists(ctx, id)
		if err != nil {
			err = fmt.Errorf("exists locally: shard=%q: %w", shardName, err)
		}
	} else {
		exists, err = i.remote.Exists(ctx, shardName, id)
		if err != nil {
			owner, _ := i.getSchema.ShardOwner(i.Config.ClassName.String(), shardName)
			err = fmt.Errorf("exists remotely: shard=%q owner=%q: %w", shardName, owner, err)
		}
	}

	return exists, err
}

func (i *Index) IncomingExists(ctx context.Context, shardName string,
	id strfmt.UUID,
) (bool, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return false, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return false, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.Exists(ctx, id)
}

func (i *Index) objectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addlProps additional.Properties, replProps *additional.ReplicationProperties, tenant string, autoCut int,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	if err := i.validateMultiTenancy(tenant); err != nil {
		return nil, nil, err
	}

	shardNames, err := i.targetShardNames(ctx, tenant)
	if err != nil || len(shardNames) == 0 {
		return nil, nil, err
	}

	// If the request is a BM25F with no properties selected, use all possible properties
	if keywordRanking != nil && keywordRanking.Type == "bm25" && len(keywordRanking.Properties) == 0 {

		cl := i.getSchema.ReadOnlyClass(i.Config.ClassName.String())
		if cl == nil {
			return nil, nil, fmt.Errorf("class %s not found in schema", i.Config.ClassName)
		}

		propHash := cl.Properties
		// Get keys of hash
		for _, v := range propHash {
			if inverted.PropertyHasSearchableIndex(i.getSchema.ReadOnlyClass(i.Config.ClassName.String()), v.Name) {
				keywordRanking.Properties = append(keywordRanking.Properties, v.Name)
			}
		}

		// WEAVIATE-471 - error if we can't find a property to search
		if len(keywordRanking.Properties) == 0 {
			return nil, []float32{}, errors.New(
				"No properties provided, and no indexed properties found in class")
		}
	}

	outObjects, outScores, err := i.objectSearchByShard(ctx, limit,
		filters, keywordRanking, sort, cursor, addlProps, shardNames, properties)
	if err != nil {
		return nil, nil, err
	}

	if len(outObjects) == len(outScores) {
		if keywordRanking != nil && keywordRanking.Type == "bm25" {
			for ii := range outObjects {
				oo := outObjects[ii]

				if oo.AdditionalProperties() == nil {
					oo.Object.Additional = make(map[string]interface{})
				}

				// Additional score is filled in by the top level function

				// Collect all keys starting with "BM25F" and add them to the Additional
				if keywordRanking.AdditionalExplanations {
					explainScore := ""
					for k, v := range oo.Object.Additional {
						if strings.HasPrefix(k, "BM25F") {

							explainScore = fmt.Sprintf("%v, %v:%v", explainScore, k, v)
							delete(oo.Object.Additional, k)
						}
					}
					oo.Object.Additional["explainScore"] = explainScore
				}
			}
		}
	}

	if len(sort) > 0 {
		if len(shardNames) > 1 {
			var err error
			outObjects, outScores, err = i.sort(outObjects, outScores, sort, limit)
			if err != nil {
				return nil, nil, errors.Wrap(err, "sort")
			}
		}
	} else if keywordRanking != nil {
		outObjects, outScores = i.sortKeywordRanking(outObjects, outScores)
	} else if len(shardNames) > 1 && !addlProps.ReferenceQuery {
		// sort only for multiple shards (already sorted for single)
		// and for not reference nested query (sort is applied for root query)
		outObjects, outScores = i.sortByID(outObjects, outScores)
	}

	if autoCut > 0 {
		cutOff := autocut.Autocut(outScores, autoCut)
		outObjects = outObjects[:cutOff]
		outScores = outScores[:cutOff]
	}

	// if this search was caused by a reference property
	// search, we should not limit the number of results.
	// for example, if the query contains a where filter
	// whose operator is `And`, and one of the operands
	// contains a path to a reference prop, the Search
	// caused by such a ref prop being limited can cause
	// the `And` to return no results where results would
	// be expected. we won't know that unless we search
	// and return all referenced object properties.
	if !addlProps.ReferenceQuery && len(outObjects) > limit {
		if len(outObjects) == len(outScores) {
			outScores = outScores[:limit]
		}
		outObjects = outObjects[:limit]
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency(types.ConsistencyLevelOne)
		}
		l := types.ConsistencyLevel(replProps.ConsistencyLevel)
		err = i.replicator.CheckConsistency(ctx, l, outObjects)
		if err != nil {
			i.logger.WithField("action", "object_search").
				Errorf("failed to check consistency of search results: %v", err)
		}
	}

	return outObjects, outScores, nil
}

func (i *Index) objectSearchByShard(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addlProps additional.Properties, shards []string, properties []string,
) ([]*storobj.Object, []float32, error) {
	resultObjects, resultScores := objectSearchPreallocate(limit, shards)

	eg := enterrors.NewErrorGroupWrapper(i.logger, "filters:", filters)
	eg.SetLimit(_NUMCPU * 2)
	shardResultLock := sync.Mutex{}
	for _, shardName := range shards {
		shardName := shardName

		eg.Go(func() error {
			var (
				objs     []*storobj.Object
				scores   []float32
				nodeName string
				err      error
			)

			shard, release, err := i.GetShard(ctx, shardName)
			if err != nil {
				return err
			}

			if shard != nil {
				defer release()
				localCtx := helpers.InitSlowQueryDetails(ctx)
				helpers.AnnotateSlowQueryLog(localCtx, "is_coordinator", true)
				objs, scores, err = shard.ObjectSearch(localCtx, limit, filters, keywordRanking, sort, cursor, addlProps, properties)
				if err != nil {
					return fmt.Errorf(
						"local shard object search %s: %w", shard.ID(), err)
				}
				nodeName = i.getSchema.NodeName()
			} else {
				i.logger.WithField("shardName", shardName).Debug("shard was not found locally, search for object remotely")

				objs, scores, nodeName, err = i.remote.SearchShard(
					ctx, shardName, nil, nil, 0, limit, filters, keywordRanking,
					sort, cursor, nil, addlProps, i.replicationEnabled(), nil, properties)
				if err != nil {
					return fmt.Errorf(
						"remote shard object search %s: %w", shardName, err)
				}
			}

			if i.replicationEnabled() {
				storobj.AddOwnership(objs, nodeName, shardName)
			}

			shardResultLock.Lock()
			resultObjects = append(resultObjects, objs...)
			resultScores = append(resultScores, scores...)
			shardResultLock.Unlock()

			return nil
		}, shardName)
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	if len(resultObjects) == len(resultScores) {

		// Force a stable sort order by UUID

		type resultSortable struct {
			object *storobj.Object
			score  float32
		}
		objs := resultObjects
		scores := resultScores
		var results []resultSortable = make([]resultSortable, len(objs))
		for i := range objs {
			results[i] = resultSortable{
				object: objs[i],
				score:  scores[i],
			}
		}

		golangSort.Slice(results, func(i, j int) bool {
			if results[i].score == results[j].score {
				return results[i].object.Object.ID > results[j].object.Object.ID
			}

			return results[i].score > results[j].score
		})

		var finalObjs []*storobj.Object = make([]*storobj.Object, len(results))
		var finalScores []float32 = make([]float32, len(results))
		for i, result := range results {

			finalObjs[i] = result.object
			finalScores[i] = result.score
		}

		return finalObjs, finalScores, nil
	}

	return resultObjects, resultScores, nil
}

func (i *Index) sortByID(objects []*storobj.Object, scores []float32,
) ([]*storobj.Object, []float32) {
	return newIDSorter().sort(objects, scores)
}

func (i *Index) sortKeywordRanking(objects []*storobj.Object,
	scores []float32,
) ([]*storobj.Object, []float32) {
	return newScoresSorter().sort(objects, scores)
}

func (i *Index) sort(objects []*storobj.Object, scores []float32,
	sort []filters.Sort, limit int,
) ([]*storobj.Object, []float32, error) {
	return sorter.NewObjectsSorter(i.getSchema.ReadOnlyClass).
		Sort(objects, scores, limit, sort)
}

func (i *Index) mergeGroups(objects []*storobj.Object, dists []float32,
	groupBy *searchparams.GroupBy, limit, shardCount int,
) ([]*storobj.Object, []float32, error) {
	return newGroupMerger(objects, dists, groupBy).Do()
}

func (i *Index) singleLocalShardObjectVectorSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, dist float32, limit int, filters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties,
	shard ShardLike, targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, error) {
	ctx = helpers.InitSlowQueryDetails(ctx)
	helpers.AnnotateSlowQueryLog(ctx, "is_coordinator", true)
	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shard.Name()))
	}
	res, resDists, err := shard.ObjectVectorSearch(
		ctx, searchVectors, targetVectors, dist, limit, filters, sort, groupBy, additional, targetCombination, properties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}
	return res, resDists, nil
}

// to be called after validating multi-tenancy
func (i *Index) targetShardNames(ctx context.Context, tenant string) ([]string, error) {
	className := i.Config.ClassName.String()
	if !i.partitioningEnabled {
		return i.getSchema.CopyShardingState(className).AllPhysicalShards(), nil
	}

	if tenant == "" {
		return []string{}, objects.NewErrMultiTenancy(fmt.Errorf("tenant name is empty"))
	}

	tenantShards, err := i.getSchema.OptimisticTenantStatus(ctx, className, tenant)
	if err != nil {
		return nil, err
	}

	if tenantShards[tenant] != "" {
		if tenantShards[tenant] == models.TenantActivityStatusHOT {
			return []string{tenant}, nil
		}
		return []string{}, objects.NewErrMultiTenancy(fmt.Errorf("%w: '%s'", enterrors.ErrTenantNotActive, tenant))
	}
	return []string{}, objects.NewErrMultiTenancy(
		fmt.Errorf("%w: %q", enterrors.ErrTenantNotFound, tenant))
}

func (i *Index) localShardSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, dist float32, limit int, localFilters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additionalProps additional.Properties,
	targetCombination *dto.TargetCombination, properties []string, shardName string,
) ([]*storobj.Object, []float32, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, nil, err
	}
	if shard != nil {
		defer release()
	}

	localCtx := helpers.InitSlowQueryDetails(ctx)
	helpers.AnnotateSlowQueryLog(localCtx, "is_coordinator", true)
	localShardResult, localShardScores, err := shard.ObjectVectorSearch(
		localCtx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}
	// Append result to out
	if i.replicationEnabled() {
		storobj.AddOwnership(localShardResult, i.getSchema.NodeName(), shardName)
	}
	return localShardResult, localShardScores, nil
}

func (i *Index) remoteShardSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, distance float32, limit int, localFilters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties,
	targetCombination *dto.TargetCombination, properties []string, shardName string,
) ([]*storobj.Object, []float32, error) {
	var outObjects []*storobj.Object
	var outScores []float32

	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, nil, err
	}
	if shard != nil {
		defer release()
	}

	if i.Config.ForceFullReplicasSearch {
		// Force a search on all the replicas for the shard
		remoteSearchResults, err := i.remote.SearchAllReplicas(ctx,
			i.logger, shardName, searchVectors, targetVectors, distance, limit, localFilters,
			nil, sort, nil, groupBy, additional, i.replicationEnabled(), i.getSchema.NodeName(), targetCombination, properties)
		// Only return an error if we failed to query remote shards AND we had no local shard to query
		if err != nil && shard == nil {
			return nil, nil, errors.Wrapf(err, "remote shard %s", shardName)
		}
		// Append the result of the search to the outgoing result
		for _, remoteShardResult := range remoteSearchResults {
			if i.replicationEnabled() {
				storobj.AddOwnership(remoteShardResult.Objects, remoteShardResult.Node, shardName)
			}
			outObjects = append(outObjects, remoteShardResult.Objects...)
			outScores = append(outScores, remoteShardResult.Scores...)
		}
	} else {
		// Search only what is necessary
		remoteResult, remoteDists, nodeName, err := i.remote.SearchShard(ctx,
			shardName, searchVectors, targetVectors, distance, limit, localFilters,
			nil, sort, nil, groupBy, additional, i.replicationEnabled(), targetCombination, properties)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "remote shard %s", shardName)
		}

		if i.replicationEnabled() {
			storobj.AddOwnership(remoteResult, nodeName, shardName)
		}
		outObjects = remoteResult
		outScores = remoteDists
	}
	return outObjects, outScores, nil
}

func (i *Index) objectVectorSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, dist float32, limit int, localFilters *filters.LocalFilter, sort []filters.Sort,
	groupBy *searchparams.GroupBy, additionalProps additional.Properties,
	replProps *additional.ReplicationProperties, tenant string, targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, error) {
	if err := i.validateMultiTenancy(tenant); err != nil {
		return nil, nil, err
	}
	shardNames, err := i.targetShardNames(ctx, tenant)
	if err != nil || len(shardNames) == 0 {
		return nil, nil, err
	}

	if len(shardNames) == 1 && !i.Config.ForceFullReplicasSearch {
		shard, release, err := i.GetShard(ctx, shardNames[0])
		if err != nil {
			return nil, nil, err
		}

		if shard != nil {
			defer release()
			return i.singleLocalShardObjectVectorSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters,
				sort, groupBy, additionalProps, shard, targetCombination, properties)
		}
	}

	// a limit of -1 is used to signal a search by distance. if that is
	// the case we have to adjust how we calculate the output capacity
	var shardCap int
	if limit < 0 {
		shardCap = len(shardNames) * hnsw.DefaultSearchByDistInitialLimit
	} else {
		shardCap = len(shardNames) * limit
	}

	eg := enterrors.NewErrorGroupWrapper(i.logger, "tenant:", tenant)
	eg.SetLimit(_NUMCPU * 2)
	m := &sync.Mutex{}

	out := make([]*storobj.Object, 0, shardCap)
	dists := make([]float32, 0, shardCap)
	var localSearches int64
	var localResponses atomic.Int64
	var remoteSearches int64
	var remoteResponses atomic.Int64

	for _, sn := range shardNames {
		shardName := sn
		shard, release, err := i.GetShard(ctx, shardName)
		if err != nil {
			return nil, nil, err
		}
		if shard != nil {
			defer release()
		}

		if shard != nil {
			localSearches++
			eg.Go(func() error {
				localShardResult, localShardScores, err1 := i.localShardSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties, shardName)
				if err1 != nil {
					return fmt.Errorf(
						"local shard object search %s: %w", shard.ID(), err1)
				}

				m.Lock()
				localResponses.Add(1)
				out = append(out, localShardResult...)
				dists = append(dists, localShardScores...)
				m.Unlock()
				return nil
			})
		}

		if shard == nil || i.Config.ForceFullReplicasSearch {
			remoteSearches++
			eg.Go(func() error {
				// If we have no local shard or if we force the query to reach all replicas
				remoteShardObject, remoteShardScores, err2 := i.remoteShardSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties, shardName)
				if err2 != nil {
					return fmt.Errorf(
						"remote shard object search %s: %w", shardName, err2)
				}
				m.Lock()
				remoteResponses.Add(1)
				out = append(out, remoteShardObject...)
				dists = append(dists, remoteShardScores...)
				m.Unlock()
				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	// If we are force querying all replicas, we need to run deduplication on the result.
	if i.Config.ForceFullReplicasSearch {
		if localSearches != localResponses.Load() {
			i.logger.Warnf("(in full replica search) local search count does not match local response count: searches=%d responses=%d", localSearches, localResponses.Load())
		}
		if remoteSearches != remoteResponses.Load() {
			i.logger.Warnf("(in full replica search) remote search count does not match remote response count: searches=%d responses=%d", remoteSearches, remoteResponses.Load())
		}
		out, dists, err = searchResultDedup(out, dists)
		if err != nil {
			return nil, nil, fmt.Errorf("could not deduplicate result after full replicas search: %w", err)
		}
	}

	if len(shardNames) == 1 {
		return out, dists, nil
	}

	if len(shardNames) > 1 && groupBy != nil {
		return i.mergeGroups(out, dists, groupBy, limit, len(shardNames))
	}

	if len(shardNames) > 1 && len(sort) > 0 {
		return i.sort(out, dists, sort, limit)
	}

	out, dists = newDistancesSorter().sort(out, dists)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		dists = dists[:limit]
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency(types.ConsistencyLevelOne)
		}
		l := types.ConsistencyLevel(replProps.ConsistencyLevel)
		err = i.replicator.CheckConsistency(ctx, l, out)
		if err != nil {
			i.logger.WithField("action", "object_vector_search").
				Errorf("failed to check consistency of search results: %v", err)
		}
	}

	return out, dists, nil
}

func (i *Index) IncomingSearch(ctx context.Context, shardName string,
	searchVectors []models.Vector, targetVectors []string, distance float32, limit int,
	filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort, cursor *filters.Cursor, groupBy *searchparams.GroupBy,
	additional additional.Properties, targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, nil, err
	}
	defer release()

	ctx = helpers.InitSlowQueryDetails(ctx)
	helpers.AnnotateSlowQueryLog(ctx, "is_coordinator", false)

	// Hacky fix here
	// shard.GetStatus() will force a lazy shard to load and we have usecases that rely on that behaviour that a search
	// will force a lazy loaded shard to load
	// However we also have cases (related to FORCE_FULL_REPLICAS_SEARCH) where we want to avoid waiting for a shard to
	// load, therefore we only call GetStatusNoLoad if replication is enabled -> another replica will be able to answer
	// the request and we want to exit early
	if i.replicationEnabled() && shard.GetStatusNoLoad() == storagestate.StatusLoading {
		return nil, nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	} else {
		if shard.GetStatus() == storagestate.StatusLoading {
			// This effectively never happens with lazy loaded shard as GetStatus will wait for the lazy shard to load
			// and then status will never be "StatusLoading"
			return nil, nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
		}
	}

	if len(searchVectors) == 0 {
		res, scores, err := shard.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, additional, properties)
		if err != nil {
			return nil, nil, err
		}

		return res, scores, nil
	}

	res, resDists, err := shard.ObjectVectorSearch(
		ctx, searchVectors, targetVectors, distance, limit, filters, sort, groupBy, additional, targetCombination, properties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "shard %s", shard.ID())
	}

	return res, resDists, nil
}

func (i *Index) deleteObject(ctx context.Context, id strfmt.UUID,
	deletionTime time.Time, replProps *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	if err := i.validateMultiTenancy(tenant); err != nil {
		return err
	}

	shardName, err := i.determineObjectShard(ctx, id, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return fmt.Errorf("determine shard: %w", err)
		default:
			return objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := types.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.DeleteObject(ctx, shardName, id, deletionTime, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate deletion: shard=%q %w", shardName, err)
		}
		return nil
	}

	// no replication, remote shard (or local not yet inited)
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}

	if shard == nil {
		if err := i.remote.DeleteObject(ctx, shardName, id, deletionTime, schemaVersion); err != nil {
			return fmt.Errorf("delete remote object: shard=%q: %w", shardName, err)
		}
		return nil
	}
	defer release()

	// no replication, local shard
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()
	if err = shard.DeleteObject(ctx, id, deletionTime); err != nil {
		return fmt.Errorf("delete local object: shard=%q: %w", shardName, err)
	}
	return nil
}

func (i *Index) IncomingDeleteObject(ctx context.Context, shardName string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.DeleteObject(ctx, id, deletionTime)
}

func (i *Index) getClass() *models.Class {
	className := i.Config.ClassName.String()
	return i.getSchema.ReadOnlyClass(className)
}

// Intended to run on "receiver" nodes, where local shard
// is expected to exist and be active
// Method first tries to get shard from Index::shards map,
// or inits shard and adds it to the map if shard was not found
func (i *Index) initLocalShard(ctx context.Context, shardName string) error {
	return i.initLocalShardWithForcedLoading(ctx, i.getClass(), shardName, false)
}

func (i *Index) LoadLocalShard(ctx context.Context, shardName string) error {
	return i.initLocalShardWithForcedLoading(ctx, i.getClass(), shardName, true)
}

func (i *Index) initLocalShardWithForcedLoading(ctx context.Context, class *models.Class, shardName string, mustLoad bool) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	// make sure same shard is not inited in parallel
	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	// check if created in the meantime by concurrent call
	if shard := i.shards.Load(shardName); shard != nil {
		if mustLoad {
			lazyShard, ok := shard.(*LazyLoadShard)
			if ok {
				return lazyShard.Load(ctx)
			}
		}

		return nil
	}

	disableLazyLoad := mustLoad || i.Config.DisableLazyLoadShards

	shard, err := i.initShard(ctx, shardName, class, i.metrics.baseMetrics, disableLazyLoad)
	if err != nil {
		return err
	}

	i.shards.Store(shardName, shard)

	return nil
}

func (i *Index) GetShard(ctx context.Context, shardName string) (
	shard ShardLike, release func(), err error,
) {
	return i.getOptInitLocalShard(ctx, shardName, false)
}

func (i *Index) getOrInitShard(ctx context.Context, shardName string) (
	shard ShardLike, release func(), err error,
) {
	return i.getOptInitLocalShard(ctx, shardName, true)
}

// getOptInitLocalShard returns the local shard with the given name.
// It is ensured that the returned instance is a fully loaded shard if ensureInit is set to true.
// The returned shard may be a lazy shard instance or nil if the shard hasn't yet been initialized.
// The returned shard cannot be closed until release is called.
func (i *Index) getOptInitLocalShard(ctx context.Context, shardName string, ensureInit bool) (
	shard ShardLike, release func(), err error,
) {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return nil, func() {}, errAlreadyShutdown
	}

	// make sure same shard is not inited in parallel
	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	// check if created in the meantime by concurrent call
	shard = i.shards.Load(shardName)
	if shard == nil {
		if !ensureInit {
			return nil, func() {}, nil
		}

		className := i.Config.ClassName.String()
		class := i.getSchema.ReadOnlyClass(className)
		if class == nil {
			return nil, func() {}, fmt.Errorf("class %s not found in schema", className)
		}

		shard, err = i.initShard(ctx, shardName, class, i.metrics.baseMetrics, true)
		if err != nil {
			return nil, func() {}, err
		}

		i.shards.Store(shardName, shard)
	}

	release, err = shard.preventShutdown()
	if err != nil {
		return nil, func() {}, fmt.Errorf("get/init local shard %q, no shutdown: %w", shardName, err)
	}

	return shard, release, nil
}

func (i *Index) mergeObject(ctx context.Context, merge objects.MergeDocument,
	replProps *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	if err := i.validateMultiTenancy(tenant); err != nil {
		return err
	}

	shardName, err := i.determineObjectShard(ctx, merge.ID, tenant)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return objects.NewErrMultiTenancy(fmt.Errorf("determine shard: %w", err))
		case errors.As(err, &authzerrors.Forbidden{}):
			return fmt.Errorf("determine shard: %w", err)
		default:
			return objects.NewErrInvalidUserInput("determine shard: %v", err)
		}
	}

	if i.replicationEnabled() {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := types.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.MergeObject(ctx, shardName, &merge, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate single update: %w", err)
		}
		return nil
	}

	// no replication, remote shard (or local not yet inited)
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}

	if shard == nil {
		if err := i.remote.MergeObject(ctx, shardName, merge, schemaVersion); err != nil {
			return fmt.Errorf("update remote object: shard=%q: %w", shardName, err)
		}
		return nil
	}
	defer release()

	// no replication, local shard
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()
	if err = shard.MergeObject(ctx, merge); err != nil {
		return fmt.Errorf("update local object: shard=%q: %w", shardName, err)
	}

	return nil
}

func (i *Index) IncomingMergeObject(ctx context.Context, shardName string,
	mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.MergeObject(ctx, mergeDoc)
}

func (i *Index) aggregate(ctx context.Context,
	params aggregation.Params, modules *modules.Provider,
) (*aggregation.Result, error) {
	if err := i.validateMultiTenancy(params.Tenant); err != nil {
		return nil, err
	}

	shardNames, err := i.targetShardNames(ctx, params.Tenant)
	if err != nil || len(shardNames) == 0 {
		return nil, err
	}

	results := make([]*aggregation.Result, len(shardNames))
	for j, shardName := range shardNames {
		var err error
		var res *aggregation.Result

		var shard ShardLike
		var release func()
		shard, release, err = i.GetShard(ctx, shardName)
		if err == nil {
			if shard != nil {
				func() {
					defer release()
					res, err = shard.Aggregate(ctx, params, modules)
				}()
			} else {
				res, err = i.remote.Aggregate(ctx, shardName, params)
			}
		}

		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		results[j] = res
	}

	return aggregator.NewShardCombiner().Do(results), nil
}

func (i *Index) IncomingAggregate(ctx context.Context, shardName string,
	params aggregation.Params, mods interface{},
) (*aggregation.Result, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.Aggregate(ctx, params, mods.(*modules.Provider))
}

func (i *Index) drop() error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	i.closeLock.Lock()
	defer i.closeLock.Unlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.closed = true

	i.closingCancel()

	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)
	fields := logrus.Fields{"action": "drop_shard", "class": i.Config.ClassName}
	dropShard := func(name string, _ ShardLike) error {
		eg.Go(func() error {
			i.shardCreateLocks.Lock(name)
			defer i.shardCreateLocks.Unlock(name)

			shard, ok := i.shards.LoadAndDelete(name)
			if !ok {
				return nil // shard already does not exist
			}
			if err := shard.drop(); err != nil {
				logrus.WithFields(fields).WithField("id", shard.ID()).Error(err)
			}

			return nil
		})
		return nil
	}

	i.shards.Range(dropShard)
	if err := eg.Wait(); err != nil {
		return err
	}

	// Dropping the shards only unregisters the shards callbacks, but we still
	// need to stop the cycle managers that those shards used to register with.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	i.logger.WithFields(logrus.Fields{
		"action":   "drop_index",
		"duration": 60 * time.Second,
	}).Debug("context.WithTimeout")

	if err := i.stopCycleManagers(ctx, "drop"); err != nil {
		return err
	}

	return os.RemoveAll(i.path())
}

func (i *Index) dropShards(names []string) error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	ec := errorcompounder.New()
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range names {
		name := name
		eg.Go(func() error {
			i.shardCreateLocks.Lock(name)
			defer i.shardCreateLocks.Unlock(name)

			shard, ok := i.shards.LoadAndDelete(name)
			if !ok {
				return nil // shard already does not exist (or inactive)
			}

			if err := shard.drop(); err != nil {
				ec.Add(err)
				i.logger.WithField("action", "drop_shard").
					WithField("shard", shard.ID()).Error(err)
			}

			return nil
		})
	}

	eg.Wait()
	return ec.ToError()
}

func (i *Index) dropCloudShards(ctx context.Context, cloud modulecapabilities.OffloadCloud, names []string, nodeId string) error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	ec := &errorcompounder.ErrorCompounder{}
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range names {
		name := name
		eg.Go(func() error {
			i.shardCreateLocks.Lock(name)
			defer i.shardCreateLocks.Unlock(name)

			if err := cloud.Delete(ctx, i.ID(), name, nodeId); err != nil {
				ec.Add(err)
				i.logger.WithField("action", "cloud_drop_shard").
					WithField("shard", name).Error(err)
			}
			return nil
		})
	}

	eg.Wait()
	return ec.ToError()
}

func (i *Index) Shutdown(ctx context.Context) error {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	i.closeLock.Lock()
	defer i.closeLock.Unlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.closed = true

	i.closingCancel()

	// TODO allow every resource cleanup to run, before returning early with error
	if err := i.shards.RangeConcurrently(i.logger, func(name string, shard ShardLike) error {
		if err := shard.Shutdown(ctx); err != nil {
			if !errors.Is(err, errAlreadyShutdown) {
				return errors.Wrapf(err, "shutdown shard %q", name)
			}
			i.logger.WithField("shard", shard.Name()).Debug("was already shut or dropped")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := i.stopCycleManagers(ctx, "shutdown"); err != nil {
		return err
	}

	return nil
}

func (i *Index) stopCycleManagers(ctx context.Context, usecase string) error {
	if err := i.cycleCallbacks.compactionCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop objects compaction cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.compactionAuxCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop non objects compaction cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.flushCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop flush cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.vectorCommitLoggerCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop vector commit logger cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.vectorTombstoneCleanupCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop vector tombstone cleanup cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.geoPropsCommitLoggerCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop geo props commit logger cycle: %w", usecase, err)
	}
	if err := i.cycleCallbacks.geoPropsTombstoneCleanupCycle.StopAndWait(ctx); err != nil {
		return fmt.Errorf("%s: stop geo props tombstone cleanup cycle: %w", usecase, err)
	}
	return nil
}

func (i *Index) shardState() *sharding.State {
	return i.getSchema.CopyShardingState(i.Config.ClassName.String())
}

func (i *Index) getShardsQueueSize(ctx context.Context, tenant string) (map[string]int64, error) {
	shardsQueueSize := make(map[string]int64)

	// TODO-RAFT should be strongly consistent?
	shardNames := i.shardState().AllPhysicalShards()

	for _, shardName := range shardNames {
		if tenant != "" && shardName != tenant {
			continue
		}
		var err error
		var size int64
		var shard ShardLike
		var release func()

		shard, release, err = i.GetShard(ctx, shardName)
		if err == nil {
			if shard != nil {
				func() {
					defer release()
					_ = shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
						size += queue.Size()
						return nil
					})
				}()
			} else {
				size, err = i.remote.GetShardQueueSize(ctx, shardName)
			}
		}

		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		shardsQueueSize[shardName] = size
	}

	return shardsQueueSize, nil
}

func (i *Index) IncomingGetShardQueueSize(ctx context.Context, shardName string) (int64, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return 0, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return 0, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}
	var size int64
	_ = shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
		size += queue.Size()
		return nil
	})
	return size, nil
}

func (i *Index) getShardsStatus(ctx context.Context, tenant string) (map[string]string, error) {
	shardsStatus := make(map[string]string)

	// TODO-RAFT should be strongly consistent?
	shardState := i.getSchema.CopyShardingState(i.Config.ClassName.String())
	if shardState == nil {
		return nil, fmt.Errorf("class %s not found in schema", i.Config.ClassName)
	}

	shardNames := shardState.AllPhysicalShards()

	for _, shardName := range shardNames {
		if tenant != "" && shardName != tenant {
			continue
		}
		var err error
		var status string
		var shard ShardLike
		var release func()

		shard, release, err = i.GetShard(ctx, shardName)
		if err == nil {
			if shard != nil {
				func() {
					defer release()
					status = shard.GetStatus().String()
				}()
			} else {
				status, err = i.remote.GetShardStatus(ctx, shardName)
			}
		}

		if err != nil {
			return nil, errors.Wrapf(err, "shard %s", shardName)
		}

		shardsStatus[shardName] = status
	}

	return shardsStatus, nil
}

func (i *Index) IncomingGetShardStatus(ctx context.Context, shardName string) (string, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return "", err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return "", enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}
	return shard.GetStatus().String(), nil
}

func (i *Index) updateShardStatus(ctx context.Context, shardName, targetStatus string, schemaVersion uint64) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	if shard == nil {
		return i.remote.UpdateShardStatus(ctx, shardName, targetStatus, schemaVersion)
	}
	defer release()
	return shard.UpdateStatus(targetStatus)
}

func (i *Index) IncomingUpdateShardStatus(ctx context.Context, shardName, targetStatus string, schemaVersion uint64) error {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.UpdateStatus(targetStatus)
}

func (i *Index) findUUIDs(ctx context.Context,
	filters *filters.LocalFilter, tenant string, repl *additional.ReplicationProperties,
) (map[string][]strfmt.UUID, error) {
	before := time.Now()
	defer i.metrics.BatchDelete(before, "filter_total")

	if err := i.validateMultiTenancy(tenant); err != nil {
		return nil, err
	}

	className := i.Config.ClassName.String()

	shardNames, err := i.targetShardNames(ctx, tenant)
	if err != nil {
		return nil, err
	}

	results := make(map[string][]strfmt.UUID)
	for _, shardName := range shardNames {
		var shard ShardLike
		var release func()
		var err error

		if i.replicationEnabled() {
			if repl == nil {
				repl = defaultConsistency()
			}

			results[shardName], err = i.replicator.FindUUIDs(ctx, className, shardName, filters, types.ConsistencyLevel(repl.ConsistencyLevel))
		} else {
			shard, release, err = i.GetShard(ctx, shardName)
			if err == nil {
				if shard != nil {
					func() {
						defer release()
						results[shardName], err = shard.FindUUIDs(ctx, filters)
					}()
				} else {
					results[shardName], err = i.remote.FindUUIDs(ctx, shardName, filters)
				}
			}
		}

		if err != nil {
			return nil, fmt.Errorf("find matching doc ids in shard %q: %w", shardName, err)
		}
	}

	return results, nil
}

func (i *Index) IncomingFindUUIDs(ctx context.Context, shardName string,
	filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return nil, err
	}
	defer release()

	if shard.GetStatus() == storagestate.StatusLoading {
		return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", shardName))
	}

	return shard.FindUUIDs(ctx, filters)
}

func (i *Index) batchDeleteObjects(ctx context.Context, shardUUIDs map[string][]strfmt.UUID,
	deletionTime time.Time, dryRun bool, replProps *additional.ReplicationProperties, schemaVersion uint64,
) (objects.BatchSimpleObjects, error) {
	before := time.Now()
	defer i.metrics.BatchDelete(before, "delete_from_shards_total")

	type result struct {
		objs objects.BatchSimpleObjects
	}

	if i.replicationEnabled() && replProps == nil {
		replProps = defaultConsistency()
	}

	wg := &sync.WaitGroup{}
	ch := make(chan result, len(shardUUIDs))
	for shardName, uuids := range shardUUIDs {
		uuids := uuids
		shardName := shardName
		wg.Add(1)
		f := func() {
			defer wg.Done()

			var objs objects.BatchSimpleObjects
			if i.replicationEnabled() {
				objs = i.replicator.DeleteObjects(ctx, shardName, uuids, deletionTime,
					dryRun, types.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
			} else {
				shard, release, err := i.GetShard(ctx, shardName)
				if err != nil {
					objs = objects.BatchSimpleObjects{
						objects.BatchSimpleObject{Err: err},
					}
				} else if shard != nil {
					i.shardTransferMutex.RLockGuard(func() error {
						defer release()
						objs = shard.DeleteObjectBatch(ctx, uuids, deletionTime, dryRun)
						return nil
					})
				} else {
					objs = i.remote.DeleteObjectBatch(ctx, shardName, uuids, deletionTime, dryRun, schemaVersion)
				}
			}

			ch <- result{objs}
		}
		enterrors.GoWrapper(f, i.logger)
	}

	wg.Wait()
	close(ch)

	var out objects.BatchSimpleObjects
	for res := range ch {
		out = append(out, res.objs...)
	}

	return out, nil
}

func (i *Index) IncomingDeleteObjectBatch(ctx context.Context, shardName string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) objects.BatchSimpleObjects {
	i.shardTransferMutex.RLock()
	defer i.shardTransferMutex.RUnlock()

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: err},
		}
	}
	defer release()

	return shard.DeleteObjectBatch(ctx, uuids, deletionTime, dryRun)
}

func defaultConsistency(l ...types.ConsistencyLevel) *additional.ReplicationProperties {
	rp := &additional.ReplicationProperties{}
	if len(l) != 0 {
		rp.ConsistencyLevel = string(l[0])
	} else {
		rp.ConsistencyLevel = string(types.ConsistencyLevelQuorum)
	}
	return rp
}

func objectSearchPreallocate(limit int, shards []string) ([]*storobj.Object, []float32) {
	perShardLimit := config.DefaultQueryMaximumResults
	if perShardLimit > int64(limit) {
		perShardLimit = int64(limit)
	}
	capacity := perShardLimit * int64(len(shards))
	objects := make([]*storobj.Object, 0, capacity)
	scores := make([]float32, 0, capacity)

	return objects, scores
}

func (i *Index) validateMultiTenancy(tenant string) error {
	if i.partitioningEnabled && tenant == "" {
		return objects.NewErrMultiTenancy(
			fmt.Errorf("class %s has multi-tenancy enabled, but request was without tenant", i.Config.ClassName),
		)
	} else if !i.partitioningEnabled && tenant != "" {
		return objects.NewErrMultiTenancy(
			fmt.Errorf("class %s has multi-tenancy disabled, but request was with tenant", i.Config.ClassName),
		)
	}
	return nil
}

// GetVectorIndexConfig returns a vector index configuration associated with targetVector.
// In case targetVector is empty string, legacy vector configuration is returned.
// Method expects that configuration associated with targetVector is present.
func (i *Index) GetVectorIndexConfig(targetVector string) schemaConfig.VectorIndexConfig {
	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	if targetVector == "" {
		return i.vectorIndexUserConfig
	}

	return i.vectorIndexUserConfigs[targetVector]
}

// GetVectorIndexConfigs returns a map of vector index configurations.
// If present, legacy vector is return under the key of empty string.
func (i *Index) GetVectorIndexConfigs() map[string]schemaConfig.VectorIndexConfig {
	i.vectorIndexUserConfigLock.Lock()
	defer i.vectorIndexUserConfigLock.Unlock()

	configs := make(map[string]schemaConfig.VectorIndexConfig, len(i.vectorIndexUserConfigs)+1)
	for k, v := range i.vectorIndexUserConfigs {
		configs[k] = v
	}

	if i.vectorIndexUserConfig != nil {
		configs[""] = i.vectorIndexUserConfig
	}

	return configs
}

func convertToVectorIndexConfig(config interface{}) schemaConfig.VectorIndexConfig {
	if config == nil {
		return nil
	}
	// in case legacy vector config was set as an empty map/object instead of nil
	if empty, ok := config.(map[string]interface{}); ok && len(empty) == 0 {
		return nil
	}
	return config.(schemaConfig.VectorIndexConfig)
}

func convertToVectorIndexConfigs(configs map[string]models.VectorConfig) map[string]schemaConfig.VectorIndexConfig {
	if len(configs) > 0 {
		vectorIndexConfigs := make(map[string]schemaConfig.VectorIndexConfig)
		for targetVector, vectorConfig := range configs {
			if vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
				vectorIndexConfigs[targetVector] = vectorIndexConfig
			}
		}
		return vectorIndexConfigs
	}
	return nil
}

// IMPORTANT:
// DebugResetVectorIndex is intended to be used for debugging purposes only.
// It drops the selected vector index, creates a new one, then reindexes it in the background.
// This function assumes the node is not receiving any traffic besides the
// debug endpoints and that async indexing is enabled.
func (i *Index) DebugResetVectorIndex(ctx context.Context, shardName, targetVector string) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	if shard == nil {
		return errors.New("shard not found")
	}
	defer release()

	// Get the vector index
	vidx, ok := shard.GetVectorIndex(targetVector)
	if !ok {
		return errors.New("vector index not found")
	}

	if !hnsw.IsHNSWIndex(vidx) {
		return errors.New("vector index is not hnsw")
	}

	// Reset the vector index
	err = shard.DebugResetVectorIndex(ctx, targetVector)
	if err != nil {
		return errors.Wrap(err, "failed to reset vector index")
	}

	// Reindex in the background
	enterrors.GoWrapper(func() {
		err = shard.FillQueue(targetVector, 0)
		if err != nil {
			i.logger.WithField("shard", shardName).WithError(err).Error("failed to reindex vector index")
			return
		}
	}, i.logger)

	return nil
}

func (i *Index) DebugRepairIndex(ctx context.Context, shardName, targetVector string) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	if shard == nil {
		return errors.New("shard not found")
	}
	defer release()

	// Repair in the background
	enterrors.GoWrapper(func() {
		err := shard.RepairIndex(context.Background(), targetVector)
		if err != nil {
			i.logger.WithField("shard", shardName).WithError(err).Error("failed to repair vector index")
			return
		}
	}, i.logger)

	return nil
}
