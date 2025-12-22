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
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	golangSort "sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/usecases/multitenancy"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/cluster/router/executor"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/autocut"
	"github.com/weaviate/weaviate/entities/backup"
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
	"github.com/weaviate/weaviate/entities/tokenizer"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Use runtime.GOMAXPROCS instead of runtime.NumCPU because NumCPU returns
// the physical CPU cores. However, in a containerization context, that might
// not be what we want. The physical node could have 128 cores, but we could
// be cgroup-limited to 2 cores. In that case, we want 2 to be our limit, not
// 128. It isn't guaranteed that MAXPROCS reflects the cgroup limit, but at
// least there is a chance that it was set correctly. If not, it defaults to
// NumCPU anyway, so we're not any worse off.
var _NUMCPU = runtime.GOMAXPROCS(0)

// shardMap is a sync.Map which specialized in storing shards
type shardMap sync.Map

// Range calls f sequentially for each key and value present in the map.
// If f returns an error, range stops the iteration
func (m *shardMap) Range(f func(name string, shard ShardLike) error) (err error) {
	(*sync.Map)(m).Range(func(key, value any) bool {
		// Safe type assertion for key
		name, ok := key.(string)
		if !ok {
			// Skip invalid keys
			return true
		}

		// Safe type assertion for value
		shard, ok := value.(ShardLike)
		if !ok || shard == nil {
			// Skip invalid or nil shards
			return true
		}

		err = f(name, shard)
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
		name, ok := key.(string)
		if !ok {
			// Skip invalid keys
			return true
		}

		// Safe type assertion for value
		shard, ok := value.(ShardLike)
		if !ok || shard == nil {
			// Skip invalid or nil shards
			return true
		}

		eg.Go(func() error {
			return f(name, shard)
		}, name, shard)
		return true
	})

	return eg.Wait()
}

// Load returns the shard or nil if no shard is present.
// NOTE: this method does not check if the shard is loaded or not and it could
// return a lazy shard that is not loaded which could result in loading it if
// the returned shard is used.
// Use Loaded if you want to check if the shard is loaded without loading it.
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

// Loaded returns the shard or nil if no shard is present.
// If it's a lazy shard, only return it if it's loaded.
func (m *shardMap) Loaded(name string) ShardLike {
	v, ok := (*sync.Map)(m).Load(name)
	if !ok {
		return nil
	}

	shard, ok := v.(ShardLike)
	if !ok {
		return nil
	}

	// If it's a lazy shard, only return it if it's loaded
	if lazyShard, ok := shard.(*LazyLoadShard); ok {
		if !lazyShard.isLoaded() {
			return nil
		}
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

	getSchema    schemaUC.SchemaGetter
	schemaReader schemaUC.SchemaReader
	logger       logrus.FieldLogger
	remote       *sharding.RemoteIndex
	stopwords    *stopwords.Detector
	replicator   *replica.Replicator

	vectorIndexUserConfigLock sync.Mutex
	vectorIndexUserConfig     schemaConfig.VectorIndexConfig
	vectorIndexUserConfigs    map[string]schemaConfig.VectorIndexConfig
	SPFreshEnabled            bool

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

	// The other locks in the index should always be called in the given order to prevent deadlocks:
	// 1. closeLock
	// 2. backupLock (for a specific shard)
	// 3. shardCreateLocks (for a specific shard)
	closeLock  sync.RWMutex       // protects against closing while doing operations
	backupLock *esync.KeyRWLocker // prevents writes while a backup is running
	// prevents concurrent shard status changes. Use .Rlock to secure against status changes and .Lock to change status
	// Minimize holding the RW lock as it will block other operations on the same shard such as searches or writes.
	shardCreateLocks *esync.KeyRWLocker

	metrics          *Metrics
	centralJobQueue  chan job
	scheduler        *queue.Scheduler
	indexCheckpoints *indexcheckpoint.Checkpoints

	cycleCallbacks *indexCycleCallbacks

	lastBackup atomic.Pointer[BackupState]

	// canceled when either Shutdown or Drop called
	closingCtx    context.Context
	closingCancel context.CancelFunc

	// always true if lazy shard loading is off, in the case of lazy shard
	// loading will be set to true once the last shard was loaded.
	allShardsReady atomic.Bool
	allocChecker   memwatch.AllocChecker

	replicationConfigLock     sync.RWMutex
	asyncReplicationSemaphore *semaphore.Weighted

	shardLoadLimiter ShardLoadLimiter

	closed bool

	shardReindexer ShardReindexerV3

	router        routerTypes.Router
	shardResolver *resolver.ShardResolver
	bitmapBufPool roaringset.BitmapBufPool
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
func NewIndex(
	ctx context.Context,
	cfg IndexConfig,
	invertedIndexConfig schema.InvertedIndexConfig,
	vectorIndexUserConfig schemaConfig.VectorIndexConfig,
	vectorIndexUserConfigs map[string]schemaConfig.VectorIndexConfig,
	router routerTypes.Router,
	shardResolver *resolver.ShardResolver,
	sg schemaUC.SchemaGetter,
	schemaReader schemaUC.SchemaReader,
	cs inverted.ClassSearcher,
	logger logrus.FieldLogger,
	nodeResolver nodeResolver,
	remoteClient sharding.RemoteIndexClient,
	replicaClient replica.Client,
	globalReplicationConfig *replication.GlobalConfig,
	promMetrics *monitoring.PrometheusMetrics,
	class *models.Class,
	jobQueueCh chan job,
	scheduler *queue.Scheduler,
	indexCheckpoints *indexcheckpoint.Checkpoints,
	allocChecker memwatch.AllocChecker,
	shardReindexer ShardReindexerV3,
	bitmapBufPool roaringset.BitmapBufPool,
) (*Index, error) {
	err := tokenizer.AddCustomDict(class.Class, invertedIndexConfig.TokenizerUserDict)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new index")
	}

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

	metrics, err := NewMetrics(logger, promMetrics, cfg.ClassName.String(), "n/a")
	if err != nil {
		return nil, fmt.Errorf("create metrics for index %q: %w", cfg.ClassName.String(), err)
	}

	index := &Index{
		Config:                  cfg,
		globalreplicationConfig: globalReplicationConfig,
		getSchema:               sg,
		schemaReader:            schemaReader,
		logger:                  logger,
		classSearcher:           cs,
		vectorIndexUserConfig:   vectorIndexUserConfig,
		invertedIndexConfig:     invertedIndexConfig,
		vectorIndexUserConfigs:  vectorIndexUserConfigs,
		stopwords:               sd,
		partitioningEnabled:     multitenancy.IsMultiTenant(class.MultiTenancyConfig),
		remote:                  sharding.NewRemoteIndex(cfg.ClassName.String(), sg, nodeResolver, remoteClient),
		metrics:                 metrics,
		centralJobQueue:         jobQueueCh,
		backupLock:              esync.NewKeyRWLocker(),
		scheduler:               scheduler,
		indexCheckpoints:        indexCheckpoints,
		allocChecker:            allocChecker,
		shardCreateLocks:        esync.NewKeyRWLocker(),
		shardLoadLimiter:        cfg.ShardLoadLimiter,
		shardReindexer:          shardReindexer,
		router:                  router,
		shardResolver:           shardResolver,
		bitmapBufPool:           bitmapBufPool,
		SPFreshEnabled:          cfg.SPFreshEnabled,
	}

	getDeletionStrategy := func() string {
		return index.DeletionStrategy()
	}

	// TODO: Fix replica router instantiation to be at the top level
	index.replicator, err = replica.NewReplicator(cfg.ClassName.String(), router, sg.NodeName(), getDeletionStrategy, replicaClient, promMetrics, logger)
	if err != nil {
		return nil, fmt.Errorf("create replicator for index %q: %w", index.ID(), err)
	}

	maxAsyncReplicationWorkers, err := resolveAsyncReplicationMaxWorkers(
		&cfg,
		defaultAsyncReplicationMaxWorkers,
	)
	if err != nil {
		return nil, err
	}

	index.asyncReplicationSemaphore = semaphore.NewWeighted(int64(maxAsyncReplicationWorkers))

	index.closingCtx, index.closingCancel = context.WithCancel(context.Background())

	index.initCycleCallbacks()

	if err := index.checkSingleShardMigration(); err != nil {
		return nil, errors.Wrap(err, "migrating sharding state from previous version")
	}

	if err := os.MkdirAll(index.path(), os.ModePerm); err != nil {
		return nil, fmt.Errorf("init index %q: %w", index.ID(), err)
	}

	if err := index.initAndStoreShards(ctx, class, promMetrics); err != nil {
		return nil, err
	}

	index.cycleCallbacks.compactionCycle.Start()
	index.cycleCallbacks.compactionAuxCycle.Start()
	index.cycleCallbacks.flushCycle.Start()

	return index, nil
}

func resolveAsyncReplicationMaxWorkers(
	cfg *IndexConfig,
	defaultWorkers int,
) (int, error) {
	maxWorkers := defaultWorkers

	if cfg.AsyncReplicationConfig != nil && cfg.AsyncReplicationConfig.MaxWorkers != nil {
		maxWorkers = int(*cfg.AsyncReplicationConfig.MaxWorkers)
	}

	maxWorkers, err := optParseInt(
		os.Getenv("ASYNC_REPLICATION_MAX_WORKERS"),
		maxWorkers,
		1,
		math.MaxInt,
	)
	if err != nil {
		return 0, fmt.Errorf("async replication max workers: %w", err)
	}

	return maxWorkers, nil
}

// since called in Index's constructor there is no risk same shard will be inited/created in parallel,
// therefore shardCreateLocks are not used here
func (i *Index) initAndStoreShards(ctx context.Context, class *models.Class,
	promMetrics *monitoring.PrometheusMetrics,
) error {
	type shardInfo struct {
		name           string
		activityStatus string
	}

	var localShards []shardInfo
	className := i.Config.ClassName.String()

	err := i.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, physical := range state.Physical {
			if state.IsLocalShard(shardName) {
				localShards = append(localShards, shardInfo{
					name:           shardName,
					activityStatus: physical.ActivityStatus(),
				})
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read sharding state: %w", err)
	}

	if i.Config.DisableLazyLoadShards {
		eg := enterrors.NewErrorGroupWrapper(i.logger)
		eg.SetLimit(_NUMCPU)

		for _, shard := range localShards {
			if shard.activityStatus != models.TenantActivityStatusHOT {
				continue
			}

			shardName := shard.name
			eg.Go(func() error {
				if err := i.shardLoadLimiter.Acquire(ctx); err != nil {
					return fmt.Errorf("acquiring permit to load shard: %w", err)
				}
				defer i.shardLoadLimiter.Release()

				newShard, err := NewShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.scheduler,
					i.indexCheckpoints, i.shardReindexer, false, i.bitmapBufPool)
				if err != nil {
					return fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
				}

				i.shards.Store(shardName, newShard)
				return nil
			}, shardName)
		}

		if err := eg.Wait(); err != nil {
			return err
		}

		i.allShardsReady.Store(true)
		return nil
	}

	activeShardNames := make([]string, 0, len(localShards))

	for _, shard := range localShards {
		if shard.activityStatus != models.TenantActivityStatusHOT {
			continue
		}

		activeShardNames = append(activeShardNames, shard.name)

		lazyShard := NewLazyLoadShard(ctx, promMetrics, shard.name, i, class, i.centralJobQueue, i.indexCheckpoints,
			i.allocChecker, i.shardLoadLimiter, i.shardReindexer, true, i.bitmapBufPool)
		i.shards.Store(shard.name, lazyShard)
	}

	// NOTE(dyma):
	// 1. So "lazy-loaded" shards are actually loaded "half-eagerly"?
	// 2. If <-ctx.Done or we fail to load a shard, should allShardsReady still report true?
	initLazyShardsInBackground := func() {
		defer i.allShardsReady.Store(true)

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		now := time.Now()

		for _, shardName := range activeShardNames {
			select {
			case <-i.closingCtx.Done():
				i.logger.
					WithField("action", "load_all_shards").
					Errorf("failed to load all shards: %v", i.closingCtx.Err())
				return
			case <-ticker.C:
				select {
				case <-i.closingCtx.Done():
					i.logger.
						WithField("action", "load_all_shards").
						Errorf("failed to load all shards: %v", i.closingCtx.Err())
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
	promMetrics *monitoring.PrometheusMetrics, disableLazyLoad bool, implicitShardLoading bool,
) (ShardLike, error) {
	if disableLazyLoad {
		if err := i.allocChecker.CheckMappingAndReserve(3, int(lsmkv.FlushAfterDirtyDefault.Seconds())); err != nil {
			return nil, errors.Wrap(err, "memory pressure: cannot init shard")
		}

		if err := i.shardLoadLimiter.Acquire(ctx); err != nil {
			return nil, fmt.Errorf("acquiring permit to load shard: %w", err)
		}
		defer i.shardLoadLimiter.Release()

		shard, err := NewShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.scheduler,
			i.indexCheckpoints, i.shardReindexer, false, i.bitmapBufPool)
		if err != nil {
			return nil, fmt.Errorf("init shard %s of index %s: %w", shardName, i.ID(), err)
		}

		return shard, nil
	}

	shard := NewLazyLoadShard(ctx, promMetrics, shardName, i, class, i.centralJobQueue, i.indexCheckpoints,
		i.allocChecker, i.shardLoadLimiter, i.shardReindexer, implicitShardLoading, i.bitmapBufPool)
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
// Note: except Dropping and Shutting Down
func (i *Index) ForEachShard(f func(name string, shard ShardLike) error) error {
	// Check if the index is being dropped or shut down to avoid panics when the index is being deleted
	if i.closingCtx.Err() != nil {
		i.logger.WithField("action", "for_each_shard").Debug("index is being dropped or shut down")
		return nil
	}

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
	// Check if the index is being dropped or shut down to avoid panics when the index is being deleted
	if i.closingCtx.Err() != nil {
		i.logger.WithField("action", "for_each_shard_concurrently").Debug("index is being dropped or shut down")
		return nil
	}
	return i.shards.RangeConcurrently(i.logger, f)
}

func (i *Index) ForEachLoadedShardConcurrently(f func(name string, shard ShardLike) error) error {
	return i.shards.RangeConcurrently(i.logger, func(name string, shard ShardLike) error {
		// Skip lazy loaded shard which are not loaded
		if asLazyLoadShard, ok := shard.(*LazyLoadShard); ok {
			if !asLazyLoadShard.isLoaded() {
				return nil
			}
		}
		return f(name, shard)
	})
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
		shard.initPropertyBuckets(ctx, eg, false, props...)
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

func (i *Index) GetInvertedIndexConfig() schema.InvertedIndexConfig {
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

	err := i.stopwords.ReplaceDetectorFromConfig(updated.Stopwords)
	if err != nil {
		return fmt.Errorf("update inverted index config: %w", err)
	}

	err = tokenizer.AddCustomDict(i.Config.ClassName.String(), updated.TokenizerUserDict)
	if err != nil {
		return errors.Wrap(err, "updating inverted index config")
	}

	return nil
}

func (i *Index) asyncReplicationGloballyDisabled() bool {
	return i.globalreplicationConfig.AsyncReplicationDisabled.Get()
}

func (i *Index) updateReplicationConfig(ctx context.Context, cfg *models.ReplicationConfig) error {
	i.replicationConfigLock.Lock()
	defer i.replicationConfigLock.Unlock()

	i.Config.ReplicationFactor = cfg.Factor
	i.Config.DeletionStrategy = cfg.DeletionStrategy
	i.Config.AsyncReplicationEnabled = cfg.AsyncEnabled && i.Config.ReplicationFactor > 1 && !i.asyncReplicationGloballyDisabled()
	i.Config.AsyncReplicationConfig = cfg.AsyncConfig

	maxAsyncReplicationWorkers, err := resolveAsyncReplicationMaxWorkers(
		&i.Config,
		defaultAsyncReplicationMaxWorkers,
	)
	if err != nil {
		return err
	}

	i.asyncReplicationSemaphore = semaphore.NewWeighted(int64(maxAsyncReplicationWorkers))

	err = i.ForEachLoadedShard(func(name string, shard ShardLike) error {
		if i.Config.AsyncReplicationEnabled && cfg.AsyncConfig != nil {
			// if async replication is being enabled, first disable it to reset any previous config
			if err := shard.SetAsyncReplicationEnabled(ctx, nil, false); err != nil {
				return fmt.Errorf("updating async replication on shard %q: %w", name, err)
			}
		}

		if err := shard.SetAsyncReplicationEnabled(ctx, i.Config.AsyncReplicationConfig, i.Config.AsyncReplicationEnabled); err != nil {
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
	QueryHybridMaximumResults           int64
	QueryNestedRefLimit                 int64
	ResourceUsage                       config.ResourceUsage
	LazySegmentsDisabled                bool
	SegmentInfoIntoFileNameEnabled      bool
	WriteMetadataFilesEnabled           bool
	MemtablesFlushDirtyAfter            int
	MemtablesInitialSizeMB              int
	MemtablesMaxSizeMB                  int
	MemtablesMinActiveSeconds           int
	MemtablesMaxActiveSeconds           int
	MinMMapSize                         int64
	MaxReuseWalSize                     int64
	SegmentsCleanupIntervalSeconds      int
	SeparateObjectsCompactions          bool
	CycleManagerRoutinesFactor          int
	IndexRangeableInMemory              bool
	MaxSegmentSize                      int64
	ReplicationFactor                   int64
	DeletionStrategy                    string
	AsyncReplicationEnabled             bool
	AsyncReplicationConfig              *models.ReplicationAsyncConfig
	AvoidMMap                           bool
	DisableLazyLoadShards               bool
	ForceFullReplicasSearch             bool
	TransferInactivityTimeout           time.Duration
	LSMEnableSegmentsChecksumValidation bool
	TrackVectorDimensions               bool
	TrackVectorDimensionsInterval       time.Duration
	UsageEnabled                        bool
	ShardLoadLimiter                    ShardLoadLimiter

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

	QuerySlowLogEnabled    *configRuntime.DynamicValue[bool]
	QuerySlowLogThreshold  *configRuntime.DynamicValue[time.Duration]
	InvertedSorterDisabled *configRuntime.DynamicValue[bool]
	MaintenanceModeEnabled func() bool

	SPFreshEnabled bool
}

func indexID(class schema.ClassName) string {
	return strings.ToLower(string(class))
}

func (i *Index) putObject(ctx context.Context, object *storobj.Object,
	replProps *additional.ReplicationProperties, tenantName string, schemaVersion uint64,
) error {
	if i.Config.ClassName != object.Class() {
		return fmt.Errorf("cannot import object of class %s into index of class %s",
			object.Class(), i.Config.ClassName)
	}

	targetShard, err := i.shardResolver.ResolveShard(ctx, object)
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
	if replProps == nil {
		replProps = defaultConsistency()
	}
	if i.shardHasMultipleReplicasWrite(tenantName, targetShard.Shard) {
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.PutObject(ctx, targetShard.Shard, object, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate insertion: shard=%q: %w", targetShard.Shard, err)
		}
		return nil
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, object.Object.Tenant, targetShard.Shard, localShardOperationWrite)
	defer release()
	if err != nil {
		return err
	}

	// no replication, remote shard (or local not yet inited)
	if shard == nil {
		if err := i.remote.PutObject(ctx, targetShard.Shard, object, schemaVersion); err != nil {
			return fmt.Errorf("put remote object: shard=%q: %w", targetShard.Shard, err)
		}
		return nil
	}

	// no replication, local shard
	i.backupLock.RLock(targetShard.Shard)
	defer i.backupLock.RUnlock(targetShard.Shard)

	err = shard.PutObject(ctx, object)
	if err != nil {
		return fmt.Errorf("put local object: shard=%q: %w", targetShard.Shard, err)
	}

	return nil
}

func (i *Index) IncomingPutObject(ctx context.Context, shardName string,
	object *storobj.Object, schemaVersion uint64,
) error {
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

	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

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

func (i *Index) shardHasMultipleReplicasWrite(tenantName, shardName string) bool {
	// if replication is enabled, we always have multiple replicas
	if i.replicationEnabled() {
		return true
	}
	// if the router is nil, preserve previous behavior by returning false
	if i.router == nil {
		return false
	}
	ws, err := i.router.GetWriteReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
	if err != nil {
		return false
	}
	// we're including additional replicas here to make sure we at least try to push the write
	// to them if they exist
	allReplicas := append(ws.NodeNames(), ws.AdditionalNodeNames()...)
	return len(allReplicas) > 1
}

func (i *Index) shardHasMultipleReplicasRead(tenantName, shardName string) bool {
	// if replication is enabled, we always have multiple replicas
	if i.replicationEnabled() {
		return true
	}
	// if the router is nil, preserve previous behavior by returning false
	if i.router == nil {
		return false
	}
	replicas, err := i.router.GetReadReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
	if err != nil {
		return false
	}
	return len(replicas.NodeNames()) > 1
}

// anyShardHasMultipleReplicasRead returns true if any of the shards has multiple replicas
func (i *Index) anyShardHasMultipleReplicasRead(tenantName string, shardNames []string) bool {
	if i.replicationEnabled() {
		return true
	}
	for _, shardName := range shardNames {
		if i.shardHasMultipleReplicasRead(tenantName, shardName) {
			return true
		}
	}
	return false
}

type localShardOperation string

const (
	localShardOperationWrite localShardOperation = "write"
	localShardOperationRead  localShardOperation = "read"
)

// getShardForDirectLocalOperation is used to try to get a shard for a local read/write operation.
// It will return the shard if it is found, and a release function to release the shard.
// The shard will be nil if the shard is not found, or if the local shard should not be used.
// The caller should always call the release function.
func (i *Index) getShardForDirectLocalOperation(ctx context.Context, tenantName string, shardName string, operation localShardOperation) (ShardLike, func(), error) {
	shard, release, err := i.GetShard(ctx, shardName)
	// NOTE release should always be ok to call, even if there is an error or the shard is nil,
	// see Index.getOptInitLocalShard for more details.
	if err != nil {
		return nil, release, err
	}

	// if the router is nil, just use the default behavior
	if i.router == nil {
		return shard, release, nil
	}

	// get the replicas for the shard
	var rs routerTypes.ReadReplicaSet
	var ws routerTypes.WriteReplicaSet
	switch operation {
	case localShardOperationWrite:
		ws, err = i.router.GetWriteReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
		if err != nil {
			return shard, release, nil
		}
		// if the local node is not in the list of replicas, don't return the shard (but still allow the caller to release)
		// we should not read/write from the local shard if the local node is not in the list of replicas (eg we should use the remote)
		if !slices.Contains(ws.NodeNames(), i.replicator.LocalNodeName()) {
			return nil, release, nil
		}
	case localShardOperationRead:
		rs, err = i.router.GetReadReplicasLocation(i.Config.ClassName.String(), tenantName, shardName)
		if err != nil {
			return shard, release, nil
		}
		// if the local node is not in the list of replicas, don't return the shard (but still allow the caller to release)
		// we should not read/write from the local shard if the local node is not in the list of replicas (eg we should use the remote)
		if !slices.Contains(rs.NodeNames(), i.replicator.LocalNodeName()) {
			return nil, release, nil
		}
	default:
		return nil, func() {}, fmt.Errorf("invalid local shard operation: %s", operation)
	}

	return shard, release, nil
}

func (i *Index) AsyncReplicationEnabled() bool {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.ReplicationFactor > 1 && i.Config.AsyncReplicationEnabled && !i.asyncReplicationGloballyDisabled()
}

func (i *Index) AsyncReplicationConfig() *models.ReplicationAsyncConfig {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.Config.AsyncReplicationConfig
}

func (i *Index) asyncReplicationWorkerAcquire(ctx context.Context) error {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	return i.asyncReplicationSemaphore.Acquire(ctx, 1)
}

func (i *Index) asyncReplicationWorkerRelease() {
	i.replicationConfigLock.RLock()
	defer i.replicationConfigLock.RUnlock()

	i.asyncReplicationSemaphore.Release(1)
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
	if replProps == nil {
		replProps = defaultConsistency()
	}

	byShard := map[string]objsAndPos{}
	for pos, obj := range objects {
		target, err := i.shardResolver.ResolveShard(ctx, obj)
		if err != nil {
			out[pos] = err
			continue
		}
		group := byShard[target.Shard]
		group.objects = append(group.objects, obj)
		group.pos = append(group.pos, pos)
		byShard[target.Shard] = group
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
			// All objects in the same shard group have the same tenant since in multi-tenant
			// systems all objects belonging to a tenant end up in the same shard.
			// For non-multi-tenant collections, Object.Tenant is empty for all objects.
			// Therefore, we can safely use the tenant from any object in the group.
			tenantName := group.objects[0].Object.Tenant
			var errs []error
			if i.shardHasMultipleReplicasWrite(tenantName, shardName) {
				errs = i.replicator.PutObjects(ctx, shardName, group.objects,
					routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
			} else {
				shard, release, err := i.getShardForDirectLocalOperation(ctx, tenantName, shardName, localShardOperationWrite)
				defer release()
				if err != nil {
					errs = []error{err}
				} else if shard != nil {
					func() {
						i.backupLock.RLock(shardName)
						defer i.backupLock.RUnlock(shardName)
						defer release()
						errs = shard.PutObjectBatch(ctx, group.objects)
					}()
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

	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

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
	if replProps == nil {
		replProps = defaultConsistency()
	}

	byShard := map[string]refsAndPos{}
	out := make([]error, len(refs))

	for pos, ref := range refs {
		shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, ref.From.TargetID, ref.Tenant)
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
		// All references in the same shard group have the same tenant since in multi-tenant
		// systems all objects belonging to a tenant end up in the same shard.
		// For non-multi-tenant collections, ref.Tenant is empty for all references.
		// Therefore, we can safely use the tenant from any reference in the group.
		tenantName := group.refs[0].Tenant
		var errs []error
		if i.shardHasMultipleReplicasWrite(tenantName, shardName) {
			errs = i.replicator.AddReferences(ctx, shardName, group.refs, routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
		} else {
			shard, release, err := i.getShardForDirectLocalOperation(ctx, tenantName, shardName, localShardOperationWrite)
			if err != nil {
				errs = duplicateErr(err, len(group.refs))
			} else if shard != nil {
				func() {
					i.backupLock.RLock(shardName)
					defer i.backupLock.RUnlock(shardName)
					defer release()
					errs = shard.AddReferencesBatch(ctx, group.refs)
				}()
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
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

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
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, id, tenant)
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

	if i.shardHasMultipleReplicasRead(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		if replProps.NodeName != "" {
			obj, err = i.replicator.NodeObject(ctx, replProps.NodeName, shardName, id, props, addl)
		} else {
			obj, err = i.replicator.GetOne(ctx, routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), shardName, id, props, addl)
		}
		return obj, err
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
	defer release()
	if err != nil {
		return obj, err
	}
	defer release()

	if shard != nil {
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
	type idsAndPos struct {
		ids []multi.Identifier
		pos []int
	}

	byShard := map[string]idsAndPos{}
	for pos, id := range query {
		shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, strfmt.UUID(id.ID), tenant)
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

		shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
		if err != nil {
			return nil, err
		} else if shard != nil {
			func() {
				defer release()
				objects, err = shard.MultiObjectByID(ctx, group.ids)
				if err != nil {
					err = errors.Wrapf(err, "local shard %s", shardId(i.ID(), shardName))
				}
			}()
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
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, id, tenant)
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
	if i.shardHasMultipleReplicasRead(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		return i.replicator.Exists(ctx, cl, shardName, id)
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
	defer release()
	if err != nil {
		return exists, err
	}

	if shard != nil {
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
	cl := i.consistencyLevel(replProps, routerTypes.ConsistencyLevelOne)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
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

	outObjects, outScores, err := i.objectSearchByShard(ctx, limit, filters, keywordRanking, sort, cursor, addlProps, tenant, readPlan, properties)
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
		if len(readPlan.Shards()) > 1 {
			var err error
			outObjects, outScores, err = i.sort(outObjects, outScores, sort, limit)
			if err != nil {
				return nil, nil, errors.Wrap(err, "sort")
			}
		}
	} else if keywordRanking != nil {
		outObjects, outScores = i.sortKeywordRanking(outObjects, outScores)
	} else if len(readPlan.Shards()) > 1 && !addlProps.ReferenceQuery {
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

	if i.anyShardHasMultipleReplicasRead(tenant, readPlan.Shards()) {
		err = i.replicator.CheckConsistency(ctx, cl, outObjects)
		if err != nil {
			i.logger.WithField("action", "object_search").
				Errorf("failed to check consistency of search results: %v", err)
		}
	}

	return outObjects, outScores, nil
}

func (i *Index) objectSearchByShard(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addlProps additional.Properties, tenant string, readPlan routerTypes.ReadRoutingPlan, properties []string,
) ([]*storobj.Object, []float32, error) {
	resultObjects, resultScores := objectSearchPreallocate(limit, readPlan.Shards())

	eg := enterrors.NewErrorGroupWrapper(i.logger, "filters:", filters)
	// When running in fractional CPU environments, _NUMCPU will be 1
	// Most cloud deployments of Weaviate are in HA clusters with rf=3
	// Therefore, we should set the maximum amount of concurrency to at least 3
	// so that single-tenant rf=3 queries are not serialized. For any higher value of
	// _NUMCPU, e.g. 8, the extra goroutine will not be a significant overhead (16 -> 17)
	eg.SetLimit(_NUMCPU*2 + 1)
	shardResultLock := sync.Mutex{}

	remoteSearch := func(shardName string) error {
		objs, scores, nodeName, err := i.remote.SearchShard(ctx, shardName, nil, nil, 0, limit, filters, keywordRanking, sort, cursor, nil, addlProps, nil, properties)
		if err != nil {
			return fmt.Errorf(
				"remote shard object search %s: %w", shardName, err)
		}

		if i.shardHasMultipleReplicasRead(tenant, shardName) {
			storobj.AddOwnership(objs, nodeName, shardName)
		}

		shardResultLock.Lock()
		resultObjects = append(resultObjects, objs...)
		resultScores = append(resultScores, scores...)
		shardResultLock.Unlock()

		return nil
	}
	localSeach := func(shardName string) error {
		// We need to getOrInit here because the shard might not yet be loaded due to eventual consistency on the schema update
		// triggering the shard loading in the database
		shard, release, err := i.getOrInitShard(ctx, shardName)
		defer release()
		if err != nil {
			return fmt.Errorf("error getting local shard %s: %w", shardName, err)
		}
		if shard == nil {
			// This will make the code hit other remote replicas, and usually resolve any kind of eventual consistency issues just thanks to delaying
			// the search to the other replica.
			// This is not ideal, but it works for now.
			return remoteSearch(shardName)
		}

		localCtx := helpers.InitSlowQueryDetails(ctx)
		helpers.AnnotateSlowQueryLog(localCtx, "is_coordinator", true)
		objs, scores, err := shard.ObjectSearch(localCtx, limit, filters, keywordRanking, sort, cursor, addlProps, properties)
		if err != nil {
			return fmt.Errorf(
				"local shard object search %s: %w", shard.ID(), err)
		}
		nodeName := i.getSchema.NodeName()

		if i.shardHasMultipleReplicasRead(tenant, shardName) {
			storobj.AddOwnership(objs, nodeName, shardName)
		}

		shardResultLock.Lock()
		resultObjects = append(resultObjects, objs...)
		resultScores = append(resultScores, scores...)
		shardResultLock.Unlock()

		return nil
	}
	err := executor.ExecuteForEachShard(readPlan,
		// Local Shard Search
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				return localSeach(shardName)
			}, shardName)
			return nil
		},
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				return remoteSearch(shardName)
			}, shardName)
			return nil
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing search for each shard: %w", err)
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
		results := make([]resultSortable, len(objs))
		for i := range objs {
			results[i] = resultSortable{
				object: objs[i],
				score:  scores[i],
			}
		}

		golangSort.Slice(results, func(i, j int) bool {
			if results[i].score == results[j].score {
				return results[i].object.Object.ID < results[j].object.Object.ID
			}

			return results[i].score > results[j].score
		})

		finalObjs := make([]*storobj.Object, len(results))
		finalScores := make([]float32, len(results))
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

func (i *Index) localShardSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, dist float32, limit int, localFilters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additionalProps additional.Properties,
	targetCombination *dto.TargetCombination, properties []string, tenantName string, shardName string,
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
	if i.shardHasMultipleReplicasRead(tenantName, shardName) {
		storobj.AddOwnership(localShardResult, i.getSchema.NodeName(), shardName)
	}
	return localShardResult, localShardScores, nil
}

func (i *Index) remoteShardSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, distance float32, limit int, localFilters *filters.LocalFilter,
	sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties,
	targetCombination *dto.TargetCombination, properties []string, tenantName string, shardName string,
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
			nil, sort, nil, groupBy, additional, i.getSchema.NodeName(), targetCombination, properties)
		// Only return an error if we failed to query remote shards AND we had no local shard to query
		if err != nil && shard == nil {
			return nil, nil, errors.Wrapf(err, "remote shard %s", shardName)
		}
		// Append the result of the search to the outgoing result
		for _, remoteShardResult := range remoteSearchResults {
			if i.shardHasMultipleReplicasRead(tenantName, shardName) {
				storobj.AddOwnership(remoteShardResult.Objects, remoteShardResult.Node, shardName)
			}
			outObjects = append(outObjects, remoteShardResult.Objects...)
			outScores = append(outScores, remoteShardResult.Scores...)
		}
	} else {
		// Search only what is necessary
		remoteResult, remoteDists, nodeName, err := i.remote.SearchShard(ctx,
			shardName, searchVectors, targetVectors, distance, limit, localFilters,
			nil, sort, nil, groupBy, additional, targetCombination, properties)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "remote shard %s", shardName)
		}

		if i.shardHasMultipleReplicasRead(tenantName, shardName) {
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
	cl := i.consistencyLevel(replProps, routerTypes.ConsistencyLevelOne)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
		return nil, nil, err
	}

	if len(readPlan.Shards()) == 1 && !i.Config.ForceFullReplicasSearch {
		shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, readPlan.Shards()[0], localShardOperationRead)
		defer release()
		if err != nil {
			return nil, nil, err
		}
		if shard != nil {
			return i.singleLocalShardObjectVectorSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters,
				sort, groupBy, additionalProps, shard, targetCombination, properties)
		}
	}

	// a limit of -1 is used to signal a search by distance. if that is
	// the case we have to adjust how we calculate the output capacity
	var shardCap int
	if limit < 0 {
		shardCap = len(readPlan.Shards()) * hnsw.DefaultSearchByDistInitialLimit
	} else {
		shardCap = len(readPlan.Shards()) * limit
	}

	eg := enterrors.NewErrorGroupWrapper(i.logger, "tenant:", tenant)
	// When running in fractional CPU environments, _NUMCPU will be 1
	// Most cloud deployments of Weaviate are in HA clusters with rf=3
	// Therefore, we should set the maximum amount of concurrency to at least 3
	// so that single-tenant rf=3 queries are not serialized. For any higher value of
	// _NUMCPU, e.g. 8, the extra goroutine will not be a significant overhead (16 -> 17)
	eg.SetLimit(_NUMCPU*2 + 1)
	m := &sync.Mutex{}

	out := make([]*storobj.Object, 0, shardCap)
	dists := make([]float32, 0, shardCap)
	var localSearches atomic.Int64
	var localResponses atomic.Int64
	var remoteSearches atomic.Int64
	var remoteResponses atomic.Int64

	remoteSearch := func(shardName string) error {
		// If we have no local shard or if we force the query to reach all replicas
		remoteShardObject, remoteShardScores, err2 := i.remoteShardSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties, tenant, shardName)
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
	}
	localSearch := func(shardName string) error {
		shard, release, err := i.GetShard(ctx, shardName)
		defer release()
		if err != nil {
			return err
		}
		if shard != nil {
			localShardResult, localShardScores, err1 := i.localShardSearch(ctx, searchVectors, targetVectors, dist, limit, localFilters, sort, groupBy, additionalProps, targetCombination, properties, tenant, shardName)
			if err1 != nil {
				return fmt.Errorf(
					"local shard object search %s: %w", shard.ID(), err1)
			}

			m.Lock()
			localResponses.Add(1)
			out = append(out, localShardResult...)
			dists = append(dists, localShardScores...)
			m.Unlock()
		} else {
			return remoteSearch(shardName)
		}

		return nil
	}

	err = executor.ExecuteForEachShard(readPlan,
		// Local Shard Search
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				localSearches.Add(1)
				return localSearch(shardName)
			}, shardName)
			return nil
		},
		func(replica routerTypes.Replica) error {
			shardName := replica.ShardName
			eg.Go(func() error {
				remoteSearches.Add(1)
				return remoteSearch(shardName)
			}, shardName)
			return nil
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing search for each shard: %w", err)
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	// If we are force querying all replicas, we need to run deduplication on the result.
	if i.Config.ForceFullReplicasSearch {
		if localSearches.Load() != localResponses.Load() {
			i.logger.Warnf("(in full replica search) local search count does not match local response count: searches=%d responses=%d", localSearches.Load(), localResponses.Load())
		}
		if remoteSearches.Load() != remoteResponses.Load() {
			i.logger.Warnf("(in full replica search) remote search count does not match remote response count: searches=%d responses=%d", remoteSearches.Load(), remoteResponses.Load())
		}
		out, dists, err = searchResultDedup(out, dists)
		if err != nil {
			return nil, nil, fmt.Errorf("could not deduplicate result after full replicas search: %w", err)
		}
	}

	if len(readPlan.Shards()) == 1 {
		return out, dists, nil
	}

	if len(readPlan.Shards()) > 1 && groupBy != nil {
		return i.mergeGroups(out, dists, groupBy, limit, len(readPlan.Shards()))
	}

	if len(readPlan.Shards()) > 1 && len(sort) > 0 {
		return i.sort(out, dists, sort, limit)
	}

	out, dists = newDistancesSorter().sort(out, dists)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		dists = dists[:limit]
	}

	if i.anyShardHasMultipleReplicasRead(tenant, readPlan.Shards()) {
		err = i.replicator.CheckConsistency(ctx, cl, out)
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
	if i.replicationEnabled() && shard.GetStatus() == storagestate.StatusLoading {
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
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, id, tenant)
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

	if i.shardHasMultipleReplicasWrite(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.DeleteObject(ctx, shardName, id, deletionTime, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate deletion: shard=%q %w", shardName, err)
		}
		return nil
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationWrite)
	defer release()
	if err != nil {
		return err
	}

	// no replication, remote shard (or local not yet inited)
	if shard == nil {
		if err := i.remote.DeleteObject(ctx, shardName, id, deletionTime, schemaVersion); err != nil {
			return fmt.Errorf("delete remote object: shard=%q: %w", shardName, err)
		}
		return nil
	}

	// no replication, local shard
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)
	if err = shard.DeleteObject(ctx, id, deletionTime); err != nil {
		return fmt.Errorf("delete local object: shard=%q: %w", shardName, err)
	}
	return nil
}

func (i *Index) IncomingDeleteObject(ctx context.Context, shardName string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

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
	return i.initLocalShardWithForcedLoading(ctx, i.getClass(), shardName, false, false)
}

func (i *Index) LoadLocalShard(ctx context.Context, shardName string, implicitShardLoading bool) error {
	// TODO: implicitShardLoading needs to be double checked if needed at all
	// consalidate mustLoad and implicitShardLoading
	return i.initLocalShardWithForcedLoading(ctx, i.getClass(), shardName, true, implicitShardLoading)
}

func (i *Index) initLocalShardWithForcedLoading(ctx context.Context, class *models.Class, shardName string, mustLoad bool, implicitShardLoading bool) error {
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

	shard, err := i.initShard(ctx, shardName, class, i.metrics.baseMetrics, disableLazyLoad, implicitShardLoading)
	if err != nil {
		return err
	}

	i.shards.Store(shardName, shard)

	return nil
}

func (i *Index) UnloadLocalShard(ctx context.Context, shardName string) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.shardCreateLocks.Lock(shardName)
	defer i.shardCreateLocks.Unlock(shardName)

	shardLike, ok := i.shards.LoadAndDelete(shardName)
	if !ok {
		return nil // shard was not found, nothing to unload
	}

	if err := shardLike.Shutdown(ctx); err != nil {
		if !errors.Is(err, errAlreadyShutdown) {
			return errors.Wrapf(err, "shutdown shard %q", shardName)
		}
		return errors.Wrapf(errAlreadyShutdown, "shutdown shard %q", shardName)
	}

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

	// make sure same shard is not inited in parallel. In case it is not loaded yet, switch to a RW lock and initialize
	// the shard
	i.shardCreateLocks.RLock(shardName)

	// check if created in the meantime by concurrent call
	shard = i.shards.Load(shardName)
	if shard == nil {
		// If the shard is not yet loaded, we need to upgrade to a write lock to ensure only one goroutine initializes
		// the shard
		i.shardCreateLocks.RUnlock(shardName)
		if !ensureInit {
			return nil, func() {}, nil
		}

		className := i.Config.ClassName.String()
		class := i.getSchema.ReadOnlyClass(className)
		if class == nil {
			return nil, func() {}, fmt.Errorf("class %s not found in schema", className)
		}

		i.shardCreateLocks.Lock(shardName)
		defer i.shardCreateLocks.Unlock(shardName)

		// double check if loaded in the meantime by concurrent call, if not load it
		shard = i.shards.Load(shardName)
		if shard == nil {
			shard, err = i.initShard(ctx, shardName, class, i.metrics.baseMetrics, true, false)
			if err != nil {
				return nil, func() {}, err
			}
			i.shards.Store(shardName, shard)
		}
	} else {
		// shard already loaded, so we can defer the Runlock
		defer i.shardCreateLocks.RUnlock(shardName)
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
	shardName, err := i.shardResolver.ResolveShardByObjectID(ctx, merge.ID, tenant)
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

	if i.shardHasMultipleReplicasWrite(tenant, shardName) {
		if replProps == nil {
			replProps = defaultConsistency()
		}
		cl := routerTypes.ConsistencyLevel(replProps.ConsistencyLevel)
		if err := i.replicator.MergeObject(ctx, shardName, &merge, cl, schemaVersion); err != nil {
			return fmt.Errorf("replicate single update: %w", err)
		}
		return nil
	}

	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationWrite)
	defer release()
	if err != nil {
		return err
	}

	// no replication, remote shard (or local not yet inited)
	if shard == nil {
		if err := i.remote.MergeObject(ctx, shardName, merge, schemaVersion); err != nil {
			return fmt.Errorf("update remote object: shard=%q: %w", shardName, err)
		}
		return nil
	}

	// no replication, local shard
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)
	if err = shard.MergeObject(ctx, merge); err != nil {
		return fmt.Errorf("update local object: shard=%q: %w", shardName, err)
	}

	return nil
}

func (i *Index) IncomingMergeObject(ctx context.Context, shardName string,
	mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.MergeObject(ctx, mergeDoc)
}

func (i *Index) aggregate(ctx context.Context, replProps *additional.ReplicationProperties,
	params aggregation.Params, modules *modules.Provider, tenant string,
) (*aggregation.Result, error) {
	cl := i.consistencyLevel(replProps)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
		return nil, err
	}

	results := make([]*aggregation.Result, len(readPlan.Shards()))
	for j, shardName := range readPlan.Shards() {
		var err error
		var res *aggregation.Result

		var shard ShardLike
		var release func()
		// anonymous func is here to ensure release is executed after each loop iteration
		func() {
			shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
			defer release()
			if err == nil {
				if shard != nil {
					res, err = shard.Aggregate(ctx, params, modules)
				} else {
					res, err = i.remote.Aggregate(ctx, shardName, params)
				}
			}
		}()

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
	i.closeLock.Lock()
	defer i.closeLock.Unlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.closed = true

	i.closingCancel()

	// Check if a backup is in progress. Dont delete files in this case so the backup process can complete successfully
	// The files will be deleted after the backup is completed and in case of a crash on next startup.
	lastBackup := i.lastBackup.Load()
	keepFiles := lastBackup != nil

	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)
	fields := logrus.Fields{"action": "drop_shard", "class": i.Config.ClassName}
	dropShard := func(shardName string, _ ShardLike) error {
		eg.Go(func() error {
			i.backupLock.RLock(shardName)
			defer i.backupLock.RUnlock(shardName)

			i.shardCreateLocks.Lock(shardName)
			defer i.shardCreateLocks.Unlock(shardName)

			shard, ok := i.shards.LoadAndDelete(shardName)
			if !ok {
				return nil // shard already does not exist
			}
			if err := shard.drop(keepFiles); err != nil {
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

	if !keepFiles {
		return os.RemoveAll(i.path())
	} else {
		return os.Rename(i.path(), filepath.Join(i.Config.RootPath, backup.DeleteMarkerAdd(i.ID())))
	}
}

func (i *Index) dropShards(names []string) error {
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
			i.backupLock.RLock(name)
			defer i.backupLock.RUnlock(name)
			i.shardCreateLocks.Lock(name)
			defer i.shardCreateLocks.Unlock(name)

			shard, ok := i.shards.LoadAndDelete(name)
			if !ok {
				// Ensure that if the shard is not loaded we delete any reference on disk for any data.
				// This ensures that we also delete inactive shards/tenants
				if err := os.RemoveAll(shardPath(i.path(), name)); err != nil {
					ec.Add(err)
					i.logger.WithField("action", "drop_shard").WithField("shard", shard.ID()).Error(err)
				}
			} else {
				// If shard is loaded use the native primitive to drop it
				if err := shard.drop(false); err != nil {
					ec.Add(err)
					i.logger.WithField("action", "drop_shard").WithField("shard", shard.ID()).Error(err)
				}
			}

			return nil
		})
	}

	eg.Wait()
	return ec.ToError()
}

func (i *Index) dropCloudShards(ctx context.Context, cloud modulecapabilities.OffloadCloud, names []string, nodeId string) error {
	i.closeLock.RLock()
	defer i.closeLock.RUnlock()

	if i.closed {
		return errAlreadyShutdown
	}

	ec := &errorcompounder.ErrorCompounder{}
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range names {
		eg.Go(func() error {
			i.backupLock.RLock(name)
			defer i.backupLock.RUnlock(name)
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
	i.closeLock.Lock()
	defer i.closeLock.Unlock()

	if i.closed {
		return errAlreadyShutdown
	}

	i.closed = true

	i.closingCancel()

	// TODO allow every resource cleanup to run, before returning early with error
	if err := i.shards.RangeConcurrently(i.logger, func(name string, shard ShardLike) error {
		i.backupLock.RLock(name)
		defer i.backupLock.RUnlock(name)

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

func (i *Index) getShardsQueueSize(ctx context.Context, tenant string) (map[string]int64, error) {
	className := i.Config.ClassName.String()
	shardNames, err := i.schemaReader.Shards(className)
	if err != nil {
		return nil, err
	}

	shardsQueueSize := make(map[string]int64)
	for _, shardName := range shardNames {
		if tenant != "" && shardName != tenant {
			continue
		}
		var err error
		var size int64
		var shard ShardLike
		var release func()

		// anonymous func is here to ensure release is executed after each loop iteration
		func() {
			shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
			defer release()
			if err == nil {
				if shard != nil {
					_ = shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
						size += queue.Size()
						return nil
					})
				} else {
					size, err = i.remote.GetShardQueueSize(ctx, shardName)
				}
			}
		}()

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
	className := i.Config.ClassName.String()
	shardNames, err := i.schemaReader.Shards(className)
	if err != nil {
		return nil, err
	}

	shardsStatus := make(map[string]string)

	for _, shardName := range shardNames {
		if tenant != "" && shardName != tenant {
			continue
		}
		var err error
		var status string
		var shard ShardLike
		var release func()

		// anonymous func is here to ensure release is executed after each loop iteration
		func() {
			shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
			defer release()
			if err == nil {
				if shard != nil {
					status = shard.GetStatus().String()
				} else {
					status, err = i.remote.GetShardStatus(ctx, shardName)
				}
			}
		}()

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

func (i *Index) updateShardStatus(ctx context.Context, tenantName, shardName, targetStatus string, schemaVersion uint64) error {
	shard, release, err := i.getShardForDirectLocalOperation(ctx, tenantName, shardName, localShardOperationWrite)
	if err != nil {
		return err
	}
	if shard == nil {
		return i.remote.UpdateShardStatus(ctx, shardName, targetStatus, schemaVersion)
	}
	defer release()
	return shard.UpdateStatus(targetStatus, "manually set by user")
}

func (i *Index) IncomingUpdateShardStatus(ctx context.Context, shardName, targetStatus string, schemaVersion uint64) error {
	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	return shard.UpdateStatus(targetStatus, "manually set by user")
}

func (i *Index) findUUIDs(ctx context.Context,
	filters *filters.LocalFilter, tenant string, repl *additional.ReplicationProperties,
) (map[string][]strfmt.UUID, error) {
	before := time.Now()
	defer i.metrics.BatchDelete(before, "filter_total")
	cl := i.consistencyLevel(repl)
	readPlan, err := i.buildReadRoutingPlan(cl, tenant)
	if err != nil {
		return nil, err
	}
	className := i.Config.ClassName.String()

	results := make(map[string][]strfmt.UUID)
	for _, shardName := range readPlan.Shards() {
		var shard ShardLike
		var release func()
		var err error

		if i.shardHasMultipleReplicasRead(tenant, shardName) {
			results[shardName], err = i.replicator.FindUUIDs(ctx, className, shardName, filters, cl)
		} else {
			// anonymous func is here to ensure release is executed after each loop iteration
			func() {
				shard, release, err = i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationRead)
				defer release()
				if err == nil {
					if shard != nil {
						results[shardName], err = shard.FindUUIDs(ctx, filters)
					} else {
						results[shardName], err = i.remote.FindUUIDs(ctx, shardName, filters)
					}
				}
			}()
		}

		if err != nil {
			return nil, fmt.Errorf("find matching doc ids in shard %q: %w", shardName, err)
		}
	}

	return results, nil
}

// consistencyLevel returns the consistency level for the given replication properties.
// If repl is not nil, the consistency level is returned from repl.
// If repl is nil and a default override is provided, the default override is returned.
// If repl is nil and no default override is provided, the default consistency level
// is returned (QUORUM).
func (i *Index) consistencyLevel(
	repl *additional.ReplicationProperties,
	defaultOverride ...routerTypes.ConsistencyLevel,
) routerTypes.ConsistencyLevel {
	if repl == nil {
		repl = defaultConsistency(defaultOverride...)
	}
	return routerTypes.ConsistencyLevel(repl.ConsistencyLevel)
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
	tenant string,
) (objects.BatchSimpleObjects, error) {
	before := time.Now()
	defer i.metrics.BatchDelete(before, "delete_from_shards_total")

	type result struct {
		objs objects.BatchSimpleObjects
	}

	if replProps == nil {
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
			if i.shardHasMultipleReplicasWrite(tenant, shardName) {
				objs = i.replicator.DeleteObjects(ctx, shardName, uuids, deletionTime,
					dryRun, routerTypes.ConsistencyLevel(replProps.ConsistencyLevel), schemaVersion)
			} else {
				shard, release, err := i.getShardForDirectLocalOperation(ctx, tenant, shardName, localShardOperationWrite)
				defer release()
				if err != nil {
					objs = objects.BatchSimpleObjects{
						objects.BatchSimpleObject{Err: err},
					}
				} else if shard != nil {
					func() {
						i.backupLock.RLock(shardName)
						defer i.backupLock.RUnlock(shardName)
						defer release()
						objs = shard.DeleteObjectBatch(ctx, uuids, deletionTime, dryRun)
					}()
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
	i.backupLock.RLock(shardName)
	defer i.backupLock.RUnlock(shardName)

	shard, release, err := i.getOrInitShard(ctx, shardName)
	if err != nil {
		return objects.BatchSimpleObjects{
			objects.BatchSimpleObject{Err: err},
		}
	}
	defer release()

	return shard.DeleteObjectBatch(ctx, uuids, deletionTime, dryRun)
}

func defaultConsistency(defaultOverride ...routerTypes.ConsistencyLevel) *additional.ReplicationProperties {
	rp := &additional.ReplicationProperties{}
	if len(defaultOverride) != 0 {
		rp.ConsistencyLevel = string(defaultOverride[0])
	} else {
		rp.ConsistencyLevel = string(routerTypes.ConsistencyLevelQuorum)
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
	// Safe type assertion
	if vectorIndexConfig, ok := config.(schemaConfig.VectorIndexConfig); ok {
		return vectorIndexConfig
	}
	return nil
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

func (i *Index) tenantDirExists(tenantName string) (bool, error) {
	tenantPath := shardPath(i.path(), tenantName)
	if _, err := os.Stat(tenantPath); err != nil {
		// when inactive tenant is not populated, its directory does not exist yet
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func (i *Index) buildReadRoutingPlan(cl routerTypes.ConsistencyLevel, tenantName string) (routerTypes.ReadRoutingPlan, error) {
	planOptions := routerTypes.RoutingPlanBuildOptions{
		Tenant:           tenantName,
		ConsistencyLevel: cl,
	}
	readPlan, err := i.router.BuildReadRoutingPlan(planOptions)
	if err != nil {
		return routerTypes.ReadRoutingPlan{}, err
	}

	return readPlan, nil
}

func (i *Index) DebugRequantizeIndex(ctx context.Context, shardName, targetVector string) error {
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
		err := shard.RequantizeIndex(context.Background(), targetVector)
		if err != nil {
			i.logger.WithField("shard", shardName).WithError(err).Error("failed to requantize vector index")
			return
		}
	}, i.logger)

	return nil
}
