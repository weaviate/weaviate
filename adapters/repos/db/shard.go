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
	"path"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"golang.org/x/sync/errgroup"
)

const IdLockPoolSize = 128

type ShardLike interface {
	Index() *Index                                                                      // Get the parent index
	Name() string                                                                       // Get the shard name
	Store() *lsmkv.Store                                                                // Get the underlying store
	NotifyReady()                                                                       // Set shard status to ready
	GetStatus() storagestate.Status                                                     // Return the shard status
	UpdateStatus(status string) error                                                   // Set shard status
	FindUUIDs(ctx context.Context, filters *filters.LocalFilter) ([]strfmt.UUID, error) // Search and return document ids

	Counter() *indexcounter.Counter
	ObjectCount() int
	GetPropertyLengthTracker() *inverted.JsonShardMetaData

	PutObject(context.Context, *storobj.Object) error
	PutObjectBatch(context.Context, []*storobj.Object) []error
	ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, id strfmt.UUID) (bool, error)
	ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties) ([]*storobj.Object, []float32, error)
	ObjectVectorSearch(ctx context.Context, searchVector []float32, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties) ([]*storobj.Object, []float32, error)
	UpdateVectorIndexConfig(ctx context.Context, updated schema.VectorIndexConfig) error
	AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error
	DeleteObjectBatch(ctx context.Context, ids []strfmt.UUID, dryRun bool) objects.BatchSimpleObjects // Delete many objects by id
	DeleteObject(ctx context.Context, id strfmt.UUID) error                                           // Delete object by id
	MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error)
	ID() string // Get the shard id
	drop() error
	addIDProperty(ctx context.Context) error
	addDimensionsProperty(ctx context.Context) error
	addTimestampProperties(ctx context.Context) error
	createPropertyIndex(ctx context.Context, prop *models.Property, eg *errgroup.Group)
	BeginBackup(ctx context.Context) error
	ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error //
	resumeMaintenanceCycles(ctx context.Context) error
	SetPropertyLengths(props []inverted.Property) error
	AnalyzeObject(*storobj.Object) ([]inverted.Property, []nilProp, error) //

	// TODO tests only
	Dimensions() int // dim(vector)*number vectors
	// TODO tests only
	QuantizedDimensions(segments int) int
	Aggregate(ctx context.Context, params aggregation.Params) (*aggregation.Result, error) //
	MergeObject(ctx context.Context, object objects.MergeDocument) error                   //
	Queue() *IndexQueue
	Shutdown(context.Context) error // Shutdown the shard
	// TODO tests only
	ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) // Search and return objects
	WasDeleted(ctx context.Context, id strfmt.UUID) (bool, error)                                                                                                                    // Check if an object was deleted
	VectorIndex() VectorIndex                                                                                                                                                        // Get the vector index
	// TODO tests only
	Versioner() *shardVersioner // Get the shard versioner

	isReadOnly() bool

	preparePutObject(context.Context, string, *storobj.Object) replica.SimpleResponse
	preparePutObjects(context.Context, string, []*storobj.Object) replica.SimpleResponse
	prepareMergeObject(context.Context, string, *objects.MergeDocument) replica.SimpleResponse
	prepareDeleteObject(context.Context, string, strfmt.UUID) replica.SimpleResponse
	prepareDeleteObjects(context.Context, string, []strfmt.UUID, bool) replica.SimpleResponse
	prepareAddReferences(context.Context, string, []objects.BatchReference) replica.SimpleResponse

	commitReplication(context.Context, string, *backupMutex) interface{}
	abortReplication(context.Context, string) replica.SimpleResponse
	reinit(context.Context) error
	filePutter(context.Context, string) (io.WriteCloser, error)

	extendDimensionTrackerLSM(int, uint64) error

	addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error
	pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair
	keyPropertyNull(isNull bool) ([]byte, error)
	keyPropertyLength(length int) ([]byte, error)

	setFallbackToSearchable(fallback bool)
	addJobToQueue(job job)
	uuidFromDocID(docID uint64) (strfmt.UUID, error)
	batchDeleteObject(ctx context.Context, id strfmt.UUID) error
	putObjectLSM(object *storobj.Object, idBytes []byte) (objectInsertStatus, error)
	mutableMergeObjectLSM(merge objects.MergeDocument, idBytes []byte) (mutableMergeResult, error)
	deleteInvertedIndexItemLSM(bucket *lsmkv.Bucket, item inverted.Countable, docID uint64) error
	batchExtendInvertedIndexItemsLSMNoFrequency(b *lsmkv.Bucket, item inverted.MergeItem) error
	updatePropertySpecificIndices(object *storobj.Object, status objectInsertStatus) error
	updateVectorIndexIgnoreDelete(vector []float32, status objectInsertStatus) error
	hasGeoIndex() bool
	ChangeObjectCountBy(int) error

	Metrics() *Metrics
}

// Shard is the smallest completely-contained index unit. A shard manages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	class            *models.Class
	index            *Index // a reference to the underlying index, which in turn contains schema information
	queue            *IndexQueue
	name             string
	store            *lsmkv.Store
	counter          *indexcounter.Counter
	indexCheckpoints *indexcheckpoint.Checkpoints
	vectorIndex      VectorIndex
	metrics          *Metrics
	promMetrics      *monitoring.PrometheusMetrics
	propertyIndices  propertyspecific.Indices
	propLenTracker   *inverted.JsonShardMetaData
	versioner        *shardVersioner

	status              storagestate.Status
	statusLock          sync.Mutex
	propertyIndicesLock sync.RWMutex
	stopMetrics         chan struct{}

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
}

func (s *Shard) initShard(ctx context.Context) (*Shard, error) {
	before := time.Now()
	var err error
	s.initCycleCallbacks()

	s.docIdLock = make([]sync.Mutex, IdLockPoolSize)

	defer s.metrics.ShardStartup(before)

	_, err = os.Stat(s.path())
	exists := false
	if err == nil {
		exists = true
	}

	if err := os.MkdirAll(s.path(), os.ModePerm); err != nil {
		return nil, err
	}

	if s.propLenTracker == nil {
		plPath := path.Join(s.path(), "proplengths")
		tracker, err := inverted.NewJsonShardMetaData(plPath, s.index.logger)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: prop length tracker", s.ID())
		}

		s.propLenTracker = tracker
	}

	if err := s.initNonVector(ctx, s.class); err != nil {
		return nil, errors.Wrapf(err, "init shard %q", s.ID())
	}

	if err := s.initVector(ctx); err != nil {
		return nil, err
	}

	s.queue, err = NewIndexQueue(s.ID(), s, s.VectorIndex(), s.centralJobQueue, s.indexCheckpoints, IndexQueueOptions{Logger: s.index.logger})
	if err != nil {
		return nil, err
	}

	if asyncEnabled() {
		go func() {
			// preload unindexed objects in the background
			err = s.queue.PreloadShard(s)
			if err != nil {
				s.queue.Logger.WithError(err).Error("preload shard")
			}
		}()
	}
	s.NotifyReady()

	if exists {
		s.index.logger.Printf("Completed loading shard %s in %s", s.ID(), time.Since(before))
	} else {
		s.index.logger.Printf("Created shard %s in %s", s.ID(), time.Since(before))
	}

	return s, nil
}

/* NewShard - create a new physical storage shard
 *  ctx - the timeout context
 *  promMetrics - prometheus metrics
 *  shardName - shard name
 *  index - The owning index
 *  class - The class this shard belongs to
 *  jobQueueCh - The central job queue
 *  indexCheckpoints - The index checkpoints
 *  propLengths - The property lengths tracker.  There should only be one instance per shard, and it is stored in the shard.  If a null pointer is passed, a new one will be created.  Care must be taken to only create one, otherwise they will overwrite each other on disk.
 */
func NewShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	indexCheckpoints *indexcheckpoint.Checkpoints, propLengths *inverted.JsonShardMetaData,
) (*Shard, error) {
	s := &Shard{
		index:       index,
		name:        shardName,
		promMetrics: promMetrics,
		metrics: NewMetrics(index.logger, promMetrics,
			string(index.Config.ClassName), shardName),
		stopMetrics:      make(chan struct{}),
		replicationMap:   pendingReplicaTasks{Tasks: make(map[string]replicaTask, 32)},
		centralJobQueue:  jobQueueCh,
		indexCheckpoints: indexCheckpoints,
		propLenTracker:   propLengths,
		class:            class,
	}
	return s.initShard(ctx)
}

func (s *Shard) initVector(ctx context.Context) error {
	var distProv distancer.Provider

	switch s.index.vectorIndexUserConfig.DistanceName() {
	case "", common.DistanceCosine:
		distProv = distancer.NewCosineDistanceProvider()
	case common.DistanceDot:
		distProv = distancer.NewDotProductProvider()
	case common.DistanceL2Squared:
		distProv = distancer.NewL2SquaredProvider()
	case common.DistanceManhattan:
		distProv = distancer.NewManhattanProvider()
	case common.DistanceHamming:
		distProv = distancer.NewHammingProvider()
	default:
		return fmt.Errorf("init vector index: %w",
			errors.Errorf("unrecognized distance metric %q,"+
				"choose one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]", s.index.vectorIndexUserConfig.DistanceName()))
	}

	switch s.index.vectorIndexUserConfig.IndexType() {
	case vectorindex.VectorIndexTypeHNSW:
		hnswUserConfig, ok := s.index.vectorIndexUserConfig.(hnswent.UserConfig)
		if !ok {
			return errors.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
				s.index.vectorIndexUserConfig)
		}

		if hnswUserConfig.Skip {
			s.vectorIndex = noop.NewIndex()
		} else {
			// starts vector cycles if vector is configured
			s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()
			s.index.cycleCallbacks.vectorTombstoneCleanupCycle.Start()

			// a shard can actually have multiple vector indexes:
			// - the main index, which is used for all normal object vectors
			// - a geo property index for each geo prop in the schema
			//
			// here we label the main vector index as such.
			vecIdxID := "main"

			vi, err := hnsw.New(hnsw.Config{
				Logger:               s.index.logger,
				RootPath:             s.path(),
				ID:                   vecIdxID,
				ShardName:            s.name,
				ClassName:            s.index.Config.ClassName.String(),
				PrometheusMetrics:    s.promMetrics,
				VectorForIDThunk:     s.vectorByIndexID,
				TempVectorForIDThunk: s.readVectorByIndexIDIntoSlice,
				DistanceProvider:     distProv,
				MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
					return hnsw.NewCommitLogger(s.path(), vecIdxID,
						s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks)
				},
			}, hnswUserConfig, s.cycleCallbacks.vectorTombstoneCleanupCallbacks,
				s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks, s.store)
			if err != nil {
				return errors.Wrapf(err, "init shard %q: hnsw index", s.ID())
			}
			s.vectorIndex = vi

			defer s.vectorIndex.PostStartup()
		}
	case vectorindex.VectorIndexTypeFLAT:
		flatUserConfig, ok := s.index.vectorIndexUserConfig.(flatent.UserConfig)
		if !ok {
			return errors.Errorf("flat vector index: config is not flat.UserConfig: %T",
				s.index.vectorIndexUserConfig)
		}
		s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()

		// a shard can actually have multiple vector indexes:
		// - the main index, which is used for all normal object vectors
		// - a geo property index for each geo prop in the schema
		//
		// here we label the main vector index as such.
		vecIdxID := "main"

		vi, err := flat.New(flat.Config{
			ID:               vecIdxID,
			Logger:           s.index.logger,
			DistanceProvider: distProv,
		}, flatUserConfig, s.store)
		if err != nil {
			return errors.Wrapf(err, "init shard %q: flat index", s.ID())
		}
		s.vectorIndex = vi
	default:
		return fmt.Errorf("Unknown vector index type: %q. Choose one from [\"%s\", \"%s\"]",
			s.index.vectorIndexUserConfig.IndexType(), vectorindex.VectorIndexTypeHNSW, vectorindex.VectorIndexTypeFLAT)
	}

	return nil
}

func (s *Shard) initNonVector(ctx context.Context, class *models.Class) error {
	err := s.initLSMStore(ctx)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: shard db", s.ID())
	}

	counter, err := indexcounter.New(s.path())
	if err != nil {
		return errors.Wrapf(err, "init shard %q: index counter", s.ID())
	}
	s.counter = counter

	dataPresent := s.counter.PreviewNext() != 0
	versionPath := path.Join(s.path(), "version")
	versioner, err := newShardVersioner(versionPath, dataPresent)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: check versions", s.ID())
	}
	s.versioner = versioner

	if err := s.initProperties(class); err != nil {
		return errors.Wrapf(err, "init shard %q: init per property indices", s.ID())
	}

	s.initDimensionTracking()

	return nil
}

func (s *Shard) ID() string {
	return shardId(s.index.ID(), s.name)
}

func (s *Shard) path() string {
	return shardPath(s.index.path(), s.name)
}

func (s *Shard) pathLSM() string {
	return path.Join(s.path(), "lsm")
}

func (s *Shard) uuidToIdLockPoolId(idBytes []byte) uint8 {
	// use the last byte of the uuid to determine which locking-pool a given object should use. The last byte is used
	// as uuids probably often have some kind of order and the last byte will in general be the one that changes the most
	return idBytes[15] % IdLockPoolSize
}

func (s *Shard) initLSMStore(ctx context.Context) error {
	annotatedLogger := s.index.logger.WithFields(logrus.Fields{
		"shard": s.name,
		"index": s.index.ID(),
		"class": s.index.Config.ClassName,
	})
	var metrics *lsmkv.Metrics
	if s.promMetrics != nil {
		metrics = lsmkv.NewMetrics(s.promMetrics, string(s.index.Config.ClassName), s.name)
	}

	store, err := lsmkv.New(s.pathLSM(), s.path(), annotatedLogger, metrics,
		s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks)
	if err != nil {
		return errors.Wrapf(err, "init lsmkv store at %s", s.pathLSM())
	}

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithMonitorCount(),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithKeepTombstones(true),
		s.dynamicMemtableSizing(),
		s.memtableIdleConfig(),
	)
	if err != nil {
		return errors.Wrap(err, "create objects bucket")
	}

	s.store = store

	return nil
}

// IMPORTANT:
// Be advised there exists LazyLoadShard::drop() implementation intended
// to drop shard that was not loaded (instantiated) yet.
// It deletes shard by performing required actions and removing entire shard directory.
// If there is any action that needs to be performed beside files/dirs being removed
// from shard directory, it needs to be reflected as well in LazyLoadShard::drop()
// method to keep drop behaviour consistent.
func (s *Shard) drop() error {
	s.metrics.DeleteShardLabels(s.index.Config.ClassName.String(), s.name)
	s.metrics.baseMetrics.StartUnloadingShard(s.index.Config.ClassName.String())
	s.replicationMap.clear()

	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
		if s.promMetrics != nil {
			// send 0 in when index gets dropped
			s.clearDimensionMetrics()
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	// unregister all callbacks at once, in parallel
	if err := cyclemanager.NewCombinedCallbackCtrl(0,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx); err != nil {
		return err
	}

	if err := s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	if _, err := os.Stat(s.pathLSM()); err == nil {
		err := os.RemoveAll(s.pathLSM())
		if err != nil {
			return errors.Wrapf(err, "remove lsm store at %s", s.pathLSM())
		}
	}
	// delete indexcount
	err := s.counter.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.path())
	}

	// delete version
	err = s.versioner.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove version at %s", s.path())
	}

	// delete queue cursor
	err = s.queue.Drop()
	if err != nil {
		return errors.Wrapf(err, "close queue at %s", s.path())
	}
	// remove vector index
	err = s.VectorIndex().Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "remove vector index at %s", s.path())
	}

	// delete property length tracker
	err = s.GetPropertyLengthTracker().Drop()
	if err != nil {
		return errors.Wrapf(err, "remove prop length tracker at %s", s.path())
	}

	s.propertyIndicesLock.Lock()
	err = s.propertyIndices.DropAll(ctx)
	s.propertyIndicesLock.Unlock()
	if err != nil {
		return errors.Wrapf(err, "remove property specific indices at %s", s.path())
	}

	s.metrics.baseMetrics.FinishUnloadingShard(s.index.Config.ClassName.String())

	return nil
}

func (s *Shard) addIDProperty(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropID),
		lsmkv.WithIdleThreshold(time.Duration(s.index.Config.MemtablesFlushIdleAfter)*time.Second),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection),
		lsmkv.WithPread(s.index.Config.AvoidMMap))
}

func (s *Shard) addDimensionsProperty(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	// Note: this data would fit the "Set" type better, but since the "Map" type
	// is currently optimized better, it is more efficient to use a Map here.
	err := s.store.CreateOrLoadBucket(ctx,
		helpers.DimensionsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection),
		lsmkv.WithPread(s.index.Config.AvoidMMap))
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addTimestampProperties(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if err := s.addCreationTimeUnixProperty(ctx); err != nil {
		return err
	}
	if err := s.addLastUpdateTimeUnixProperty(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Shard) addCreationTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropCreationTimeUnix),
		lsmkv.WithIdleThreshold(time.Duration(s.index.Config.MemtablesFlushIdleAfter)*time.Second),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap))
}

func (s *Shard) addLastUpdateTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
		lsmkv.WithIdleThreshold(time.Duration(s.index.Config.MemtablesFlushIdleAfter)*time.Second),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap))
}

func (s *Shard) memtableIdleConfig() lsmkv.BucketOption {
	return lsmkv.WithIdleThreshold(
		time.Duration(s.index.Config.MemtablesFlushIdleAfter) * time.Second)
}

func (s *Shard) dynamicMemtableSizing() lsmkv.BucketOption {
	return lsmkv.WithDynamicMemtableSizing(
		s.index.Config.MemtablesInitialSizeMB,
		s.index.Config.MemtablesMaxSizeMB,
		s.index.Config.MemtablesMinActiveSeconds,
		s.index.Config.MemtablesMaxActiveSeconds,
	)
}

func (s *Shard) createPropertyIndex(ctx context.Context, prop *models.Property, eg *errgroup.Group) {
	if !inverted.HasInvertedIndex(prop) {
		return
	}

	eg.Go(func() error {
		if err := s.createPropertyValueIndex(ctx, prop); err != nil {
			return errors.Wrapf(err, "create property '%s' value index on shard '%s'", prop.Name, s.ID())
		}

		if s.index.invertedIndexConfig.IndexNullState {
			eg.Go(func() error {
				if err := s.createPropertyNullIndex(ctx, prop); err != nil {
					return errors.Wrapf(err, "create property '%s' null index on shard '%s'", prop.Name, s.ID())
				}
				return nil
			})
		}

		if s.index.invertedIndexConfig.IndexPropertyLength {
			eg.Go(func() error {
				if err := s.createPropertyLengthIndex(ctx, prop); err != nil {
					return errors.Wrapf(err, "create property '%s' length index on shard '%s'", prop.Name, s.ID())
				}
				return nil
			})
		}

		return nil
	})
}

func (s *Shard) createPropertyValueIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	bucketOpts := []lsmkv.BucketOption{
		s.memtableIdleConfig(),
		s.dynamicMemtableSizing(),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
	}

	if inverted.HasFilterableIndex(prop) {
		if dt, _ := schema.AsPrimitive(prop.DataType); dt == schema.DataTypeGeoCoordinates {
			return s.initGeoProp(prop)
		}

		if schema.IsRefDataType(prop.DataType) {
			if err := s.store.CreateOrLoadBucket(ctx,
				helpers.BucketFromPropNameMetaCountLSM(prop.Name),
				append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))...,
			); err != nil {
				return err
			}
		}

		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketFromPropNameLSM(prop.Name),
			append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))...,
		); err != nil {
			return err
		}
	}

	if inverted.HasSearchableIndex(prop) {
		searchableBucketOpts := append(bucketOpts,
			lsmkv.WithStrategy(lsmkv.StrategyMapCollection), lsmkv.WithPread(s.index.Config.AvoidMMap))
		if s.versioner.Version() < 2 {
			searchableBucketOpts = append(searchableBucketOpts, lsmkv.WithLegacyMapSorting())
		}

		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketSearchableFromPropNameLSM(prop.Name),
			searchableBucketOpts...,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) createPropertyLengthIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	// some datatypes are not added to the inverted index, so we can skip them here
	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeInt,
		schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return nil
	default:
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLengthLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap))
}

func (s *Shard) createPropertyNullIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameNullLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap))
}

func (s *Shard) UpdateVectorIndexConfig(ctx context.Context, updated schema.VectorIndexConfig) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	err := s.UpdateStatus(storagestate.StatusReadOnly.String())
	if err != nil {
		return fmt.Errorf("attempt to mark read-only: %w", err)
	}

	return s.VectorIndex().UpdateUserConfig(updated, func() {
		s.UpdateStatus(storagestate.StatusReady.String())
	})
}

func (s *Shard) Shutdown(ctx context.Context) error {
	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
	}

	if err := s.GetPropertyLengthTracker().Close(); err != nil {
		return errors.Wrap(err, "close prop length tracker")
	}

	if err := s.queue.Close(); err != nil {
		return errors.Wrap(err, "shut down vector index queue")
	}

	// to ensure that all commitlog entries are written to disk.
	// otherwise in some cases the tombstone cleanup process'
	// 'RemoveTombstone' entry is not picked up on restarts
	// resulting in perpetually attempting to remove a tombstone
	// which doesn't actually exist anymore
	if err := s.VectorIndex().Flush(); err != nil {
		return errors.Wrap(err, "flush vector index commitlog")
	}

	if err := s.VectorIndex().Shutdown(ctx); err != nil {
		return errors.Wrap(err, "shut down vector index")
	}

	// unregister all callbacks at once, in parallel
	if err := cyclemanager.NewCombinedCallbackCtrl(0,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx); err != nil {
		return err
	}

	if err := s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	return nil
}

func (s *Shard) NotifyReady() {
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}

func (s *Shard) ObjectCount() int {
	return s.GetPropertyLengthTracker().ObjectTally()
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
