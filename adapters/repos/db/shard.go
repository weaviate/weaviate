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
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
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
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const IdLockPoolSize = 128

var errAlreadyShutdown = errors.New("already shut or dropped")

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
	ObjectCountAsync() int
	GetPropertyLengthTracker() *inverted.JsonShardMetaData

	PutObject(context.Context, *storobj.Object) error
	PutObjectBatch(context.Context, []*storobj.Object) []error
	ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error)
	ObjectByIDErrDeleted(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, id strfmt.UUID) (bool, error)
	ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor, additional additional.Properties) ([]*storobj.Object, []float32, error)
	ObjectVectorSearch(ctx context.Context, searchVector []float32, targetVector string, targetDist float32, limit int, filters *filters.LocalFilter, sort []filters.Sort, groupBy *searchparams.GroupBy, additional additional.Properties) ([]*storobj.Object, []float32, error)
	UpdateVectorIndexConfig(ctx context.Context, updated schemaConfig.VectorIndexConfig) error
	UpdateVectorIndexConfigs(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error
	UpdateAsyncReplication(ctx context.Context, enabled bool) error
	AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error
	DeleteObjectBatch(ctx context.Context, ids []strfmt.UUID, dryRun bool) objects.BatchSimpleObjects // Delete many objects by id
	DeleteObject(ctx context.Context, id strfmt.UUID) error                                           // Delete object by id
	MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error)
	ObjectDigestsByTokenRange(ctx context.Context, initialToken, finalToken uint64, limit int) (objs []replica.RepairResponse, lastTokenRead uint64, err error)
	ID() string // Get the shard id
	drop() error
	addIDProperty(ctx context.Context) error
	addDimensionsProperty(ctx context.Context) error
	addTimestampProperties(ctx context.Context) error
	createPropertyIndex(ctx context.Context, eg *enterrors.ErrorGroupWrapper, props ...*models.Property) error
	BeginBackup(ctx context.Context) error
	ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error
	resumeMaintenanceCycles(ctx context.Context) error
	SetPropertyLengths(props []inverted.Property) error
	AnalyzeObject(*storobj.Object) ([]inverted.Property, []inverted.NilProperty, error)
	Aggregate(ctx context.Context, params aggregation.Params, modules *modules.Provider) (*aggregation.Result, error)
	HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)
	MergeObject(ctx context.Context, object objects.MergeDocument) error
	Queue() *IndexQueue
	Queues() map[string]*IndexQueue
	Shutdown(context.Context) error // Shutdown the shard
	preventShutdown() (release func(), err error)

	// TODO tests only
	ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor,
		additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) // Search and return objects
	WasDeleted(ctx context.Context, id strfmt.UUID) (bool, error) // Check if an object was deleted
	VectorIndex() VectorIndex                                     // Get the vector index
	VectorIndexes() map[string]VectorIndex                        // Get the vector indexes
	hasTargetVectors() bool
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
	filePutter(context.Context, string) (io.WriteCloser, error)

	// TODO tests only
	Dimensions(ctx context.Context) int // dim(vector)*number vectors
	// TODO tests only
	QuantizedDimensions(ctx context.Context, segments int) int
	extendDimensionTrackerLSM(dimLength int, docID uint64) error
	extendDimensionTrackerForVecLSM(dimLength int, docID uint64, vecName string) error
	publishDimensionMetrics(ctx context.Context)

	addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error
	pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair

	setFallbackToSearchable(fallback bool)
	addJobToQueue(job job)
	uuidFromDocID(docID uint64) (strfmt.UUID, error)
	batchDeleteObject(ctx context.Context, id strfmt.UUID) error
	putObjectLSM(object *storobj.Object, idBytes []byte) (objectInsertStatus, error)
	mayUpsertObjectHashTree(object *storobj.Object, idBytes []byte, status objectInsertStatus) error
	mutableMergeObjectLSM(merge objects.MergeDocument, idBytes []byte) (mutableMergeResult, error)
	deleteFromPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	batchExtendInvertedIndexItemsLSMNoFrequency(b *lsmkv.Bucket, item inverted.MergeItem) error
	updatePropertySpecificIndices(object *storobj.Object, status objectInsertStatus) error
	updateVectorIndexIgnoreDelete(vector []float32, status objectInsertStatus) error
	updateVectorIndexesIgnoreDelete(vectors map[string][]float32, status objectInsertStatus) error
	hasGeoIndex() bool

	Metrics() *Metrics

	// A thread-safe counter that goes up any time there is activity on this
	// shard. The absolute value has no meaning, it's only purpose is to compare
	// the previous value to the current value.
	Activity() int32
}

// Shard is the smallest completely-contained index unit. A shard manages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	index             *Index // a reference to the underlying index, which in turn contains schema information
	class             *models.Class
	queue             *IndexQueue
	queues            map[string]*IndexQueue
	name              string
	store             *lsmkv.Store
	counter           *indexcounter.Counter
	indexCheckpoints  *indexcheckpoint.Checkpoints
	vectorIndex       VectorIndex
	vectorIndexes     map[string]VectorIndex
	metrics           *Metrics
	promMetrics       *monitoring.PrometheusMetrics
	slowQueryReporter helpers.SlowQueryReporter
	propertyIndices   propertyspecific.Indices
	propLenTracker    *inverted.JsonShardMetaData
	versioner         *shardVersioner

	hashtree             hashtree.AggregatedHashTree
	hashtreeRWMux        sync.RWMutex
	hashtreeInitialized  atomic.Bool
	hashBeaterCtx        context.Context
	hashBeaterCancelFunc context.CancelFunc

	objectPropagationNeededCond *sync.Cond
	objectPropagationNeeded     bool

	lastComparedHosts    []string
	lastComparedHostsMux sync.RWMutex

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
	bitmapFactory  *roaringset.BitmapFactory

	activityTracker atomic.Int32

	// indicates whether shard is shut down or dropped (or ongoing)
	shut bool
	// indicates whether shard in being used at the moment (e.g. write request)
	inUseCounter atomic.Int64
	// allows concurrent shut read/write
	shutdownLock *sync.RWMutex
}

func NewShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	indexCheckpoints *indexcheckpoint.Checkpoints,
) (*Shard, error) {
	before := time.Now()
	var err error

	s := &Shard{
		index:       index,
		class:       class,
		name:        shardName,
		promMetrics: promMetrics,
		metrics: NewMetrics(index.logger, promMetrics,
			string(index.Config.ClassName), shardName),
		slowQueryReporter: helpers.NewSlowQueryReporterFromEnv(index.logger),
		stopMetrics:       make(chan struct{}),
		replicationMap:    pendingReplicaTasks{Tasks: make(map[string]replicaTask, 32)},
		centralJobQueue:   jobQueueCh,
		indexCheckpoints:  indexCheckpoints,

		shut:         false,
		shutdownLock: new(sync.RWMutex),

		status: storagestate.StatusLoading,
	}

	s.activityTracker.Store(1) // initial state
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

	mux := sync.Mutex{}
	s.objectPropagationNeededCond = sync.NewCond(&mux)

	if err := s.initNonVector(ctx, class); err != nil {
		return nil, errors.Wrapf(err, "init shard %q", s.ID())
	}

	if s.hasTargetVectors() {
		if err := s.initTargetVectors(ctx); err != nil {
			return nil, err
		}
		if err := s.initTargetQueues(); err != nil {
			return nil, err
		}
	} else {
		if err := s.initLegacyVector(ctx); err != nil {
			return nil, err
		}
		if err := s.initLegacyQueue(); err != nil {
			return nil, err
		}
	}

	s.initDimensionTracking(ctx)

	if asyncEnabled() {
		f := func() {
			// preload unindexed objects in the background
			if s.hasTargetVectors() {
				for targetVector, queue := range s.queues {
					err := queue.PreloadShard(s)
					if err != nil {
						queue.Logger.WithError(err).Errorf("preload shard for target vector: %s", targetVector)
					}
				}
			} else {
				err := s.queue.PreloadShard(s)
				if err != nil {
					s.queue.Logger.WithError(err).Error("preload shard")
				}
			}
		}
		enterrors.GoWrapper(f, s.index.logger)
	}
	s.NotifyReady()

	if exists {
		s.index.logger.Printf("Completed loading shard %s in %s", s.ID(), time.Since(before))
	} else {
		s.index.logger.Printf("Created shard %s in %s", s.ID(), time.Since(before))
	}

	return s, nil
}

func (s *Shard) hasTargetVectors() bool {
	return hasTargetVectors(s.index.vectorIndexUserConfig, s.index.vectorIndexUserConfigs)
}

// target vectors and legacy vector are (supposed to be) exclusive
// method allows to distinguish which of them is configured for the class
func hasTargetVectors(cfg schemaConfig.VectorIndexConfig, targetCfgs map[string]schemaConfig.VectorIndexConfig) bool {
	return len(targetCfgs) != 0
}

func (s *Shard) initTargetVectors(ctx context.Context) error {
	s.vectorIndexes = make(map[string]VectorIndex)
	for targetVector, vectorIndexConfig := range s.index.vectorIndexUserConfigs {
		vectorIndex, err := s.initVectorIndex(ctx, targetVector, vectorIndexConfig)
		if err != nil {
			return fmt.Errorf("cannot create vector index for %q: %w", targetVector, err)
		}
		s.vectorIndexes[targetVector] = vectorIndex
	}
	return nil
}

func (s *Shard) initTargetQueues() error {
	s.queues = make(map[string]*IndexQueue)
	for targetVector, vectorIndex := range s.vectorIndexes {
		queue, err := NewIndexQueue(s.ID(), targetVector, s, vectorIndex, s.centralJobQueue,
			s.indexCheckpoints, IndexQueueOptions{Logger: s.index.logger})
		if err != nil {
			return fmt.Errorf("cannot create index queue for %q: %w", targetVector, err)
		}
		s.queues[targetVector] = queue
	}
	return nil
}

func (s *Shard) initLegacyVector(ctx context.Context) error {
	vectorindex, err := s.initVectorIndex(ctx, "", s.index.vectorIndexUserConfig)
	if err != nil {
		return err
	}
	s.vectorIndex = vectorindex
	return nil
}

func (s *Shard) initLegacyQueue() error {
	queue, err := NewIndexQueue(s.ID(), "", s, s.vectorIndex, s.centralJobQueue,
		s.indexCheckpoints, IndexQueueOptions{Logger: s.index.logger})
	if err != nil {
		return err
	}
	s.queue = queue
	return nil
}

func (s *Shard) initVectorIndex(ctx context.Context,
	targetVector string, vectorIndexUserConfig schemaConfig.VectorIndexConfig,
) (VectorIndex, error) {
	var distProv distancer.Provider

	switch vectorIndexUserConfig.DistanceName() {
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
		return nil, fmt.Errorf("init vector index: %w",
			errors.Errorf("unrecognized distance metric %q,"+
				"choose one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]", vectorIndexUserConfig.DistanceName()))
	}

	var vectorIndex VectorIndex

	switch vectorIndexUserConfig.IndexType() {
	case vectorindex.VectorIndexTypeHNSW:
		hnswUserConfig, ok := vectorIndexUserConfig.(hnswent.UserConfig)
		if !ok {
			return nil, errors.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
				vectorIndexUserConfig)
		}

		if hnswUserConfig.Skip {
			vectorIndex = noop.NewIndex()
		} else {
			// starts vector cycles if vector is configured
			s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()
			s.index.cycleCallbacks.vectorTombstoneCleanupCycle.Start()

			// a shard can actually have multiple vector indexes:
			// - the main index, which is used for all normal object vectors
			// - a geo property index for each geo prop in the schema
			//
			// here we label the main vector index as such.
			vecIdxID := s.vectorIndexID(targetVector)

			vi, err := hnsw.New(hnsw.Config{
				Logger:               s.index.logger,
				RootPath:             s.path(),
				ID:                   vecIdxID,
				ShardName:            s.name,
				ClassName:            s.index.Config.ClassName.String(),
				PrometheusMetrics:    s.promMetrics,
				VectorForIDThunk:     hnsw.NewVectorForIDThunk(targetVector, s.vectorByIndexID),
				TempVectorForIDThunk: hnsw.NewTempVectorForIDThunk(targetVector, s.readVectorByIndexIDIntoSlice),
				DistanceProvider:     distProv,
				MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
					return hnsw.NewCommitLogger(s.path(), vecIdxID,
						s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks,
						hnsw.WithAllocChecker(s.index.allocChecker),
						hnsw.WithCommitlogThresholdForCombining(s.index.Config.HNSWMaxLogSize),
						// consistent with previous logic where the individual limit is 1/5 of the combined limit
						hnsw.WithCommitlogThreshold(s.index.Config.HNSWMaxLogSize/5),
					)
				},
				AllocChecker: s.index.allocChecker,
			}, hnswUserConfig, s.cycleCallbacks.vectorTombstoneCleanupCallbacks,
				s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks, s.store)
			if err != nil {
				return nil, errors.Wrapf(err, "init shard %q: hnsw index", s.ID())
			}
			vectorIndex = vi
		}
	case vectorindex.VectorIndexTypeFLAT:
		flatUserConfig, ok := vectorIndexUserConfig.(flatent.UserConfig)
		if !ok {
			return nil, errors.Errorf("flat vector index: config is not flat.UserConfig: %T",
				vectorIndexUserConfig)
		}
		s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()

		// a shard can actually have multiple vector indexes:
		// - the main index, which is used for all normal object vectors
		// - a geo property index for each geo prop in the schema
		//
		// here we label the main vector index as such.
		vecIdxID := s.vectorIndexID(targetVector)

		vi, err := flat.New(flat.Config{
			ID:               vecIdxID,
			TargetVector:     targetVector,
			Logger:           s.index.logger,
			DistanceProvider: distProv,
			AllocChecker:     s.index.allocChecker,
		}, flatUserConfig, s.store)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: flat index", s.ID())
		}
		vectorIndex = vi
	case vectorindex.VectorIndexTypeDYNAMIC:
		dynamicUserConfig, ok := vectorIndexUserConfig.(dynamicent.UserConfig)
		if !ok {
			return nil, errors.Errorf("dynamic vector index: config is not dynamic.UserConfig: %T",
				vectorIndexUserConfig)
		}
		s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()

		// a shard can actually have multiple vector indexes:
		// - the main index, which is used for all normal object vectors
		// - a geo property index for each geo prop in the schema
		//
		// here we label the main vector index as such.
		vecIdxID := s.vectorIndexID(targetVector)

		vi, err := dynamic.New(dynamic.Config{
			ID:                   vecIdxID,
			TargetVector:         targetVector,
			Logger:               s.index.logger,
			DistanceProvider:     distProv,
			RootPath:             s.path(),
			ShardName:            s.name,
			ClassName:            s.index.Config.ClassName.String(),
			PrometheusMetrics:    s.promMetrics,
			VectorForIDThunk:     hnsw.NewVectorForIDThunk(targetVector, s.vectorByIndexID),
			TempVectorForIDThunk: hnsw.NewTempVectorForIDThunk(targetVector, s.readVectorByIndexIDIntoSlice),
			MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
				return hnsw.NewCommitLogger(s.path(), vecIdxID,
					s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks)
			},
			TombstoneCallbacks:       s.cycleCallbacks.vectorTombstoneCleanupCallbacks,
			ShardCompactionCallbacks: s.cycleCallbacks.compactionCallbacks,
			ShardFlushCallbacks:      s.cycleCallbacks.flushCallbacks,
		}, dynamicUserConfig, s.store)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: dynamic index", s.ID())
		}
		vectorIndex = vi
	default:
		return nil, fmt.Errorf("Unknown vector index type: %q. Choose one from [\"%s\", \"%s\", \"%s\"]",
			vectorIndexUserConfig.IndexType(), vectorindex.VectorIndexTypeHNSW, vectorindex.VectorIndexTypeFLAT, vectorindex.VectorIndexTypeDYNAMIC)
	}
	defer vectorIndex.PostStartup()
	return vectorIndex, nil
}

func (s *Shard) initNonVector(ctx context.Context, class *models.Class) error {
	err := s.initLSMStore(ctx)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: shard db", s.ID())
	}

	if s.index.asyncReplicationEnabled() {
		err = s.initHashTree(ctx)
		if err != nil {
			return errors.Wrapf(err, "init shard %q: shard hashtree", s.ID())
		}
	} else if s.index.replicationEnabled() {
		s.index.logger.Infof("async replication disabled on shard %q", s.ID())
	}

	counter, err := indexcounter.New(s.path())
	if err != nil {
		return errors.Wrapf(err, "init shard %q: index counter", s.ID())
	}
	s.counter = counter
	s.bitmapFactory = roaringset.NewBitmapFactory(s.counter.Get, s.index.logger)

	dataPresent := s.counter.PreviewNext() != 0
	versionPath := path.Join(s.path(), "version")
	versioner, err := newShardVersioner(versionPath, dataPresent)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: check versions", s.ID())
	}
	s.versioner = versioner

	plPath := path.Join(s.path(), "proplengths")
	tracker, err := inverted.NewJsonShardMetaData(plPath, s.index.logger)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: prop length tracker", s.ID())
	}

	s.propLenTracker = tracker

	if err := s.initProperties(class); err != nil {
		return errors.Wrapf(err, "init shard %q: init per property indices", s.ID())
	}

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

func (s *Shard) vectorIndexID(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("vectors_%s", targetVector)
	}
	return "main"
}

func (s *Shard) pathHashTree() string {
	return path.Join(s.path(), "hashtree")
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

	opts := []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(2),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithKeepTombstones(true),
		s.dynamicMemtableSizing(),
		s.memtableDirtyConfig(),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
	}

	if s.metrics != nil && !s.metrics.grouped {
		// If metrics are grouped we cannot observe the count of an individual
		// shard's object store because there is just a single metric. We would
		// override it. See https://github.com/weaviate/weaviate/issues/4396 for
		// details.
		opts = append(opts, lsmkv.WithMonitorCount())
	}
	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM, opts...)
	if err != nil {
		return errors.Wrap(err, "create objects bucket")
	}

	s.store = store

	return nil
}

func (s *Shard) initHashTree(ctx context.Context) error {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)

	if bucket.GetSecondaryIndices() < 2 {
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Warn("secondary index for token ranges is not available")
		return nil
	}

	s.hashBeaterCtx, s.hashBeaterCancelFunc = context.WithCancel(context.Background())

	if err := os.MkdirAll(s.pathHashTree(), os.ModePerm); err != nil {
		return err
	}

	partitioningEnabled := s.index.shardState.PartitioningEnabled

	// load the most recent hashtree file
	dirEntries, err := os.ReadDir(s.pathHashTree())
	if err != nil {
		return err
	}

	for i := len(dirEntries) - 1; i >= 0; i-- {
		dirEntry := dirEntries[i]

		if dirEntry.IsDir() || filepath.Ext(dirEntry.Name()) != ".ht" {
			continue
		}

		hashtreeFilename := filepath.Join(s.pathHashTree(), dirEntry.Name())

		if s.hashtree != nil {
			err := os.Remove(hashtreeFilename)
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("deleting older hashtree file %q: %v", hashtreeFilename, err)
			continue
		}

		f, err := os.OpenFile(hashtreeFilename, os.O_RDONLY, os.ModePerm)
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("reading hashtree file %q: %v", hashtreeFilename, err)
			continue
		}

		// attempt to load hashtree from file
		if partitioningEnabled {
			s.hashtree, err = hashtree.DeserializeCompactHashTree(bufio.NewReader(f))
		} else {
			s.hashtree, err = hashtree.DeserializeMultiSegmentHashTree(bufio.NewReader(f))
		}
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("reading hashtree file %q: %v", hashtreeFilename, err)
		}

		f.Close()
		os.Remove(hashtreeFilename)
	}

	if s.hashtree != nil {
		s.hashtreeInitialized.Store(true)
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashtree successfully initialized")
		s.initHashBeater()
		return nil
	}

	if partitioningEnabled {
		s.hashtree, err = s.buildCompactHashTree()
	} else {
		s.hashtree, err = s.buildMultiSegmentHashTree()
	}
	if err != nil {
		return err
	}

	// sync hashtree with current object states

	enterrors.GoWrapper(func() {
		prevContextEvaluation := time.Now()

		objCount := 0

		err := bucket.IterateObjects(ctx, func(object *storobj.Object) error {
			if time.Since(prevContextEvaluation) > time.Second {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				prevContextEvaluation = time.Now()

				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("objectCount", objCount).
					Infof("hashtree initialization is progress...")
			}

			uuid, err := uuid.MustParse(object.ID().String()).MarshalBinary()
			if err != nil {
				return err
			}

			err = s.upsertObjectHashTree(object, uuid, objectInsertStatus{})
			if err != nil {
				return err
			}

			objCount++

			return nil
		})
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("iterating objects during hashtree initialization: %v", err)
			return
		}

		s.hashtreeInitialized.Store(true)

		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Info("hashtree successfully initialized")

		s.initHashBeater()
	}, s.index.logger)

	return nil
}

func (s *Shard) UpdateAsyncReplication(ctx context.Context, enabled bool) error {
	s.hashtreeRWMux.Lock()
	defer s.hashtreeRWMux.Unlock()

	if enabled {
		if s.hashtree != nil {
			return nil
		}

		err := s.initHashTree(ctx)
		if err != nil {
			return errors.Wrapf(err, "hashtree initialization on shard %q", s.ID())
		}

		return nil
	}

	if s.hashtree == nil {
		return nil
	}

	s.stopHashBeater()
	s.hashtree = nil
	s.hashtreeInitialized.Store(false)

	return nil
}

func (s *Shard) buildCompactHashTree() (hashtree.AggregatedHashTree, error) {
	return hashtree.NewCompactHashTree(math.MaxUint64, 16)
}

func (s *Shard) buildMultiSegmentHashTree() (hashtree.AggregatedHashTree, error) {
	shardState := s.index.shardState

	virtualNodes := make([]sharding.Virtual, len(shardState.Virtual))
	copy(virtualNodes, shardState.Virtual)

	sort.SliceStable(virtualNodes, func(a, b int) bool {
		return virtualNodes[a].Upper < virtualNodes[b].Upper
	})

	virtualNodesPos := make(map[string]int, len(virtualNodes))
	for i, v := range virtualNodes {
		virtualNodesPos[v.Name] = i
	}

	physical := shardState.Physical[s.name]

	segments := make([]hashtree.Segment, len(physical.OwnsVirtual))

	for i, v := range physical.OwnsVirtual {
		var segmentStart uint64
		var segmentSize uint64

		vi := virtualNodesPos[v]

		if vi == 0 {
			segmentStart = virtualNodes[len(virtualNodes)-1].Upper
			segmentSize = virtualNodes[0].Upper + (math.MaxUint64 - segmentStart)
		} else {
			segmentStart = virtualNodes[vi-1].Upper
			segmentSize = virtualNodes[vi].Upper - segmentStart
		}

		segments[i] = hashtree.NewSegment(segmentStart, segmentSize)
	}

	return hashtree.NewMultiSegmentHashTree(segments, 16)
}

func (s *Shard) closeHashTree() error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))

	hashtreeFilename := filepath.Join(s.pathHashTree(), fmt.Sprintf("hashtree-%x.ht", string(b[:])))

	f, err := os.OpenFile(hashtreeFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	_, err = s.hashtree.Serialize(w)
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
	}

	err = w.Flush()
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
	}

	return nil
}

func (s *Shard) HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error) {
	s.hashtreeRWMux.RLock()
	defer s.hashtreeRWMux.RUnlock()

	if !s.hashtreeInitialized.Load() {
		return nil, fmt.Errorf("hashtree not initialized on shard %q", s.ID())
	}

	// TODO (jeroiraz): reusable pool of digests slices
	digests = make([]hashtree.Digest, hashtree.LeavesCount(level+1))

	n, err := s.hashtree.Level(level, discriminant, digests)
	if err != nil {
		return nil, err
	}

	return digests[:n], nil
}

// IMPORTANT:
// Be advised there exists LazyLoadShard::drop() implementation intended
// to drop shard that was not loaded (instantiated) yet.
// It deletes shard by performing required actions and removing entire shard directory.
// If there is any action that needs to be performed beside files/dirs being removed
// from shard directory, it needs to be reflected as well in LazyLoadShard::drop()
// method to keep drop behaviour consistent.
func (s *Shard) drop() (err error) {
	s.metrics.DeleteShardLabels(s.index.Config.ClassName.String(), s.name)
	s.metrics.baseMetrics.StartUnloadingShard(s.index.Config.ClassName.String())
	s.replicationMap.clear()

	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
		// send 0 in when index gets dropped
		s.clearDimensionMetrics()
	}

	s.hashtreeRWMux.Lock()
	if s.hashtree != nil {
		s.stopHashBeater()
	}
	s.hashtreeRWMux.Unlock()

	ctx, cancel := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancel()

	// unregister all callbacks at once, in parallel
	if err = cyclemanager.NewCombinedCallbackCtrl(0, s.index.logger,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx); err != nil {
		return err
	}

	if err = s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	if _, err = os.Stat(s.pathLSM()); err == nil {
		err := os.RemoveAll(s.pathLSM())
		if err != nil {
			return errors.Wrapf(err, "remove lsm store at %s", s.pathLSM())
		}
	}
	// delete indexcount
	err = s.counter.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.path())
	}

	// delete version
	err = s.versioner.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove version at %s", s.path())
	}

	if s.hasTargetVectors() {
		// TODO run in parallel?
		for targetVector, queue := range s.queues {
			if err = queue.Drop(); err != nil {
				return fmt.Errorf("close queue of vector %q at %s: %w", targetVector, s.path(), err)
			}
		}
		for targetVector, vectorIndex := range s.vectorIndexes {
			if err = vectorIndex.Drop(ctx); err != nil {
				return fmt.Errorf("remove vector index of vector %q at %s: %w", targetVector, s.path(), err)
			}
		}
	} else {
		// delete queue cursor
		if err = s.queue.Drop(); err != nil {
			return errors.Wrapf(err, "close queue at %s", s.path())
		}
		// remove vector index
		if err = s.vectorIndex.Drop(ctx); err != nil {
			return errors.Wrapf(err, "remove vector index at %s", s.path())
		}
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
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
	)
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
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
	)
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
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
	)
}

func (s *Shard) addLastUpdateTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
	)
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

func (s *Shard) createPropertyIndex(ctx context.Context, eg *enterrors.ErrorGroupWrapper, props ...*models.Property) error {
	for _, prop := range props {
		if !inverted.HasInvertedIndex(prop) {
			continue
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

		if err := eg.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) createPropertyValueIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	bucketOpts := []lsmkv.BucketOption{
		s.memtableDirtyConfig(),
		s.dynamicMemtableSizing(),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
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
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
	)
}

func (s *Shard) createPropertyNullIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameNullLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
	)
}

func (s *Shard) UpdateVectorIndexConfig(ctx context.Context, updated schemaConfig.VectorIndexConfig) error {
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

func (s *Shard) UpdateVectorIndexConfigs(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}
	if err := s.UpdateStatus(storagestate.StatusReadOnly.String()); err != nil {
		return fmt.Errorf("attempt to mark read-only: %w", err)
	}

	wg := new(sync.WaitGroup)
	var err error
	for targetName, targetCfg := range updated {
		wg.Add(1)
		if err = s.VectorIndexForName(targetName).UpdateUserConfig(targetCfg, wg.Done); err != nil {
			break
		}
	}

	f := func() {
		wg.Wait()
		s.UpdateStatus(storagestate.StatusReady.String())
	}
	enterrors.GoWrapper(f, s.index.logger)

	return err
}

/*

	batch
		shut
		false
			in_use ++
			defer in_use --
		true
			fail request



	shutdown
		loop + time:
		if shut == true
			fail request
		in_use == 0 && shut == false
			shut = true



*/

func (s *Shard) Shutdown(ctx context.Context) (err error) {
	if err = s.waitForShutdown(ctx); err != nil {
		return
	}

	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
	}

	if err = s.GetPropertyLengthTracker().Close(); err != nil {
		return errors.Wrap(err, "close prop length tracker")
	}

	// unregister all callbacks at once, in parallel
	if err = cyclemanager.NewCombinedCallbackCtrl(0, s.index.logger,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx); err != nil {
		return err
	}

	s.hashtreeRWMux.Lock()
	if s.hashtree != nil {
		s.stopHashBeater()
		s.closeHashTree()
	}
	s.hashtreeRWMux.Unlock()

	if s.hasTargetVectors() {
		// TODO run in parallel?
		for targetVector, queue := range s.queues {
			if err = queue.Close(); err != nil {
				return fmt.Errorf("shut down vector index queue of vector %q: %w", targetVector, err)
			}
		}
		for targetVector, vectorIndex := range s.vectorIndexes {
			if err = vectorIndex.Flush(); err != nil {
				return fmt.Errorf("flush vector index commitlog of vector %q: %w", targetVector, err)
			}
			if err = vectorIndex.Shutdown(ctx); err != nil {
				return fmt.Errorf("shut down vector index of vector %q: %w", targetVector, err)
			}
		}
	} else {
		if err = s.queue.Close(); err != nil {
			return errors.Wrap(err, "shut down vector index queue")
		}
		// to ensure that all commitlog entries are written to disk.
		// otherwise in some cases the tombstone cleanup process'
		// 'RemoveTombstone' entry is not picked up on restarts
		// resulting in perpetually attempting to remove a tombstone
		// which doesn't actually exist anymore
		if err = s.vectorIndex.Flush(); err != nil {
			return errors.Wrap(err, "flush vector index commitlog")
		}
		if err = s.vectorIndex.Shutdown(ctx); err != nil {
			return errors.Wrap(err, "shut down vector index")
		}
	}

	if err = s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	return nil
}

func (s *Shard) preventShutdown() (release func(), err error) {
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()

	if s.shut {
		return func() {}, errAlreadyShutdown
	}

	s.inUseCounter.Add(1)
	return func() { s.inUseCounter.Add(-1) }, nil
}

func (s *Shard) waitForShutdown(ctx context.Context) error {
	checkInterval := 50 * time.Millisecond
	timeout := 30 * time.Second

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if eligible, err := s.checkEligibleForShutdown(); err != nil {
		return err
	} else if !eligible {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("Shard::proceedWithShutdown: %w", ctx.Err())
			case <-ticker.C:
				if eligible, err := s.checkEligibleForShutdown(); err != nil {
					return err
				} else if eligible {
					return nil
				}
			}
		}
	}
	return nil
}

// checks whether shutdown can be executed
// (shard is not in use at the moment)
func (s *Shard) checkEligibleForShutdown() (eligible bool, err error) {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shut {
		return false, errAlreadyShutdown
	}

	if s.inUseCounter.Load() == 0 {
		s.shut = true
		return true, nil
	}

	return false, nil
}

func (s *Shard) NotifyReady() {
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
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
	if s.index.shardState.PartitioningEnabled {
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

func bucketKeyPropertyLength(length int) ([]byte, error) {
	return inverted.LexicographicallySortableInt64(int64(length))
}

func bucketKeyPropertyNull(isNull bool) ([]byte, error) {
	if isNull {
		return []byte{uint8(filters.InternalNullState)}, nil
	}
	return []byte{uint8(filters.InternalNotNullState)}, nil
}

func (s *Shard) Activity() int32 {
	return s.activityTracker.Load()
}
