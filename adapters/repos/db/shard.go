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
	"sync"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
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
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const IdLockPoolSize = 128

var errAlreadyShutdown = errors.New("already shut or dropped")

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
	UpdateAsyncReplication(ctx context.Context, enabled bool) error
	AddReferencesBatch(ctx context.Context, refs objects.BatchReferences) []error
	DeleteObjectBatch(ctx context.Context, ids []strfmt.UUID, deletionTime time.Time, dryRun bool) objects.BatchSimpleObjects // Delete many objects by id
	DeleteObject(ctx context.Context, id strfmt.UUID, deletionTime time.Time) error                                           // Delete object by id
	MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error)
	ObjectDigestsByTokenRange(ctx context.Context, initialToken, finalToken uint64, limit int) (objs []replica.RepairResponse, lastTokenRead uint64, err error)
	ID() string // Get the shard id
	drop() error
	HaltForTransfer(ctx context.Context) error
	initPropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper, props ...*models.Property)
	ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error
	resumeMaintenanceCycles(ctx context.Context) error
	SetPropertyLengths(props []inverted.Property) error
	AnalyzeObject(*storobj.Object) ([]inverted.Property, []inverted.NilProperty, error)
	Aggregate(ctx context.Context, params aggregation.Params, modules *modules.Provider) (*aggregation.Result, error)
	HashTreeLevel(ctx context.Context, level int, discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)
	MergeObject(ctx context.Context, object objects.MergeDocument) error
	Queue() *VectorIndexQueue
	Queues() map[string]*VectorIndexQueue
	VectorDistanceForQuery(ctx context.Context, id uint64, searchVectors []models.Vector, targets []string) ([]float32, error)
	ConvertQueue(targetVector string) error
	FillQueue(targetVector string, from uint64) error
	Shutdown(context.Context) error // Shutdown the shard
	preventShutdown() (release func(), err error)

	// TODO tests only
	ObjectList(ctx context.Context, limit int, sort []filters.Sort, cursor *filters.Cursor,
		additional additional.Properties, className schema.ClassName) ([]*storobj.Object, error) // Search and return objects
	WasDeleted(ctx context.Context, id strfmt.UUID) (bool, time.Time, error) // Check if an object was deleted
	VectorIndex() VectorIndex                                                // Get the vector index
	VectorIndexes() map[string]VectorIndex                                   // Get the vector indexes
	hasTargetVectors() bool
	// TODO tests only
	Versioner() *shardVersioner // Get the shard versioner

	isReadOnly() error

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
	Dimensions(ctx context.Context) int // dim(vector)*number vectors
	// TODO tests only
	QuantizedDimensions(ctx context.Context, segments int) int
	extendDimensionTrackerLSM(dimLength int, docID uint64) error
	extendDimensionTrackerForVecLSM(dimLength int, docID uint64, vecName string) error
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

	Metrics() *Metrics

	// A thread-safe counter that goes up any time there is activity on this
	// shard. The absolute value has no meaning, it's only purpose is to compare
	// the previous value to the current value.
	Activity() int32
	// Debug methods
	DebugResetVectorIndex(ctx context.Context, targetVector string) error
	RepairIndex(ctx context.Context, targetVector string) error
}

// Shard is the smallest completely-contained index unit. A shard manages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	index             *Index // a reference to the underlying index, which in turn contains schema information
	class             *models.Class
	queue             *VectorIndexQueue
	queues            map[string]*VectorIndexQueue
	scheduler         *queue.Scheduler
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

	status              ShardStatus
	statusLock          sync.Mutex
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

	activityTracker atomic.Int32

	// indicates whether shard is shut down or dropped (or ongoing)
	shut bool
	// indicates whether shard in being used at the moment (e.g. write request)
	inUseCounter atomic.Int64
	// allows concurrent shut read/write
	shutdownLock *sync.RWMutex
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

func (s *Shard) initHashTree() error {
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
		ht, err := hashtree.DeserializeCompactHashTree(bufio.NewReader(f))
		if err != nil {
			s.index.logger.
				WithField("action", "async_replication").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Warnf("reading hashtree file %q: %v", hashtreeFilename, err)
		} else {
			s.hashtree = ht
		}

		err = f.Close()
		if err != nil {
			return err
		}

		err = os.Remove(hashtreeFilename)
		if err != nil {
			return err
		}
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

	ht, err := s.buildCompactHashTree()
	if err != nil {
		return err
	}
	s.hashtree = ht

	// sync hashtree with current object states

	enterrors.GoWrapper(func() {
		prevContextEvaluation := time.Now()

		objCount := 0

		// data inserted before v1.26 does not contain the required secondary index
		// to support async replication thus such data is not inserted into the hashtree
		err := bucket.IterateObjectsWith(s.hashBeaterCtx, 2, func(object *storobj.Object) error {
			if time.Since(prevContextEvaluation) > time.Second {
				if s.hashBeaterCtx.Err() != nil {
					return s.hashBeaterCtx.Err()
				}

				prevContextEvaluation = time.Now()

				s.index.logger.
					WithField("action", "async_replication").
					WithField("class_name", s.class.Class).
					WithField("shard_name", s.name).
					WithField("object_count", objCount).
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

func (s *Shard) mayCloseAndStoreHashTree() {
	s.hashtreeRWMux.Lock()
	if s.hashtreeInitialized.Load() {
		// the hashtree needs to be fully in sync with stored data before it can be persisted
		s.closeAndStoreHashTree()
	}
	s.hashtreeRWMux.Unlock()
}

func (s *Shard) mayCleanupHashTree() {
	s.hashtreeRWMux.Lock()
	if s.hashtree != nil {
		s.cleanupHashTree()
	}
	s.hashtreeRWMux.Unlock()
}

func (s *Shard) cleanupHashTree() {
	s.hashtree = nil
	s.hashtreeInitialized.Store(false)
}

func (s *Shard) UpdateAsyncReplication(ctx context.Context, enabled bool) error {
	s.hashtreeRWMux.Lock()
	defer s.hashtreeRWMux.Unlock()

	if enabled {
		if s.hashtree == nil {
			err := s.initHashTree()
			if err != nil {
				return errors.Wrapf(err, "hashtree initialization on shard %q", s.ID())
			}
			return nil
		}

		if s.hashBeaterCtx == nil || s.hashBeaterCtx.Err() != nil {
			s.hashBeaterCtx, s.hashBeaterCancelFunc = context.WithCancel(context.Background())
			s.initHashBeater()
		}

		return nil
	}

	if s.hashtree == nil {
		return nil
	}

	s.stopHashBeater()
	s.cleanupHashTree()

	return nil
}

func (s *Shard) buildCompactHashTree() (hashtree.AggregatedHashTree, error) {
	return hashtree.NewCompactHashTree(math.MaxUint64, 16)
}

func (s *Shard) closeAndStoreHashTree() error {
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

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("storing hashtree in %q: %w", hashtreeFilename, err)
	}

	s.hashtree = nil
	s.hashtreeInitialized.Store(false)

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

	return s.VectorIndex().UpdateUserConfig(updated, func() {
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
