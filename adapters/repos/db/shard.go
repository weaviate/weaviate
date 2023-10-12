//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/docid"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"golang.org/x/sync/errgroup"
)

const IdLockPoolSize = 128

// Shard is the smallest completely-contained index unit. A shard manages
// database files for all the objects it owns. How a shard is determined for a
// target object (e.g. Murmur hash, etc.) is still open at this point
type Shard struct {
	index           *Index // a reference to the underlying index, which in turn contains schema information
	queue           *IndexQueue
	name            string
	store           *lsmkv.Store
	counter         *indexcounter.Counter
	vectorIndex     VectorIndex
	metrics         *Metrics
	promMetrics     *monitoring.PrometheusMetrics
	propertyIndices propertyspecific.Indices
	deletedDocIDs   *docid.InMemDeletedTracker
	propLengths     *inverted.JsonPropertyLengthTracker
	versioner       *shardVersioner

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

func NewShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
) (*Shard, error) {
	before := time.Now()

	s := &Shard{
		index:       index,
		name:        shardName,
		promMetrics: promMetrics,
		metrics: NewMetrics(index.logger, promMetrics,
			string(index.Config.ClassName), shardName),
		deletedDocIDs:   docid.NewInMemDeletedTracker(),
		stopMetrics:     make(chan struct{}),
		replicationMap:  pendingReplicaTasks{Tasks: make(map[string]replicaTask, 32)},
		centralJobQueue: jobQueueCh,
	}
	s.initCycleCallbacks()

	s.docIdLock = make([]sync.Mutex, IdLockPoolSize)

	defer s.metrics.ShardStartup(before)

	hnswUserConfig, ok := index.vectorIndexUserConfig.(hnswent.UserConfig)
	if !ok {
		return nil, errors.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
			index.vectorIndexUserConfig)
	}

	if hnswUserConfig.Skip {
		s.vectorIndex = noop.NewIndex()
	} else {
		if err := s.initVectorIndex(ctx, hnswUserConfig); err != nil {
			return nil, fmt.Errorf("init vector index: %w", err)
		}

		defer s.vectorIndex.PostStartup()
	}

	if err := s.initNonVector(ctx, class); err != nil {
		return nil, errors.Wrapf(err, "init shard %q", s.ID())
	}

	var err error
	s.queue, err = NewIndexQueue(s, s.vectorIndex, s.centralJobQueue, IndexQueueOptions{
		Logger: s.index.logger,
	})
	if err != nil {
		return nil, err
	}

	if asyncEnabled() {
		// load non-indexed vectors and add them to the queue
		start := time.Now()
		total := 0
		nonIndexed := 0

		cursor := s.store.Bucket(helpers.ObjectsBucketLSM).Cursor()

		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			obj, err := storobj.FromBinary(v)
			if err != nil {
				return nil, err
			}
			total++
			id := obj.DocID()
			if s.vectorIndex.ContainsNode(id) {
				continue
			}
			if len(obj.Vector) == 0 {
				continue
			}

			nonIndexed++
			desc := vectorDescriptor{
				id:     id,
				vector: obj.Vector,
			}
			s.queue.Push(context.Background(), desc)
		}
		cursor.Close()
		s.index.logger.WithField("non-indexed", nonIndexed).WithField("total", total).Debugf("loaded non-indexed vectors in %s", time.Since(start))
	}

	return s, nil
}

func (s *Shard) initVectorIndex(
	ctx context.Context, hnswUserConfig hnswent.UserConfig,
) error {
	var distProv distancer.Provider

	switch hnswUserConfig.Distance {
	case "", hnswent.DistanceCosine:
		distProv = distancer.NewCosineDistanceProvider()
	case hnswent.DistanceDot:
		distProv = distancer.NewDotProductProvider()
	case hnswent.DistanceL2Squared:
		distProv = distancer.NewL2SquaredProvider()
	case hnswent.DistanceManhattan:
		distProv = distancer.NewManhattanProvider()
	case hnswent.DistanceHamming:
		distProv = distancer.NewHammingProvider()
	default:
		return errors.Errorf("unrecognized distance metric %q,"+
			"choose one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]", hnswUserConfig.Distance)
	}

	// starts vector cycles if vector is configured
	s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()
	s.index.cycleCallbacks.vectorTombstoneCleanupCycle.Start()

	vi, err := hnsw.New(hnsw.Config{
		Logger:               s.index.logger,
		RootPath:             s.index.Config.RootPath,
		ID:                   s.ID(),
		ShardName:            s.name,
		ClassName:            s.index.Config.ClassName.String(),
		PrometheusMetrics:    s.promMetrics,
		VectorForIDThunk:     s.vectorByIndexID,
		TempVectorForIDThunk: s.readVectorByIndexIDIntoSlice,
		DistanceProvider:     distProv,
		MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(s.index.Config.RootPath, s.ID(),
				s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks)
		},
	}, hnswUserConfig,
		s.cycleCallbacks.vectorTombstoneCleanupCallbacks, s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: hnsw index", s.ID())
	}
	s.vectorIndex = vi

	return nil
}

func (s *Shard) initNonVector(ctx context.Context, class *models.Class) error {
	err := s.initLSMStore(ctx)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: shard db", s.ID())
	}

	counter, err := indexcounter.New(s.ID(), s.index.Config.RootPath)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: index counter", s.ID())
	}
	s.counter = counter

	dataPresent := s.counter.PreviewNext() != 0
	versionPath := path.Join(s.index.Config.RootPath, s.ID()+".version")
	versioner, err := newShardVersioner(versionPath, dataPresent)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: check versions", s.ID())
	}
	s.versioner = versioner

	plPath := path.Join(s.index.Config.RootPath, s.ID()+".proplengths")
	propLengths, err := inverted.NewJsonPropertyLengthTracker(plPath, s.index.logger)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: prop length tracker", s.ID())
	}
	s.propLengths = propLengths

	if err := s.initProperties(class); err != nil {
		return errors.Wrapf(err, "init shard %q: init per property indices", s.ID())
	}

	s.initDimensionTracking()

	return nil
}

func (s *Shard) ID() string {
	return fmt.Sprintf("%s_%s", s.index.ID(), s.name)
}

func (s *Shard) DBPathLSM() string {
	return fmt.Sprintf("%s/%s_lsm", s.index.Config.RootPath, s.ID())
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

	store, err := lsmkv.New(s.DBPathLSM(), s.index.Config.RootPath, annotatedLogger, metrics,
		s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks)
	if err != nil {
		return errors.Wrapf(err, "init lsmkv store at %s", s.DBPathLSM())
	}

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithMonitorCount(),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		s.dynamicMemtableSizing(),
		s.memtableIdleConfig(),
	)
	if err != nil {
		return errors.Wrap(err, "create objects bucket")
	}

	s.store = store

	return nil
}

func (s *Shard) drop() error {
	s.replicationMap.clear()

	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
		if s.promMetrics != nil {
			// send 0 in when index gets dropped
			s.sendVectorDimensionsMetric(0)
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

	if _, err := os.Stat(s.DBPathLSM()); err == nil {
		err := os.RemoveAll(s.DBPathLSM())
		if err != nil {
			return errors.Wrapf(err, "remove lsm store at %s", s.DBPathLSM())
		}
	}
	// delete indexcount
	err := s.counter.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.DBPathLSM())
	}

	// delete indexcount
	err = s.versioner.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.DBPathLSM())
	}
	// remove vector index
	err = s.vectorIndex.Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "remove vector index at %s", s.DBPathLSM())
	}

	// delete indexcount
	err = s.propLengths.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove prop length tracker at %s", s.DBPathLSM())
	}

	// TODO: can we remove this?
	s.deletedDocIDs.BulkRemove(s.deletedDocIDs.GetAll())
	s.propertyIndicesLock.Lock()
	err = s.propertyIndices.DropAll(ctx)
	s.propertyIndicesLock.Unlock()
	if err != nil {
		return errors.Wrapf(err, "remove property specific indices at %s", s.DBPathLSM())
	}

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

func (s *Shard) updateVectorIndexConfig(ctx context.Context,
	updated schema.VectorIndexConfig,
) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	err := s.updateStatus(storagestate.StatusReadOnly.String())
	if err != nil {
		return fmt.Errorf("attempt to mark read-only: %w", err)
	}
	return s.vectorIndex.UpdateUserConfig(updated, func() {
		s.updateStatus(storagestate.StatusReady.String())
	})
}

func (s *Shard) shutdown(ctx context.Context) error {
	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
	}

	if err := s.propLengths.Close(); err != nil {
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
	if err := s.vectorIndex.Flush(); err != nil {
		return errors.Wrap(err, "flush vector index commitlog")
	}

	if err := s.vectorIndex.Shutdown(ctx); err != nil {
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

func (s *Shard) notifyReady() {
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}

func (s *Shard) objectCount() int {
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return 0
	}

	return b.Count()
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
