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
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func NewShard_unmerged(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	indexCheckpoints *indexcheckpoint.Checkpoints,
) (*Shard, error) {
	before := time.Now()
	var err error
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

		shut:         false,
		shutdownLock: new(sync.RWMutex),
	}
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

	s.initDimensionTracking()

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

func (s *Shard) hasTargetVectors_unmerged() bool {
	return hasTargetVectors(s.index.vectorIndexUserConfig, s.index.vectorIndexUserConfigs)
}

// target vectors and legacy vector are (supposed to be) exclusive
// method allows to distinguish which of them is configured for the class
func hasTargetVectors_unmerged(cfg schemaConfig.VectorIndexConfig, targetCfgs map[string]schemaConfig.VectorIndexConfig) bool {
	return len(targetCfgs) != 0
}

func (s *Shard) initTargetVectors_unmerged(ctx context.Context) error {
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

func (s *Shard) initTargetQueues_unmerged() error {
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

func (s *Shard) initLegacyVector_unmerged(ctx context.Context) error {
	vectorindex, err := s.initVectorIndex(ctx, "", s.index.vectorIndexUserConfig)
	if err != nil {
		return err
	}
	s.vectorIndex = vectorindex
	return nil
}

func (s *Shard) initLegacyQueue_unmerged() error {
	queue, err := NewIndexQueue(s.ID(), "", s, s.vectorIndex, s.centralJobQueue,
		s.indexCheckpoints, IndexQueueOptions{Logger: s.index.logger})
	if err != nil {
		return err
	}
	s.queue = queue
	return nil
}

func (s *Shard) initVectorIndex_unmerged(ctx context.Context,
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
				VectorForIDThunk:     s.vectorByIndexID,
				TempVectorForIDThunk: s.readVectorByIndexIDIntoSlice,
				DistanceProvider:     distProv,
				MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
					return hnsw.NewCommitLogger(s.path(), vecIdxID,
						s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks,
						hnsw.WithAllocChecker(s.index.allocChecker))
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
	default:
		return nil, fmt.Errorf("Unknown vector index type: %q. Choose one from [\"%s\", \"%s\"]",
			vectorIndexUserConfig.IndexType(), vectorindex.VectorIndexTypeHNSW, vectorindex.VectorIndexTypeFLAT)
	}
	defer vectorIndex.PostStartup()
	return vectorIndex, nil
}

func (s *Shard) initNonVector_unmerged(ctx context.Context, class *models.Class) error {
	err := s.initLSMStore_unmerged(ctx)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: shard db", s.ID())
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
	tracker, err := inverted.NewJsonPropertyLengthTracker(plPath, s.index.logger)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: prop length tracker", s.ID())
	}

	s.propLenTracker = tracker

	if err := s.initProperties_unmerged(class); err != nil {
		return errors.Wrapf(err, "init shard %q: init per property indices", s.ID())
	}

	return nil
}

func (s *Shard) ID_unmerged() string {
	return shardId(s.index.ID(), s.name)
}

func (s *Shard) path_unmerged() string {
	return shardPath(s.index.path(), s.name)
}

func (s *Shard) pathLSM_unmerged() string {
	return path.Join(s.path(), "lsm")
}

func (s *Shard) vectorIndexID_unmerged(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("vectors_%s", targetVector)
	}
	return "main"
}

func (s *Shard) UuidToIdLockPoolId_unmerged(idBytes []byte) uint8 {
	// use the last byte of the uuid to determine which locking-pool a given object should use. The last byte is used
	// as uuids probably often have some kind of order and the last byte will in general be the one that changes the most
	return idBytes[15] % IdLockPoolSize
}

func (s *Shard) initLSMStore_unmerged(ctx context.Context) error {
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
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithKeepTombstones(true),
		s.dynamicMemtableSizing_unmerged(),
		s.memtableDirtyConfig_unmerged(),
		lsmkv.WithAllocChecker(s.index.allocChecker),
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

// IMPORTANT:
// Be advised there exists LazyLoadShard::drop() implementation intended
// to drop shard that was not loaded (instantiated) yet.
// It deletes shard by performing required actions and removing entire shard directory.
// If there is any action that needs to be performed beside files/dirs being removed
// from shard directory, it needs to be reflected as well in LazyLoadShard::drop()
// method to keep drop behaviour consistent.
func (s *Shard) Drop_unmerged() error {
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

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	// unregister all callbacks at once, in parallel
	if err := cyclemanager.NewCombinedCallbackCtrl(0, s.index.logger,
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

func (s *Shard) addIDProperty_unmerged(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropertyNameLSM(filters.InternalPropID),
		s.memtableDirtyConfig_unmerged(),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
	)
}

func (s *Shard) addDimensionsProperty_unmerged(ctx context.Context) error {
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
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addTimestampProperties_unmerged(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if err := s.addCreationTimeUnixProperty_unmerged(ctx); err != nil {
		return err
	}
	if err := s.addLastUpdateTimeUnixProperty_unmerged(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Shard) addCreationTimeUnixProperty_unmerged(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropertyNameLSM(filters.InternalPropCreationTimeUnix),
		s.memtableDirtyConfig_unmerged(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
	)
}

func (s *Shard) addLastUpdateTimeUnixProperty_unmerged(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropertyNameLSM(filters.InternalPropLastUpdateTimeUnix),
		s.memtableDirtyConfig_unmerged(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
	)
}

func (s *Shard) memtableDirtyConfig_unmerged() lsmkv.BucketOption {
	return lsmkv.WithDirtyThreshold(
		time.Duration(s.index.Config.MemtablesFlushDirtyAfter) * time.Second)
}

func (s *Shard) dynamicMemtableSizing_unmerged() lsmkv.BucketOption {
	return lsmkv.WithDynamicMemtableSizing(
		s.index.Config.MemtablesInitialSizeMB,
		s.index.Config.MemtablesMaxSizeMB,
		s.index.Config.MemtablesMinActiveSeconds,
		s.index.Config.MemtablesMaxActiveSeconds,
	)
}

func (s *Shard) CreatePropertyIndex_unmerged(ctx context.Context, eg *enterrors.ErrorGroupWrapper, props []*models.Property) error {
	for _, prop := range props {
		if !inverted.HasInvertedIndex(prop) {
			continue
		}

		eg.Go(func() error {
			if err := s.createPropertyValueIndex_unmerged(ctx, prop); err != nil {
				return errors.Wrapf(err, "create property '%s' value index on shard '%s'", prop.Name, s.ID())
			}

			if s.index.invertedIndexConfig.IndexNullState {
				eg.Go(func() error {
					if err := s.createPropertyNullIndex_unmerged(ctx, prop); err != nil {
						return errors.Wrapf(err, "create property '%s' null index on shard '%s'", prop.Name, s.ID())
					}
					return nil
				})
			}

			if s.index.invertedIndexConfig.IndexPropertyLength {
				eg.Go(func() error {
					if err := s.createPropertyLengthIndex_unmerged(ctx, prop); err != nil {
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

func (s *Shard) createPropertyValueIndex_unmerged(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	bucketOpts := []lsmkv.BucketOption{
		s.memtableDirtyConfig_unmerged(),
		s.dynamicMemtableSizing_unmerged(),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
	}

	if inverted.HasFilterableIndex(prop) {
		if dt, _ := schema.AsPrimitive(prop.DataType); dt == schema.DataTypeGeoCoordinates {
			return s.initGeoProp(prop)
		}

		if schema.IsRefDataType(prop.DataType) {
			if err := s.store.CreateOrLoadBucket(ctx,
				helpers.BucketFromPropertyNameMetaCountLSM(prop.Name),
				append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))...,
			); err != nil {
				return err
			}
		}

		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketFromPropertyNameLSM(prop.Name),
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
			helpers.BucketSearchableFromPropertyNameLSM(prop.Name),
			searchableBucketOpts...,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) createPropertyLengthIndex_unmerged(ctx context.Context, prop *models.Property) error {
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
		helpers.BucketFromPropertyNameLengthLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
	)
}

func (s *Shard) createPropertyNullIndex_unmerged(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropertyNameNullLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
	)
}

func (s *Shard) UpdateVectorIndexConfig_unmerged(ctx context.Context, updated schemaConfig.VectorIndexConfig) error {
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

func (s *Shard) UpdateVectorIndexConfigs_unmerged(ctx context.Context, updated map[string]schemaConfig.VectorIndexConfig) error {
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

func (s *Shard) Shutdown_unmerged(ctx context.Context) error {
	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
	}

	var err error
	if err = s.GetPropertyLengthTracker().Close(); err != nil {
		return errors.Wrap(err, "close prop length tracker")
	}

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

	return nil
}

func (s *Shard) NotifyReady_unmerged() {
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}

// ObjectCount returns the exact count at any moment
func (s *Shard) ObjectCount_unmerged() int {
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return 0
	}

	return b.Count()
}

// ObjectCountAsync returns the eventually consistent "async" count which is
// much cheaper to obtain
func (s *Shard) ObjectCountAsync_unmerged() int {
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return 0
	}

	return b.CountAsync()
}

func (s *Shard) IsFallbackToSearchable_unmerged() bool {
	return s.fallbackToSearchable
}

func (s *Shard) Tenant_unmerged() string {
	// TODO provide better impl
	if s.index.partitioningEnabled {
		return s.name
	}
	return ""
}

func shardId_unmerged(indexId, shardName string) string {
	return fmt.Sprintf("%s_%s", indexId, shardName)
}

func shardPath_unmerged(indexPath, shardName string) string {
	return path.Join(indexPath, shardName)
}

func bucketKeyPropertyLength_unmerged(length int) ([]byte, error) {
	return inverted.LexicographicallySortableInt64(int64(length))
}

func bucketKeyPropertyNull_unmerged(isNull bool) ([]byte, error) {
	if isNull {
		return []byte{uint8(filters.InternalNullState)}, nil
	}
	return []byte{uint8(filters.InternalNotNullState)}, nil
}
