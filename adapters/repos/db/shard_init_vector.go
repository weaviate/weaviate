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
	stderrors "errors"
	"fmt"
	"maps"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	vcommon "github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	"github.com/weaviate/weaviate/entities/modelsext"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hfreshent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// vectorFromObjectForTarget reads targetVector's vector straight out of an
// object's stored bytes, for the startup prefill's parallel scan of the
// objects bucket. An object without that vector is skipped. The shard binds
// this like every other object read, so the index never learns its name.
func vectorFromObjectForTarget(targetVector string) hnsw.VectorFromObject {
	return func(objectBytes []byte) ([]float32, error) {
		vec, err := storobj.VectorFromBinary(objectBytes, nil, targetVector)
		if err != nil {
			var notFound storobj.ErrTargetVectorNotFound
			if stderrors.As(err, &notFound) || stderrors.Is(err, storobj.ErrNoVector) {
				return nil, nil
			}
			return nil, err
		}
		return vec, nil
	}
}

func (s *Shard) initShardVectors(ctx context.Context) error {
	// Snapshot under the index config lock: updateVectorIndexConfig(s) mutate
	// these concurrently, and ranging the live map is a fatal
	// "concurrent map read and map write", not a recoverable race.
	legacy := s.index.GetVectorIndexConfig("")
	targets := s.index.getTargetVectorIndexConfigs()
	// Read after the configs: a drop marks the schema before it deletes the
	// config, so one missing above because of a drop is marked here, and one
	// still above is about to be deleted.
	dropped := s.droppedVectorNames()
	for _, name := range dropped {
		delete(targets, name)
	}
	active, skipped := vectorIndexConfigsByStorage(legacy, targets)

	err := newCompressedVectorsMigrator(s.index.logger).removeVectorsCompressedLink(s.store.GetDir())
	if err != nil {
		return fmt.Errorf("shard %q: %w", s.ID(), err)
	}

	// The manifest says where each index lives; nothing opens before it is in order.
	entries, firstLoad, err := s.manifest.Open(ctx, active, dropped)
	if err != nil {
		return fmt.Errorf("shard %q: %w", s.ID(), err)
	}

	// Only a shard without a mapping can still hold the pre-1.30.19 layout: a
	// failed repair fails the load before the mapping is written, so the next
	// load retries it. 1.40 only logged a failed repair and wrote the mapping;
	// such a shard keeps the old layout and is not repaired again (accepted).
	if firstLoad {
		err := newCompressedVectorsMigrator(s.index.logger).do(s, entries)
		if err != nil {
			return fmt.Errorf("shard %q: migrate compressed vectors: %w", s.ID(), err)
		}
	}

	for _, e := range entries {
		err := s.createVectorIndex(ctx, e.Name, e.StorageID, e.QueueID, e.Config, s.lazySegmentLoadingEnabled)
		if err != nil {
			return err
		}
	}
	// a skipped vector owns no files and no record, but callers need its slot
	for name, cfg := range skipped {
		id := s.vectorIndexID(name)
		err := s.createVectorIndex(ctx, name, id, id, cfg, s.lazySegmentLoadingEnabled)
		if err != nil {
			return err
		}
	}

	err = s.manifest.Ready(active)
	if err != nil {
		return fmt.Errorf("shard %q: %w", s.ID(), err)
	}
	return nil
}

// droppedVectorNames lists the vectors the class marks as being dropped.
func (s *Shard) droppedVectorNames() []string {
	class := s.index.getClass()
	if class == nil {
		return nil
	}
	var names []string
	for name, cfg := range class.VectorConfig {
		if modelsext.IsVectorIndexDropped(cfg) {
			names = append(names, name)
		}
	}
	return names
}

// vectorIndexConfigsByStorage splits the schema's vectors, the legacy one
// under the empty name, into those that own storage and the skipped ones.
func vectorIndexConfigsByStorage(legacy schemaConfig.VectorIndexConfig,
	targets map[string]schemaConfig.VectorIndexConfig,
) (active, skipped map[string]schemaConfig.VectorIndexConfig) {
	active = make(map[string]schemaConfig.VectorIndexConfig, len(targets)+1)
	skipped = make(map[string]schemaConfig.VectorIndexConfig)
	if legacy != nil {
		targets = maps.Clone(targets)
		targets[""] = legacy
	}
	for name, cfg := range targets {
		if vectorIndexHasStorage(cfg) {
			active[name] = cfg
		} else {
			skipped[name] = cfg
		}
	}
	return active, skipped
}

// vectorIndexHasStorage is false for a skipped hnsw config: a no-op index
// owns no files, so the mapping does not record it.
func vectorIndexHasStorage(cfg schemaConfig.VectorIndexConfig) bool {
	hnswCfg, ok := cfg.(hnswent.UserConfig)
	return !ok || !hnswCfg.Skip
}

// vectorIndexLogger identifies every log line under one vector index: the
// logical name for operators, the physical id for storage. The index, its
// queue and everything they construct inherit it.
func (s *Shard) vectorIndexLogger(targetVector, indexID string) logrus.FieldLogger {
	return s.index.logger.WithFields(logrus.Fields{
		"class":         s.index.Config.ClassName.String(),
		"shard":         s.name,
		"target_vector": targetVector,
		"index_id":      indexID,
	})
}

func (s *Shard) initVectorIndex(ctx context.Context,
	targetVector, physicalID string, vectorIndexUserConfig schemaConfig.VectorIndexConfig, lazyLoadSegments bool,
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

	makeBucketOptions := s.makeDefaultBucketOptions
	if lazyLoadSegments != s.lazySegmentLoadingEnabled {
		makeBucketOptions = s.overwrittenMakeDefaultBucketOptions(lsmkv.WithLazySegmentLoading(lazyLoadSegments))
	}

	// Every log line under this index carries both identities: the logical
	// name for operators ("which vector?") and the physical id for storage
	// ("which files?"). Implementations and the entities they own (compressors,
	// commit loggers, queues) inherit it and add nothing of their own.
	logger := s.vectorIndexLogger(targetVector, physicalID)

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

			vi, err := hnsw.New(hnsw.Config{
				Logger:                            logger,
				RootPath:                          s.path(),
				ID:                                physicalID,
				ShardName:                         s.name,
				ClassName:                         s.index.Config.ClassName.String(),
				PrometheusMetrics:                 s.promMetrics,
				VectorForIDThunk:                  hnsw.NewVectorForIDThunk(targetVector, s.vectorByIndexID),
				VectorFromObject:                  vectorFromObjectForTarget(targetVector),
				MultiVectorForIDThunk:             hnsw.NewVectorForIDThunk(targetVector, s.multiVectorByIndexID),
				TempMultiVectorForIDThunk:         hnsw.NewTempMultiVectorForIDThunk(targetVector, s.readMultiVectorByIndexIDIntoSlice),
				GetViewThunk:                      func() vcommon.BucketView { return s.GetObjectsBucketView() },
				TempVectorForIDWithViewThunk:      hnsw.NewTempVectorForIDWithViewThunk(targetVector, s.readVectorByIndexIDIntoSliceWithView),
				TempMultiVectorForIDWithViewThunk: hnsw.NewTempVectorForIDWithViewThunk(targetVector, s.readMultiVectorByIndexIDIntoSliceWithView),
				DistanceProvider:                  distProv,
				MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
					// Prepend our default options, caller's opts take precedence
					allOpts := append([]hnsw.CommitlogOption{
						// consistent with previous logic where the individual limit is 1/5 of the combined limit
						hnsw.WithCommitlogThreshold(s.index.Config.HNSWMaxLogSize / 5),
					}, opts...)
					return hnsw.NewCommitLogger(s.path(), physicalID,
						logger, s.cycleCallbacks.vectorCommitLoggerCallbacks,
						allOpts...,
					)
				},
				AllocChecker:           s.index.allocChecker,
				WaitForCachePrefill:    s.index.Config.HNSWWaitForCachePrefill,
				FlatSearchConcurrency:  s.index.Config.HNSWFlatSearchConcurrency,
				AcornFilterRatio:       s.index.Config.HNSWAcornFilterRatio,
				VisitedListPoolMaxSize: s.index.Config.VisitedListPoolMaxSize,
				MakeBucketOptions:      makeBucketOptions,
				AsyncIndexingEnabled:   s.index.AsyncIndexingEnabled,
			}, hnswUserConfig, s.cycleCallbacks.vectorTombstoneCleanupCallbacks, s.store)
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

		vi, err := flat.New(flat.Config{
			ID:                physicalID,
			RootPath:          s.path(),
			Logger:            logger,
			DistanceProvider:  distProv,
			AllocChecker:      s.index.allocChecker,
			MakeBucketOptions: makeBucketOptions,
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
		s.index.cycleCallbacks.vectorTombstoneCleanupCycle.Start()

		vi, err := dynamic.New(dynamic.Config{
			ID:                           physicalID,
			Logger:                       logger,
			DistanceProvider:             distProv,
			RootPath:                     s.path(),
			ShardName:                    s.name,
			ClassName:                    s.index.Config.ClassName.String(),
			PrometheusMetrics:            s.promMetrics,
			VectorForIDThunk:             hnsw.NewVectorForIDThunk(targetVector, s.vectorByIndexID),
			VectorFromObject:             vectorFromObjectForTarget(targetVector),
			GetViewThunk:                 func() vcommon.BucketView { return s.GetObjectsBucketView() },
			TempVectorForIDWithViewThunk: hnsw.NewTempVectorForIDWithViewThunk(targetVector, s.readVectorByIndexIDIntoSliceWithView),
			MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
				// Prepend our default options, caller's opts take precedence
				allOpts := append([]hnsw.CommitlogOption{
					// consistent with previous logic where the individual limit is 1/5 of the combined limit
					hnsw.WithCommitlogThreshold(s.index.Config.HNSWMaxLogSize / 5),
				}, opts...)
				return hnsw.NewCommitLogger(s.path(), physicalID,
					logger, s.cycleCallbacks.vectorCommitLoggerCallbacks,
					allOpts...,
				)
			},
			TombstoneCallbacks:   s.cycleCallbacks.vectorTombstoneCleanupCallbacks,
			State:                s.metadataDB.Namespace(dynamic.StateNamespace),
			AllocChecker:         s.index.allocChecker,
			MakeBucketOptions:    makeBucketOptions,
			AsyncIndexingEnabled: s.index.AsyncIndexingEnabled,
		}, dynamicUserConfig, s.store)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: dynamic index", s.ID())
		}
		vectorIndex = vi
	case vectorindex.VectorIndexTypeHFresh:
		userConfig, ok := vectorIndexUserConfig.(hfreshent.UserConfig)
		if !ok {
			return nil, errors.Errorf("hfresh vector index: config is not hfresh.UserConfig: %T",
				vectorIndexUserConfig)
		}

		s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()
		s.index.cycleCallbacks.vectorTombstoneCleanupCycle.Start()

		hfreshConfigID := physicalID
		centroidsID := helpers.CentroidsID(hfreshConfigID)
		rootPath := filepath.Join(s.path(), helpers.HFreshDirName(hfreshConfigID))

		hfreshConfig := &hfresh.Config{
			Logger:            logger,
			Scheduler:         s.index.scheduler,
			DistanceProvider:  distProv,
			RootPath:          rootPath,
			ID:                hfreshConfigID,
			ShardName:         s.name,
			ClassName:         s.index.Config.ClassName.String(),
			PrometheusMetrics: s.promMetrics,
			Store: hfresh.StoreConfig{
				MakeBucketOptions: makeBucketOptions,
			},
			VectorForIDThunk:             hnsw.NewVectorForIDThunk(targetVector, s.vectorByIndexID),
			MultiVectorForIDThunk:        hnsw.NewVectorForIDThunk(targetVector, s.multiVectorByIndexID),
			TempVectorForIDWithViewThunk: hnsw.NewTempVectorForIDWithViewThunk(targetVector, s.readVectorByIndexIDIntoSliceWithView),
			TombstoneCallbacks:           s.cycleCallbacks.vectorTombstoneCleanupCallbacks,
			Centroids: hfresh.CentroidConfig{
				HNSWConfig: &hnsw.Config{
					Logger:                            logger,
					RootPath:                          rootPath,
					ID:                                centroidsID,
					ShardName:                         s.name,
					ClassName:                         s.index.Config.ClassName.String(),
					PrometheusMetrics:                 s.promMetrics,
					HFreshMode:                        true,
					TempMultiVectorForIDThunk:         hnsw.NewTempMultiVectorForIDThunk(targetVector, s.readMultiVectorByIndexIDIntoSlice),
					GetViewThunk:                      func() vcommon.BucketView { return s.GetObjectsBucketView() },
					TempVectorForIDWithViewThunk:      hnsw.NewTempVectorForIDWithViewThunk(targetVector, s.readVectorByIndexIDIntoSliceWithView),
					TempMultiVectorForIDWithViewThunk: hnsw.NewTempVectorForIDWithViewThunk(targetVector, s.readMultiVectorByIndexIDIntoSliceWithView),
					DistanceProvider:                  distProv,
					MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
						// Prepend our default options, caller's opts take precedence
						allOpts := append([]hnsw.CommitlogOption{
							// consistent with previous logic where the individual limit is 1/5 of the combined limit
							hnsw.WithCommitlogThreshold(s.index.Config.HNSWMaxLogSize / 5),
						}, opts...)
						return hnsw.NewCommitLogger(rootPath, centroidsID,
							logger.WithField("index_id", centroidsID), s.cycleCallbacks.vectorCommitLoggerCallbacks,
							allOpts...,
						)
					},
					AllocChecker:           s.index.allocChecker,
					WaitForCachePrefill:    s.index.Config.HNSWWaitForCachePrefill,
					FlatSearchConcurrency:  s.index.Config.HNSWFlatSearchConcurrency,
					AcornFilterRatio:       s.index.Config.HNSWAcornFilterRatio,
					VisitedListPoolMaxSize: s.index.Config.VisitedListPoolMaxSize,
					MakeBucketOptions:      makeBucketOptions,
				},
			},
		}

		vi, err := hfresh.New(hfreshConfig, userConfig, s.store)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: hfresh index", s.ID())
		}
		vectorIndex = vi
	default:
		return nil, fmt.Errorf("unknown vector index type: %q. Choose one from [\"%s\", \"%s\", \"%s\", \"%s\"]",
			vectorIndexUserConfig.IndexType(), vectorindex.VectorIndexTypeHNSW, vectorindex.VectorIndexTypeFLAT, vectorindex.VectorIndexTypeDYNAMIC, vectorindex.VectorIndexTypeHFresh)
	}
	defer vectorIndex.PostStartup(s.shutCtx)
	return vectorIndex, nil
}

// initTargetVector creates the named vector's index and queue under the
// naming rule unless the shard has them already, recording it before the
// build and once built. Creates are serialized by the slots, so two concurrent
// UpdateVectorIndexConfigs calls that both saw the target absent build it
// once; the second finds it in place and returns.
func (s *Shard) initTargetVector(ctx context.Context, targetVector string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) error {
	if !vectorIndexHasStorage(cfg) {
		id := s.vectorIndexID(targetVector)
		return s.createVectorIndex(ctx, targetVector, id, id, cfg, lazyLoadSegments)
	}
	_, err := s.vectors.Create(targetVector, func() (VectorIndex, *VectorIndexQueue, error) {
		entry, err := s.manifest.Create(ctx, targetVector, cfg)
		if err != nil {
			return nil, nil, err
		}
		index, queue, err := s.buildVectorIndexAndQueue(ctx, targetVector, entry.StorageID, entry.QueueID, cfg, lazyLoadSegments)
		if err != nil {
			return nil, nil, err
		}
		err = s.manifest.Ready(map[string]schemaConfig.VectorIndexConfig{targetVector: cfg})
		if err != nil {
			// nothing may run unpublished; the record stays creating for the retry
			errs := []error{err}
			if closeErr := queue.Close(ctx); closeErr != nil {
				errs = append(errs, fmt.Errorf("close the unrecorded queue: %w", closeErr))
			}
			if shutdownErr := index.Shutdown(s.shutCtx); shutdownErr != nil {
				errs = append(errs, fmt.Errorf("shut the unrecorded vector index down: %w", shutdownErr))
			}
			return nil, nil, stderrors.Join(errs...)
		}
		return index, queue, nil
	})
	return err
}

// createVectorIndex builds and publishes targetVector's index at physicalID
// and its queue at queueID unless the slot exists.
func (s *Shard) createVectorIndex(ctx context.Context, targetVector, physicalID, queueID string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) error {
	_, err := s.vectors.Create(targetVector, func() (VectorIndex, *VectorIndexQueue, error) {
		return s.buildVectorIndexAndQueue(ctx, targetVector, physicalID, queueID, cfg, lazyLoadSegments)
	})
	return err
}

// buildVectorIndexAndQueue constructs a vector's index at physicalID and
// its queue at queueID. A queue that fails to build takes the index down
// with it, so nothing is left running unpublished.
func (s *Shard) buildVectorIndexAndQueue(ctx context.Context, targetVector, physicalID, queueID string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) (VectorIndex, *VectorIndexQueue, error) {
	vectorIndex, err := s.initVectorIndex(ctx, targetVector, physicalID, cfg, lazyLoadSegments)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create vector index for %q: %w", targetVector, err)
	}
	queue, err := newVectorIndexQueueWithID(s, queueID, s.vectorIndexLogger(targetVector, physicalID), targetVector, vectorIndex, 0)
	if err != nil {
		if shutdownErr := vectorIndex.Shutdown(s.shutCtx); shutdownErr != nil {
			return nil, nil, fmt.Errorf("cannot create index queue for %q: %w (shutting down the orphaned vector index also failed: %w)",
				targetVector, err, shutdownErr)
		}
		return nil, nil, fmt.Errorf("cannot create index queue for %q: %w", targetVector, err)
	}
	return vectorIndex, queue, nil
}

func (s *Shard) setVectorIndex(targetVector string, index VectorIndex) {
	s.vectors.Replace(targetVector, index)
}

// perVectorDropper is implemented by index types whose Drop() would reach
// beyond the one vector being dropped. Only dynamic needs it today: its state
// DB is shared across the shard, so Drop's Close()+Remove would take every
// sibling's state with it.
type perVectorDropper interface {
	DropTargetVector(ctx context.Context) error
}

// dropOneVectorIndex tears down a single named vector's index. Index types that
// own nothing shard-wide fall through to Drop, which is the same thing for
// them; the interface exists so a type that DOES own shared state has
// somewhere to say so, rather than the caller having to know which types are
// special.
func dropOneVectorIndex(ctx context.Context, index VectorIndex) error {
	if d, ok := index.(perVectorDropper); ok {
		return d.DropTargetVector(ctx)
	}
	return index.Drop(ctx)
}

// DropVectorIndex removes the named vector's index and queue from this shard
// and deletes what the manifest says it owns. A vector the manifest does not
// know owns nothing: a skipped vector, or a drop that already completed.
func (s *Shard) DropVectorIndex(ctx context.Context, targetVector string) error {
	recorded, err := s.manifest.BeginDrop(targetVector)
	if err != nil {
		return fmt.Errorf("drop vector %q: %w", targetVector, err)
	}
	err = s.vectors.Remove(ctx, targetVector, s.index.logger, func(index VectorIndex, queue *VectorIndexQueue) error {
		if queue != nil {
			if err := queue.Drop(ctx); err != nil {
				return fmt.Errorf("drop queue for vector %q: %w", targetVector, err)
			}
		}
		if index != nil {
			if err := dropOneVectorIndex(ctx, index); err != nil {
				return fmt.Errorf("drop vector index %q: %w", targetVector, err)
			}
		}
		return nil
	})
	if err != nil || !recorded {
		return err
	}
	return s.manifest.FinishDrop(ctx, targetVector)
}
