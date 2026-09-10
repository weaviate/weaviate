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
			if stderrors.As(err, &notFound) {
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

	// The mapping is the shard's record of which indexes it has. A shard
	// without one builds under the naming rule and writes it; one with it
	// reconciles the records against the schema first.
	_, initialized, err := s.mapping.Load()
	if err != nil {
		return fmt.Errorf("shard %q: %w", s.ID(), err)
	}
	// an initialized mapping is reconciled in the next commit; until then
	// it builds as before and writes nothing

	if legacy != nil {
		if err := s.initLegacyVector(ctx, legacy, s.lazySegmentLoadingEnabled); err != nil {
			return err
		}
	}

	if err := s.initTargetVectors(ctx, legacy, targets, s.lazySegmentLoadingEnabled); err != nil {
		return err
	}

	if !initialized {
		err = s.initVectorIndexMapping(activeVectorIndexConfigs(legacy, targets))
		if err != nil {
			return fmt.Errorf("shard %q: %w", s.ID(), err)
		}
	}
	return nil
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

	// a shard can actually have multiple vector indexes:
	// - the main index, which is used for all normal object vectors
	// - a geo property index for each geo prop in the schema
	//
	// here we label the main vector index as such.
	vecIdxID := physicalID

	// Every log line under this index carries both identities: the logical
	// name for operators ("which vector?") and the physical id for storage
	// ("which files?"). Implementations and the entities they own (compressors,
	// commit loggers, queues) inherit it and add nothing of their own.
	logger := s.vectorIndexLogger(targetVector, vecIdxID)

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
				ID:                                vecIdxID,
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
					return hnsw.NewCommitLogger(s.path(), vecIdxID,
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
			ID:                vecIdxID,
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
			ID:                           vecIdxID,
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
				return hnsw.NewCommitLogger(s.path(), vecIdxID,
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

		hfreshConfigID := vecIdxID
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

// initTargetVectors builds the named target-vector indexes. legacy and configs
// are caller-held snapshots; the migrator needs legacy to tell a
// single-named-vector layout from a legacy-plus-named one.
func (s *Shard) initTargetVectors(ctx context.Context, legacy schemaConfig.VectorIndexConfig,
	configs map[string]schemaConfig.VectorIndexConfig, lazyLoadSegments bool,
) error {
	if err := newCompressedVectorsMigrator(s.index.logger).do(s, legacy, configs); err != nil {
		s.index.logger.WithFields(logrus.Fields{
			"action":   "init_target_vectors",
			"shard_id": s.ID(),
		}).Errorf("failed to migrate vectors compressed folder: %v", err)
	}

	for targetVector, vectorIndexConfig := range configs {
		if err := s.initTargetVector(ctx, targetVector, vectorIndexConfig, lazyLoadSegments); err != nil {
			return err
		}
	}
	return nil
}

// initTargetVector creates the named vector's index and queue under the
// naming rule unless the shard has them already. Creates are serialized by
// the slots, so two concurrent UpdateVectorIndexConfigs calls that both saw
// the target absent build it once; the second finds it in place and returns.
func (s *Shard) initTargetVector(ctx context.Context, targetVector string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) error {
	return s.createVectorIndex(ctx, targetVector, s.vectorIndexID(targetVector), cfg, lazyLoadSegments)
}

// createVectorIndex builds and publishes targetVector's index and queue at
// physicalID unless the slot exists. The ID is the caller's: the naming
// rule for a new vector, the mapping's record for one the shard already has.
func (s *Shard) createVectorIndex(ctx context.Context, targetVector, physicalID string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) error {
	_, err := s.vectors.Create(targetVector, func() (VectorIndex, *VectorIndexQueue, error) {
		return s.buildVectorIndexAndQueue(ctx, targetVector, physicalID, cfg, lazyLoadSegments)
	})
	return err
}

// buildVectorIndexAndQueue constructs a vector's index and its queue at
// physicalID. A queue that fails to build takes the index down with it, so
// nothing is left running unpublished.
func (s *Shard) buildVectorIndexAndQueue(ctx context.Context, targetVector, physicalID string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) (VectorIndex, *VectorIndexQueue, error) {
	vectorIndex, err := s.initVectorIndex(ctx, targetVector, physicalID, cfg, lazyLoadSegments)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create vector index for %q: %w", targetVector, err)
	}
	queue, err := NewVectorIndexQueue(s, targetVector, vectorIndex)
	if err != nil {
		if shutdownErr := vectorIndex.Shutdown(s.shutCtx); shutdownErr != nil {
			return nil, nil, fmt.Errorf("cannot create index queue for %q: %w (shutting down the orphaned vector index also failed: %w)",
				targetVector, err, shutdownErr)
		}
		return nil, nil, fmt.Errorf("cannot create index queue for %q: %w", targetVector, err)
	}
	return vectorIndex, queue, nil
}

// initLegacyVector creates the legacy vector's index and queue under the
// naming rule unless the shard has them already. A second init used to
// replace the running index with a fresh instance and orphan it; now it
// finds the first in place.
func (s *Shard) initLegacyVector(ctx context.Context, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) error {
	return s.createVectorIndex(ctx, "", s.vectorIndexID(""), cfg, lazyLoadSegments)
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
// own nothing shard-wide fall through to Drop(keepFiles=false), which is the
// same thing for them; the interface exists so a type that DOES own shared
// state has somewhere to say so, rather than the caller having to know which
// types are special.
func dropOneVectorIndex(ctx context.Context, index VectorIndex) error {
	if d, ok := index.(perVectorDropper); ok {
		return d.DropTargetVector(ctx)
	}
	return index.Drop(ctx, false)
}

// DropVectorIndex shuts down and removes the named vector index and its queue
// from this shard, deleting associated files from disk. It also removes the
// LSM buckets that store the raw and compressed vector data.
func (s *Shard) DropVectorIndex(ctx context.Context, targetVector string) error {
	err := s.vectors.Remove(ctx, targetVector, s.index.logger, func(index VectorIndex, queue *VectorIndexQueue) error {
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
	if err != nil {
		return err
	}

	// Remove every on-disk artifact this vector owns — the raw and compressed
	// buckets, the multivector ones (muvera OR mv_mappings), and hfresh's
	// directory and buckets. The set lives in helpers so the live drop, the
	// file sweep and the tests cannot drift apart; passing the collection's
	// other vector names is what stops a sibling being deleted when its own
	// bucket collides with one of this target's artifact names.
	artifacts := helpers.VectorIndexArtifactsFor(targetVector,
		otherTargetVectors(s.class, targetVector))
	for _, bucket := range artifacts.LSMBuckets {
		if err := s.removeBucket(ctx, bucket); err != nil {
			return fmt.Errorf("drop bucket %q for vector %q: %w", bucket, targetVector, err)
		}
	}
	for _, dir := range artifacts.ShardDirs {
		if err := s.removeDirIfExists(s.path(), dir); err != nil {
			return fmt.Errorf("drop directory %q for vector %q: %w", dir, targetVector, err)
		}
	}

	// the record goes last, so a failed drop keeps it for the retry
	err = s.mapping.Delete(targetVector)
	if err != nil {
		return fmt.Errorf("drop mapping record for vector %q: %w", targetVector, err)
	}
	return nil
}
