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
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"go.etcd.io/bbolt"
)

func (s *Shard) initShardVectors(ctx context.Context, lazyLoadSegments bool) error {
	if s.index.vectorIndexUserConfig != nil {
		if err := s.initLegacyVector(ctx, lazyLoadSegments); err != nil {
			return err
		}
	}

	if err := s.initTargetVectors(ctx, lazyLoadSegments); err != nil {
		return err
	}

	return nil
}

func (s *Shard) initVectorIndex(ctx context.Context,
	targetVector string, vectorIndexUserConfig schemaConfig.VectorIndexConfig, lazyLoadSegments bool,
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
				Logger:                    s.index.logger,
				RootPath:                  s.path(),
				ID:                        vecIdxID,
				ShardName:                 s.name,
				ClassName:                 s.index.Config.ClassName.String(),
				PrometheusMetrics:         s.promMetrics,
				VectorForIDThunk:          hnsw.NewVectorForIDThunk(targetVector, s.vectorByIndexID),
				MultiVectorForIDThunk:     hnsw.NewVectorForIDThunk(targetVector, s.multiVectorByIndexID),
				TempVectorForIDThunk:      hnsw.NewTempVectorForIDThunk(targetVector, s.readVectorByIndexIDIntoSlice),
				TempMultiVectorForIDThunk: hnsw.NewTempMultiVectorForIDThunk(targetVector, s.readMultiVectorByIndexIDIntoSlice),
				DistanceProvider:          distProv,
				MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
					return hnsw.NewCommitLogger(s.path(), vecIdxID,
						s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks,
						hnsw.WithAllocChecker(s.index.allocChecker),
						hnsw.WithCommitlogThresholdForCombining(s.index.Config.HNSWMaxLogSize),
						// consistent with previous logic where the individual limit is 1/5 of the combined limit
						hnsw.WithCommitlogThreshold(s.index.Config.HNSWMaxLogSize/5),
						hnsw.WithSnapshotDisabled(s.index.Config.HNSWDisableSnapshots),
						hnsw.WithSnapshotCreateInterval(time.Duration(s.index.Config.HNSWSnapshotIntervalSeconds)*time.Second),
						hnsw.WithSnapshotMinDeltaCommitlogsNumer(s.index.Config.HNSWSnapshotMinDeltaCommitlogsNumber),
						hnsw.WithSnapshotMinDeltaCommitlogsSizePercentage(s.index.Config.HNSWSnapshotMinDeltaCommitlogsSizePercentage),
					)
				},
				AllocChecker:           s.index.allocChecker,
				WaitForCachePrefill:    s.index.Config.HNSWWaitForCachePrefill,
				FlatSearchConcurrency:  s.index.Config.HNSWFlatSearchConcurrency,
				AcornFilterRatio:       s.index.Config.HNSWAcornFilterRatio,
				VisitedListPoolMaxSize: s.index.Config.VisitedListPoolMaxSize,
				DisableSnapshots:       s.index.Config.HNSWDisableSnapshots,
				SnapshotOnStartup:      s.index.Config.HNSWSnapshotOnStartup,
				LazyLoadSegments:       lazyLoadSegments,
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

		// a shard can actually have multiple vector indexes:
		// - the main index, which is used for all normal object vectors
		// - a geo property index for each geo prop in the schema
		//
		// here we label the main vector index as such.
		vecIdxID := s.vectorIndexID(targetVector)

		vi, err := flat.New(flat.Config{
			ID:               vecIdxID,
			TargetVector:     targetVector,
			RootPath:         s.path(),
			Logger:           s.index.logger,
			DistanceProvider: distProv,
			AllocChecker:     s.index.allocChecker,
			MinMMapSize:      s.index.Config.MinMMapSize,
			MaxWalReuseSize:  s.index.Config.MaxReuseWalSize,
			LazyLoadSegments: lazyLoadSegments,
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

		sharedDB, err := s.getOrInitDynamicVectorIndexDB()
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: dynamic index", s.ID())
		}

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
					s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks,
					hnsw.WithAllocChecker(s.index.allocChecker),
					hnsw.WithCommitlogThresholdForCombining(s.index.Config.HNSWMaxLogSize),
					// consistent with previous logic where the individual limit is 1/5 of the combined limit
					hnsw.WithCommitlogThreshold(s.index.Config.HNSWMaxLogSize/5),
					hnsw.WithSnapshotDisabled(s.index.Config.HNSWDisableSnapshots),
					hnsw.WithSnapshotCreateInterval(time.Duration(s.index.Config.HNSWSnapshotIntervalSeconds)*time.Second),
					hnsw.WithSnapshotMinDeltaCommitlogsNumer(s.index.Config.HNSWSnapshotMinDeltaCommitlogsNumber),
					hnsw.WithSnapshotMinDeltaCommitlogsSizePercentage(s.index.Config.HNSWSnapshotMinDeltaCommitlogsSizePercentage),
				)
			},
			TombstoneCallbacks:    s.cycleCallbacks.vectorTombstoneCleanupCallbacks,
			SharedDB:              sharedDB,
			HNSWDisableSnapshots:  s.index.Config.HNSWDisableSnapshots,
			HNSWSnapshotOnStartup: s.index.Config.HNSWSnapshotOnStartup,
			MinMMapSize:           s.index.Config.MinMMapSize,
			MaxWalReuseSize:       s.index.Config.MaxReuseWalSize,
			LazyLoadSegments:      lazyLoadSegments,
			AllocChecker:          s.index.allocChecker,
		}, dynamicUserConfig, s.store)
		if err != nil {
			return nil, errors.Wrapf(err, "init shard %q: dynamic index", s.ID())
		}
		vectorIndex = vi
	default:
		return nil, fmt.Errorf("unknown vector index type: %q. Choose one from [\"%s\", \"%s\", \"%s\"]",
			vectorIndexUserConfig.IndexType(), vectorindex.VectorIndexTypeHNSW, vectorindex.VectorIndexTypeFLAT, vectorindex.VectorIndexTypeDYNAMIC)
	}
	defer vectorIndex.PostStartup()
	return vectorIndex, nil
}

func (s *Shard) getOrInitDynamicVectorIndexDB() (*bbolt.DB, error) {
	if s.dynamicVectorIndexDB == nil {
		path := filepath.Join(s.path(), "index.db")

		db, err := bbolt.Open(path, 0o600, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "open %q", path)
		}

		s.dynamicVectorIndexDB = db
	}

	return s.dynamicVectorIndexDB, nil
}

func (s *Shard) initTargetVectors(ctx context.Context, lazyLoadSegments bool) error {
	s.vectorIndexMu.Lock()
	defer s.vectorIndexMu.Unlock()

	s.vectorIndexes = make(map[string]VectorIndex, len(s.index.vectorIndexUserConfigs))
	s.queues = make(map[string]*VectorIndexQueue, len(s.index.vectorIndexUserConfigs))

	for targetVector, vectorIndexConfig := range s.index.vectorIndexUserConfigs {
		if err := s.initTargetVectorWithLock(ctx, targetVector, vectorIndexConfig, lazyLoadSegments); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) initTargetVector(ctx context.Context, targetVector string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) error {
	s.vectorIndexMu.Lock()
	defer s.vectorIndexMu.Unlock()
	return s.initTargetVectorWithLock(ctx, targetVector, cfg, lazyLoadSegments)
}

func (s *Shard) initTargetVectorWithLock(ctx context.Context, targetVector string, cfg schemaConfig.VectorIndexConfig, lazyLoadSegments bool) error {
	vectorIndex, err := s.initVectorIndex(ctx, targetVector, cfg, lazyLoadSegments)
	if err != nil {
		return fmt.Errorf("cannot create vector index for %q: %w", targetVector, err)
	}
	queue, err := NewVectorIndexQueue(s, targetVector, vectorIndex)
	if err != nil {
		return fmt.Errorf("cannot create index queue for %q: %w", targetVector, err)
	}

	s.vectorIndexes[targetVector] = vectorIndex
	s.queues[targetVector] = queue
	return nil
}

func (s *Shard) initLegacyVector(ctx context.Context, lazyLoadSegments bool) error {
	s.vectorIndexMu.Lock()
	defer s.vectorIndexMu.Unlock()

	vectorIndex, err := s.initVectorIndex(ctx, "", s.index.vectorIndexUserConfig, lazyLoadSegments)
	if err != nil {
		return err
	}

	queue, err := NewVectorIndexQueue(s, "", vectorIndex)
	if err != nil {
		return err
	}
	s.vectorIndex = vectorIndex
	s.queue = queue
	return nil
}

func (s *Shard) setVectorIndex(targetVector string, index VectorIndex) {
	s.vectorIndexMu.Lock()
	defer s.vectorIndexMu.Unlock()

	if targetVector == "" {
		s.vectorIndex = index
	} else {
		s.vectorIndexes[targetVector] = index
	}
}
