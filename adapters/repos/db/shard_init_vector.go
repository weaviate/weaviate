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
)

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
				AllocChecker:           s.index.allocChecker,
				WaitForCachePrefill:    s.index.Config.HNSWWaitForCachePrefill,
				FlatSearchConcurrency:  s.index.Config.HNSWFlatSearchConcurrency,
				VisitedListPoolMaxSize: s.index.Config.VisitedListPoolMaxSize,
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
			RootPath:         s.path(),
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
		queue, err := NewIndexQueue(s.index.Config.ClassName.String(), s.ID(), targetVector, s, vectorIndex, s.centralJobQueue,
			s.indexCheckpoints, IndexQueueOptions{Logger: s.index.logger}, s.promMetrics)
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
	queue, err := NewIndexQueue(s.index.Config.ClassName.String(), s.ID(), "", s, s.vectorIndex, s.centralJobQueue,
		s.indexCheckpoints, IndexQueueOptions{Logger: s.index.logger}, s.promMetrics)
	if err != nil {
		return err
	}
	s.queue = queue
	return nil
}
