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

// Some standard accessors for the shard struct.
// It is important to NEVER access the shard struct directly, because we lazy load shards, so the information might not be there.
package db

import (
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
)

// ForEachVectorIndex iterates through each vector index initialized in the shard (named and legacy).
// Iteration stops at the first return of non-nil error.
func (s *Shard) ForEachVectorIndex(f func(targetVector string, index VectorIndex) error) error {
	// As we expect the mutex to be write-locked very rarely, we allow the callback
	// to be invoked under the lock. If we find contention here, we should make a copy of the indexes
	// before iterating over them.
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	for targetVector, idx := range s.vectorIndexes {
		if idx == nil {
			continue
		}

		if err := f(targetVector, idx); err != nil {
			return err
		}
	}
	if s.vectorIndex != nil {
		if err := f("", s.vectorIndex); err != nil {
			return err
		}
	}
	return nil
}

// ForEachVectorQueue iterates through each vector index queue initialized in the shard (named and legacy).
// Iteration stops at the first return of non-nil error.
func (s *Shard) ForEachVectorQueue(f func(targetVector string, queue *VectorIndexQueue) error) error {
	// As we expect the mutex to be write-locked very rarely, we allow the callback
	// to be invoked under the lock. If we find contention here, we should make a copy of the queues
	// before iterating over them.
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	for targetVector, q := range s.queues {
		if q == nil {
			continue
		}

		if err := f(targetVector, q); err != nil {
			return err
		}
	}
	if s.queue != nil {
		if err := f("", s.queue); err != nil {
			return err
		}
	}
	return nil
}

// GetVectorIndexQueue retrieves a vector index queue associated with the targetVector.
// Empty targetVector is treated as a request to access a queue for the legacy vector index.
func (s *Shard) GetVectorIndexQueue(targetVector string) (*VectorIndexQueue, bool) {
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	if s.isTargetVectorLegacyWithLock(targetVector) {
		return s.queue, s.queue != nil
	}

	queue, ok := s.queues[targetVector]
	return queue, ok
}

// GetVectorIndex retrieves a vector index queue associated with the targetVector.
// Empty targetVector is treated as a request to access a queue for the legacy vector index.
func (s *Shard) GetVectorIndex(targetVector string) (VectorIndex, bool) {
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	if s.isTargetVectorLegacyWithLock(targetVector) {
		return s.vectorIndex, s.vectorIndex != nil
	}

	index, ok := s.vectorIndexes[targetVector]
	return index, ok
}

func (s *Shard) isTargetVectorLegacyWithLock(targetVector string) bool {
	if targetVector == "" {
		return true
	}

	return s.vectorIndex != nil && targetVector == modelsext.DefaultNamedVectorName
}

func (s *Shard) hasLegacyVectorIndex() bool {
	_, ok := s.GetVectorIndex("")
	return ok
}

func (s *Shard) hasAnyVectorIndex() bool {
	s.vectorIndexMu.RLock()
	defer s.vectorIndexMu.RUnlock()

	return len(s.vectorIndexes) > 0 || s.vectorIndex != nil
}

func (s *Shard) Versioner() *shardVersioner {
	return s.versioner
}

func (s *Shard) Index() *Index {
	return s.index
}

// Shard name(identifier?)
func (s *Shard) Name() string {
	return s.name
}

// The physical data store
func (s *Shard) Store() *lsmkv.Store {
	return s.store
}

func (s *Shard) Counter() *indexcounter.Counter {
	return s.counter
}

// Tracks the lengths of all properties.  Must be updated on inserts/deletes.
func (s *Shard) GetPropertyLengthTracker() *inverted.JsonShardMetaData {
	return s.propLenTracker
}

// Tracks the lengths of all properties.  Must be updated on inserts/deletes.
func (s *Shard) SetPropertyLengthTracker(tracker *inverted.JsonShardMetaData) {
	s.propLenTracker = tracker
}

// Grafana metrics
func (s *Shard) Metrics() *Metrics {
	return s.metrics
}

func (s *Shard) setFallbackToSearchable(fallback bool) {
	s.fallbackToSearchable = fallback
}

func (s *Shard) addJobToQueue(job job) {
	s.centralJobQueue <- job
}

func (s *Shard) hasGeoIndex() bool {
	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	for _, idx := range s.propertyIndices {
		if idx.Type == schema.DataTypeGeoCoordinates {
			return true
		}
	}
	return false
}
